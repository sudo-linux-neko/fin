// Author: vaibhav (final authority over all project decisions)
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Pass B — halt filter + universe gates, forward in time (spec §4a/§4b).
//
// Per ticker-day, causal only (no future data touched):
// - Volume == 0 -> Tradable=false, Close forward-filled from previous
//   ADJUSTED close. consec_untradable counts the run; > 5 drops the row
//   from the selection universe that day (history retained).
// - price_pass: adj_close >= $5.00.
// - adv_pass: trailing-30-trading-row mean(raw_close * volume) >= $10M,
//   requires a full 30 rows (young tickers fail until seasoned).
// - universe_pass = price_pass && adv_pass && !halted_out.
// - mcap gate: WIRED BUT PENDING — no shares-outstanding in price data.
//   Carried as a nullable predicate; Section 4c (SEC) plugs it in and
//   `universe_pass &= mcap_pass` with no rework (see DECISIONS.md D5).

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Date32Array, Float32Array, Float64Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::price::TickerDict;

pub const MIN_PRICE: f32 = 5.0;
pub const MIN_ADV_DOLLAR: f64 = 10_000_000.0;
pub const ADV_WINDOW: usize = 30;
pub const MAX_HALT_RUN: u32 = 5;

struct TickerState {
    prev_ac: f32,
    consec: u32,
    dv_window: VecDeque<f64>,
    dv_sum: f64,
    rows_seen: u64,
    /// Remaining repair product over missed splits with T > current date.
    repair: f64,
    ev_idx: usize,
    repair_init: bool,
}

impl TickerState {
    fn new() -> Self {
        Self {
            prev_ac: f32::NAN,
            consec: 0,
            dv_window: VecDeque::with_capacity(ADV_WINDOW + 1),
            dv_sum: 0.0,
            rows_seen: 0,
            repair: 1.0,
            ev_idx: 0,
            repair_init: false,
        }
    }
}

pub struct UniverseStats {
    pub rows: u64,
    pub tradable_false: u64,
    pub halted_out: u64,
    pub price_pass: u64,
    pub adv_pass: u64,
    pub universe_pass: u64,
    pub repaired_rows: u64,
}

pub fn forward_pass(
    tmp_dir: &str,
    num_rgs: usize,
    dict: &TickerDict,
    missed: &HashMap<u32, Vec<(i32, f64)>>,
    out_path: &str,
) -> Result<UniverseStats> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new("ticker", DataType::Utf8, false),
        Field::new("adj_open", DataType::Float32, false),
        Field::new("adj_high", DataType::Float32, false),
        Field::new("adj_low", DataType::Float32, false),
        Field::new("adj_close", DataType::Float32, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("dividends", DataType::Float32, false),
        Field::new("splits", DataType::Float32, false),
        Field::new("tradable", DataType::Boolean, false),
        Field::new("consec_untradable", DataType::UInt32, false),
        Field::new("adv30_dollar", DataType::Float64, false),
        Field::new("price_pass", DataType::Boolean, false),
        Field::new("adv_pass", DataType::Boolean, false),
        Field::new("universe_pass", DataType::Boolean, false),
        // Audit trail: cumulative repair factor applied to adj_* on this row
        // (1.0 = vendor value untouched). Appended last so earlier indices
        // are stable for readers.
        Field::new("repair_factor", DataType::Float32, false),
    ]));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(1_000_000)
        .build();
    let out_f = File::create(out_path).context("create output parquet")?;
    let mut writer = ArrowWriter::try_new(out_f, schema.clone(), Some(props))?;

    let mut states: HashMap<u32, TickerState> = HashMap::with_capacity(16_384);
    let mut stats = UniverseStats {
        rows: 0,
        tradable_false: 0,
        halted_out: 0,
        price_pass: 0,
        adv_pass: 0,
        universe_pass: 0,
        repaired_rows: 0,
    };

    for rg in 0..num_rgs {
        let path = format!("{tmp_dir}/rg-{rg:02}.parquet");
        let f = File::open(&path).with_context(|| format!("open {path}"))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)?.build()?;

        // Output builders for this RG.
        use arrow::array::{
            BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, StringBuilder,
            UInt32Builder,
        };
        let mut b_date = Date32Builder::new();
        let mut b_tick = StringBuilder::new();
        let mut b_ao = Float32Builder::new();
        let mut b_ah = Float32Builder::new();
        let mut b_al = Float32Builder::new();
        let mut b_ac = Float32Builder::new();
        let mut b_vol = Float64Builder::new();
        let mut b_div = Float32Builder::new();
        let mut b_spl = Float32Builder::new();
        let mut b_trad = BooleanBuilder::new();
        let mut b_consec = UInt32Builder::new();
        let mut b_adv = Float64Builder::new();
        let mut b_pp = BooleanBuilder::new();
        let mut b_ap = BooleanBuilder::new();
        let mut b_up = BooleanBuilder::new();
        let mut b_rep = Float32Builder::new();

        let mut n = 0usize;
        for batch in reader {
            let batch: RecordBatch = batch?;
            let g = |i: usize| batch.column(i);
            let dates = g(0).as_any().downcast_ref::<Date32Array>().unwrap();
            let tids = g(1).as_any().downcast_ref::<UInt32Array>().unwrap();
            let ao = g(2).as_any().downcast_ref::<Float32Array>().unwrap();
            let ah = g(3).as_any().downcast_ref::<Float32Array>().unwrap();
            let al = g(4).as_any().downcast_ref::<Float32Array>().unwrap();
            let ac = g(5).as_any().downcast_ref::<Float32Array>().unwrap();
            let rc = g(6).as_any().downcast_ref::<Float32Array>().unwrap();
            let vol = g(7).as_any().downcast_ref::<Float64Array>().unwrap();
            let div = g(8).as_any().downcast_ref::<Float32Array>().unwrap();
            let spl = g(9).as_any().downcast_ref::<Float32Array>().unwrap();
            for i in 0..batch.num_rows() {
                let tid = tids.value(i);
                let day = dates.value(i);
                let st = states.entry(tid).or_insert_with(TickerState::new);
                // Repair schedule: rows with date < T scale by 1/k.
                // Forward walk keeps the remaining product: init to the full
                // product, divide events out (multiply by k) once row >= T.
                if !st.repair_init {
                    st.repair_init = true;
                    if let Some(ev) = missed.get(&tid) {
                        st.repair = ev.iter().map(|&(_, k)| 1.0 / k).product();
                    }
                }
                if let Some(ev) = missed.get(&tid) {
                    while st.ev_idx < ev.len() && day >= ev[st.ev_idx].0 {
                        st.repair *= ev[st.ev_idx].1;
                        st.ev_idx += 1;
                    }
                }
                let rep = st.repair;
                let ao_r = ao.value(i) * rep as f32;
                let ah_r = ah.value(i) * rep as f32;
                let al_r = al.value(i) * rep as f32;
                if rep != 1.0 {
                    stats.repaired_rows += 1;
                }
                let volume = vol.value(i);
                let tradable = volume != 0.0;
                let mut close = ac.value(i) * rep as f32;
                if !tradable {
                    stats.tradable_false += 1;
                    if st.rows_seen > 0 {
                        close = st.prev_ac; // forward-fill adjusted close
                    }
                    st.consec += 1;
                } else {
                    st.consec = 0;
                }
                let halted_out = st.consec > MAX_HALT_RUN;
                if halted_out {
                    stats.halted_out += 1;
                }

                // ADV on RAW dollar volume (DECISIONS.md D4).
                let dv = rc.value(i) as f64 * volume;
                st.dv_window.push_back(dv);
                st.dv_sum += dv;
                if st.dv_window.len() > ADV_WINDOW {
                    st.dv_sum -= st.dv_window.pop_front().unwrap();
                }
                let full = st.dv_window.len() == ADV_WINDOW;
                let adv = if full {
                    st.dv_sum / ADV_WINDOW as f64
                } else {
                    f64::NAN
                };

                let price_pass = close >= MIN_PRICE;
                let adv_pass = full && adv >= MIN_ADV_DOLLAR;
                let universe_pass = price_pass && adv_pass && !halted_out;
                if price_pass {
                    stats.price_pass += 1;
                }
                if adv_pass {
                    stats.adv_pass += 1;
                }
                if universe_pass {
                    stats.universe_pass += 1;
                }

                st.prev_ac = close;
                st.rows_seen += 1;

                b_date.append_value(dates.value(i));
                b_tick.append_value(&dict.names[tid as usize]);
                b_ao.append_value(ao_r);
                b_ah.append_value(ah_r);
                b_al.append_value(al_r);
                b_ac.append_value(close);
                b_vol.append_value(volume);
                b_div.append_value(div.value(i));
                b_spl.append_value(spl.value(i));
                b_trad.append_value(tradable);
                b_consec.append_value(st.consec);
                b_adv.append_value(adv);
                b_pp.append_value(price_pass);
                b_ap.append_value(adv_pass);
                b_up.append_value(universe_pass);
                b_rep.append_value(rep as f32);
                n += 1;
            }
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(b_date.finish()) as _,
                Arc::new(b_tick.finish()) as _,
                Arc::new(b_ao.finish()) as _,
                Arc::new(b_ah.finish()) as _,
                Arc::new(b_al.finish()) as _,
                Arc::new(b_ac.finish()) as _,
                Arc::new(b_vol.finish()) as _,
                Arc::new(b_div.finish()) as _,
                Arc::new(b_spl.finish()) as _,
                Arc::new(b_trad.finish()) as _,
                Arc::new(b_consec.finish()) as _,
                Arc::new(b_adv.finish()) as _,
                Arc::new(b_pp.finish()) as _,
                Arc::new(b_ap.finish()) as _,
                Arc::new(b_up.finish()) as _,
                Arc::new(b_rep.finish()) as _,
            ],
        )?;
        stats.rows += n as u64;
        eprintln!("[adjust] forward RG {rg}/{num_rgs} rows={n}");
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(stats)
}
