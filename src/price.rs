// Author: vaibhav (final authority over all project decisions)
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Pass A — vendor-adjustment adopter (spec §4a, corrected per DECISIONS.md D11).
//
// The source file's OHLC columns are ALREADY reverse-back-adjusted by the
// vendor (proven: AAPL printed 124.20 on 2020-08-28, i.e. the post-4:1 price,
// not the ~$497 raw print; NVDA's 10:1 has no event row at all yet prices
// are continuous; a 9.3%-yield dividend on IP 1962-11-19 shows no ex-div
// drop). Applying the spec's back-adjustment formula here would
// DOUBLE-adjust — the verify gate caught exactly this (ratio ≈ k jumps).
//
// So this pass streams RGs oldest→newest (no backward walk needed), interns
// tickers, copies prices through as `adj_*`, and records data-quality
// counters (non-finite/NaN split markers, zero closes). The proof that the
// vendor adjustment is trustworthy is recomputed independently in verify.rs:
// continuity across every recorded forward-split event.
//
// Memory: one row group (~1M rows) resident at a time + ticker dict.

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Date32Array, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub struct TickerDict {
    pub map: HashMap<String, u32>,
    pub names: Vec<String>,
}

impl TickerDict {
    pub fn new() -> Self {
        Self {
            map: HashMap::with_capacity(16_384),
            names: Vec::with_capacity(16_384),
        }
    }
    pub fn intern(&mut self, t: &str) -> u32 {
        if let Some(&id) = self.map.get(t) {
            return id;
        }
        let id = self.names.len() as u32;
        self.names.push(t.to_string());
        self.map.insert(t.to_string(), id);
        id
    }
}

pub struct AdjustStats {
    pub rows_in: u64,
    pub rows_out: u64,
    pub split_events: u64,
    pub div_events: u64,
    /// Split markers that are NaN / infinite (3x `inf` measured): ignored.
    pub split_nan_inf: u64,
    /// Rows with Close <= 0 (91,215 measured, 0.26%): passed through;
    /// the $5 price gate excludes them from the universe.
    pub zero_close: u64,
    /// Classified split events.
    pub split_ok_vendor: u64,
    pub split_missed: u64,
    pub split_ambiguous: u64,
}

/// A vendor-missed split needing repair is stored directly as
/// `missed: HashMap<tid, Vec<(date, k)>>`; rows with `date < t` scale by `1/k`.
/// Ambiguous event left untouched, for eyeball audit.
pub struct AmbiguousEvent {
    pub ticker: String,
    pub date: i32,
    pub k: f64,
    pub prev_close: f64,
    pub event_close: f64,
}

pub struct PassAResult {
    pub stats: AdjustStats,
    /// Per ticker, missed splits sorted by date ascending.
    pub missed: HashMap<u32, Vec<(i32, f64)>>,
    pub ambiguous: Vec<AmbiguousEvent>,
}

/// Stream RGs oldest→newest, intern tickers, copy prices as `adj_*`.
/// Writes per-RG temp files to `tmp_dir/rg-{:02}.parquet`.
/// Inline classifies every recorded split event (needs only the ticker's
/// previous close, available in forward order) into vendor-ok / missed /
/// ambiguous. Missed events are returned as a repair schedule for pass B.
pub fn passthrough_pass(src: &str, tmp_dir: &str, dict: &mut TickerDict) -> Result<PassAResult> {
    std::fs::create_dir_all(tmp_dir).context("create tmp dir")?;
    let f = File::open(src).context("open source parquet")?;
    let probe = ParquetRecordBatchReaderBuilder::try_new(f)?;
    let num_rgs = probe.metadata().num_row_groups();
    drop(probe);

    let mut stats = AdjustStats {
        rows_in: 0,
        rows_out: 0,
        split_events: 0,
        div_events: 0,
        split_nan_inf: 0,
        zero_close: 0,
        split_ok_vendor: 0,
        split_missed: 0,
        split_ambiguous: 0,
    };
    let mut missed: HashMap<u32, Vec<(i32, f64)>> = HashMap::new();
    let mut ambiguous: Vec<AmbiguousEvent> = Vec::new();
    let mut last_close: HashMap<u32, f64> = HashMap::with_capacity(16_384);
    let mut has_prev: HashMap<u32, bool> = HashMap::with_capacity(16_384);

    let out_schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new("ticker_id", DataType::UInt32, false),
        Field::new("adj_open", DataType::Float32, false),
        Field::new("adj_high", DataType::Float32, false),
        Field::new("adj_low", DataType::Float32, false),
        Field::new("adj_close", DataType::Float32, false),
        Field::new("raw_close", DataType::Float32, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("dividends", DataType::Float32, false),
        Field::new("splits", DataType::Float32, false),
    ]));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(1_000_000)
        .build();

    for rg in 0..num_rgs {
        let f = File::open(src).context("reopen source parquet")?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)?
            .with_row_groups(vec![rg])
            .build()?;
        let mut dates: Vec<i32> = Vec::new();
        let mut tids: Vec<u32> = Vec::new();
        let mut o: Vec<f32> = Vec::new();
        let mut h: Vec<f32> = Vec::new();
        let mut l: Vec<f32> = Vec::new();
        let mut c: Vec<f32> = Vec::new();
        let mut v: Vec<f64> = Vec::new();
        let mut d: Vec<f32> = Vec::new();
        let mut s: Vec<f32> = Vec::new();
        for batch in reader {
            let batch: RecordBatch = batch?;
            let da = batch
                .column(0)
                .as_any()
                .downcast_ref::<Date32Array>()
                .context("date col not Date32")?;
            let ta = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("ticker col not Utf8")?;
            let fa = |i: usize| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .context("numeric col not f64")
            };
            let oa = fa(2)?;
            let ha = fa(3)?;
            let la = fa(4)?;
            let ca = fa(5)?;
            let va = fa(6)?;
            let dad = fa(7)?;
            let sa = fa(8)?;
            for i in 0..batch.num_rows() {
                let day = da.value(i);
                let tid = dict.intern(ta.value(i));
                dates.push(day);
                tids.push(tid);
                o.push(oa.value(i) as f32);
                h.push(ha.value(i) as f32);
                l.push(la.value(i) as f32);
                let close = ca.value(i);
                c.push(close as f32);
                if close <= 0.0 {
                    stats.zero_close += 1;
                }
                v.push(va.value(i));
                let dv = dad.value(i);
                d.push(dv as f32);
                if dv != 0.0 {
                    stats.div_events += 1;
                }
                let sp = sa.value(i);
                if sp != 0.0 {
                    stats.split_events += 1;
                    if !sp.is_finite() {
                        stats.split_nan_inf += 1;
                    } else if sp != 1.0 {
                        // Classify: needs the ticker's previous close.
                        let prev_ok = *has_prev.get(&tid).unwrap_or(&false);
                        let p = *last_close.get(&tid).unwrap_or(&f64::NAN);
                        if !prev_ok
                            || !p.is_finite()
                            || p <= 0.0
                            || !close.is_finite()
                            || close <= 0.0
                        {
                            // First row or bad prints: cannot prove a miss;
                            // assume vendor-ok, log as ambiguous for audit.
                            stats.split_ambiguous += 1;
                            ambiguous.push(AmbiguousEvent {
                                ticker: dict.names[tid as usize].clone(),
                                date: day,
                                k: sp,
                                prev_close: p,
                                event_close: close,
                            });
                        } else {
                            let ratio = close / p;
                            let target = 1.0 / sp;
                            if (ratio - target).abs() <= 0.15 && (ratio - 1.0).abs() > 0.15 {
                                // Raw printed 1/k: vendor missed a FORWARD k:1
                                // event -> repair rows-before by 1/k.
                                stats.split_missed += 1;
                                missed.entry(tid).or_default().push((day, sp));
                            } else if (2.0..=4.0).contains(&sp)
                                && ((ratio - sp).abs() / sp) <= 0.10
                                && (ratio - 1.0).abs() > 0.35
                            {
                                // Raw printed k (not 1/k, not 1): the true
                                // event was the INVERSE (a 1-for-k reverse
                                // recorded under forward convention) and the
                                // vendor missed it -> repair rows-before by k.
                                // Stored as k' = 1/sp so pass B's 1/k'
                                // product yields x sp. Restricted to 2<=k<=4:
                                // for k<1, ratio~=k is a crash, never repair.
                                stats.split_missed += 1;
                                missed.entry(tid).or_default().push((day, 1.0 / sp));
                            } else if (ratio - 1.0).abs() <= 0.35 {
                                stats.split_ok_vendor += 1;
                            } else {
                                stats.split_ambiguous += 1;
                                ambiguous.push(AmbiguousEvent {
                                    ticker: dict.names[tid as usize].clone(),
                                    date: day,
                                    k: sp,
                                    prev_close: p,
                                    event_close: close,
                                });
                            }
                        }
                    }
                }
                s.push(sp as f32);
                last_close.insert(tid, close);
                has_prev.insert(tid, true);
            }
        }
        let n = dates.len();
        stats.rows_in += n as u64;

        let out_path = format!("{tmp_dir}/rg-{rg:02}.parquet");
        let out_f = File::create(&out_path).context("create tmp rg file")?;
        let mut writer = ArrowWriter::try_new(out_f, out_schema.clone(), Some(props.clone()))?;
        {
            use arrow::array::{
                Date32Array as D32, Float32Array as F32, Float64Array as F64, UInt32Array as U32,
            };
            // adj_* == vendor values verbatim (D11); raw_close kept for
            // RAW-dollar-volume ADV in pass B.
            let batch = RecordBatch::try_new(
                out_schema.clone(),
                vec![
                    Arc::new(D32::from(dates)),
                    Arc::new(U32::from(tids)),
                    Arc::new(F32::from(o.clone())),
                    Arc::new(F32::from(h.clone())),
                    Arc::new(F32::from(l.clone())),
                    Arc::new(F32::from(c.clone())),
                    Arc::new(F32::from(c)),
                    Arc::new(F64::from(v)),
                    Arc::new(F32::from(d)),
                    Arc::new(F32::from(s)),
                ],
            )?;
            writer.write(&batch)?;
        }
        writer.close()?;
        stats.rows_out += n as u64;
        eprintln!(
            "[adjust] passthrough RG {rg}/{num_rgs} rows={n} tickers={}",
            dict.names.len()
        );
    }
    for v in missed.values_mut() {
        v.sort_by_key(|&(d, _)| d);
    }
    Ok(PassAResult {
        stats,
        missed,
        ambiguous,
    })
}
