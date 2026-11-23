// Author: vaibhav (final authority over all project decisions)
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Independent verification of the adjusted+filtered universe parquet.
// Every check recomputes from the output file itself (not from the
// in-memory state of the adjust run):
// 1. Row-count parity: CSV lines-1 == source parquet rows == output rows.
// 2. 1:1 mapping: per-RG row counts of output match source RGs.
// 3. Date order: non-decreasing dates across the whole output.
// 4. Split-continuity: across any nonzero split event, the adjusted
//    close ratio stays within ±35% (a missed 4:1 split would print -75%).
// 5. Halt accounting: rows with consec_untradable > 5 never pass universe.
// 6. Gate recomputation: universe_pass == price && adv && !halted,
//    price == adj_close >= 5, adv == adv30_dollar >= 10M (NaN fails).
// 7. Head/tail spot checks printed for eyeball audit.

use std::collections::HashMap;
use std::fs::File;

use anyhow::{bail, Context, Result};
use arrow::array::{
    BooleanArray, Date32Array, Float32Array, Float64Array, StringArray, UInt32Array,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

/// Audited allowlist: universe rows whose split-gate trip was human-
/// investigated and proven to be genuine price discovery or a documented
/// vendor artifact that must NOT be "repaired" (repairing would fabricate
/// history). Format: (ticker, date_days_since_epoch, rationale).
/// - LIVE 2014-02-12 (16113): genuine +37% breakout on 1.4M sh (vs ~300k),
///   continued to 59.28/59.46 next sessions. Spurious 3.0 marker.
/// - TLSA 2020-07-31 (18474): genuine +41% on 29.9M sh (vs ~600k) biotech
///   volume day, smooth fade after. Spurious 2.0 marker.
/// - DHR 2016-07-05 (16987): Fortive spin-off artifact (+61% mega-cap print
///   is impossible as price discovery; 1 FTV per 2 DHR distributed 2016-07-02
///   per Danaher 8-K/press release, $24.56 distribution recorded as dividend).
///   Vendor spin handling deficient; level-shift direction unrecoverable from
///   price data alone, so no repair is applied (see DECISIONS.md D12).
///   Bounded by 1/99 winsorization; CRSP-style fix deferred to §4c/§5.
const SPLIT_ALLOWLIST: &[(&str, i32)] = &[("LIVE", 16113), ("TLSA", 18474), ("DHR", 16987)];

fn split_allowlisted(ticker: &str, date: i32) -> bool {
    SPLIT_ALLOWLIST
        .iter()
        .any(|&(t, d)| t == ticker && d == date)
}

pub struct VerifyReport {
    pub csv_rows: u64,
    pub src_rows: u64,
    pub out_rows: u64,
    /// Forward splits (>= 1.0) on stocks with prev adj close > $1: hard gate.
    /// A missed k:1 split prints exactly -(1-1/k); tolerance is ±35%.
    /// Residual idiosyncratic vendor defects (stale zero-volume prints,
    /// garbage OTC rows) are expected at ~1e-5 rate: only violations on
    /// UNIVERSE rows fail the run, because non-universe rows provably
    /// cannot enter the book, the training panel, or any label
    /// (Section 5 consumes universe_pass rows only).
    pub fwd_events: u64,
    pub fwd_violations: u64,
    pub fwd_violations_universe: u64,
    pub max_fwd_jump: f64,
    /// Reverse splits (< 1.0): REPORT ONLY. They cluster in distressed
    /// microcaps (1-for-1000+ with $95k prints) where the price process
    /// itself is discontinuous; a hard gate there would be fiction.
    pub rev_events: u64,
    pub rev_big: u64,
    pub halted_rows: u64,
    pub halted_leaks: u64,
    pub gate_mismatches: u64,
    pub universe_pass: u64,
    pub tradable_false: u64,
    pub date_regressions: u64,
}

fn parquet_num_rows(path: &str) -> Result<(u64, usize)> {
    let f = File::open(path).with_context(|| format!("open {path}"))?;
    let reader = SerializedFileReader::new(f)?;
    let md = reader.metadata();
    Ok((md.file_metadata().num_rows() as u64, md.num_row_groups()))
}

fn parquet_rg_rows(path: &str) -> Result<Vec<u64>> {
    let f = File::open(path).with_context(|| format!("open {path}"))?;
    let reader = SerializedFileReader::new(f)?;
    let md = reader.metadata();
    Ok((0..md.num_row_groups())
        .map(|i| md.row_group(i).num_rows() as u64)
        .collect())
}

fn csv_data_rows(path: &str) -> Result<u64> {
    // memmap + byte scan: 3.3GB in ~1s, no CSV parse needed for a line count.
    let f = File::open(path).with_context(|| format!("open {path}"))?;
    let mmap = unsafe { memmap2::Mmap::map(&f)? };
    let nl = mmap.iter().filter(|&&b| b == b'\n').count() as u64;
    Ok(nl.saturating_sub(1)) // minus header
}

pub fn verify(csv: &str, src: &str, out: &str) -> Result<VerifyReport> {
    let csv_rows = csv_data_rows(csv)?;
    let (src_rows, _) = parquet_num_rows(src)?;
    let (out_rows, out_rgs) = parquet_num_rows(out)?;
    if csv_rows != src_rows {
        bail!("CSV rows {csv_rows} != source parquet rows {src_rows}");
    }
    if src_rows != out_rows {
        bail!("source rows {src_rows} != output rows {out_rows}: mapping is not 1:1");
    }
    let src_rgs = parquet_rg_rows(src)?;
    let out_rgs_rows = parquet_rg_rows(out)?;
    let src_total: u64 = src_rgs.iter().sum();
    let out_total: u64 = out_rgs_rows.iter().sum();
    if src_total != out_total {
        bail!("RG row totals differ: src {src_total} vs out {out_total}");
    }

    let mut rep = VerifyReport {
        csv_rows,
        src_rows,
        out_rows,
        fwd_events: 0,
        fwd_violations: 0,
        fwd_violations_universe: 0,
        max_fwd_jump: 0.0,
        rev_events: 0,
        rev_big: 0,
        halted_rows: 0,
        halted_leaks: 0,
        gate_mismatches: 0,
        universe_pass: 0,
        tradable_false: 0,
        date_regressions: 0,
    };

    // Per-ticker previous adjusted close for split-continuity.
    let mut prev: HashMap<String, f32> = HashMap::with_capacity(16_384);
    let mut last_date = i32::MIN;
    let mut head: Vec<String> = Vec::new();
    let mut tail: Vec<String> = Vec::new();

    for rg in 0..out_rgs {
        let f = File::open(out)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)?
            .with_row_groups(vec![rg])
            .build()?;
        for batch in reader {
            let b: RecordBatch = batch?;
            let dates = b.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
            let ticks = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let ac = b.column(5).as_any().downcast_ref::<Float32Array>().unwrap();
            let spl = b.column(8).as_any().downcast_ref::<Float32Array>().unwrap();
            let trad = b.column(9).as_any().downcast_ref::<BooleanArray>().unwrap();
            let consec = b.column(10).as_any().downcast_ref::<UInt32Array>().unwrap();
            let adv = b
                .column(11)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let pp = b
                .column(12)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let ap = b
                .column(13)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let up = b
                .column(14)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                let dt = dates.value(i);
                let tk = ticks.value(i);
                let close = ac.value(i);
                if dt < last_date {
                    rep.date_regressions += 1;
                }
                last_date = dt;
                if head.len() < 3 {
                    head.push(format!("{dt}:{tk}:{close}"));
                }
                tail.push(format!("{dt}:{tk}:{close}"));
                if tail.len() > 3 {
                    tail.remove(0);
                }

                let s = spl.value(i);
                if s != 0.0 && s != 1.0 && s.is_finite() {
                    if let Some(&p) = prev.get(tk) {
                        if p > 0.0 && close > 0.0 {
                            let ratio = close as f64 / p as f64;
                            let jump = (ratio - 1.0).abs();
                            if s >= 1.0 && p > 1.0 {
                                // Clean universe: forward split on a real price.
                                rep.fwd_events += 1;
                                if jump > rep.max_fwd_jump {
                                    rep.max_fwd_jump = jump;
                                }
                                if jump > 0.35 {
                                    rep.fwd_violations += 1;
                                    if up.value(i) && !split_allowlisted(tk, dates.value(i)) {
                                        rep.fwd_violations_universe += 1;
                                        eprintln!(
                                            "[verify] UNIVERSE-VIOLATION {tk} date={} split={s} prev={p} now={close} ratio={ratio:.4} adv={} vol_tradable={}",
                                            dates.value(i),
                                            adv.value(i),
                                            trad.value(i)
                                        );
                                    } else if up.value(i) {
                                        eprintln!(
                                            "[verify] allowlisted (audited) {tk} date={} ratio={ratio:.4}",
                                            dates.value(i)
                                        );
                                    }
                                    if rep.fwd_violations <= 10 {
                                        eprintln!(
                                            "[verify] FWD split jump {tk} split={s} prev={p} now={close} ratio={ratio:.4} universe={}",
                                            up.value(i)
                                        );
                                    }
                                }
                            } else if s < 1.0 {
                                // Distressed-microcap reverse split: report only.
                                rep.rev_events += 1;
                                if jump > 0.35 {
                                    rep.rev_big += 1;
                                }
                            }
                        }
                    }
                }
                prev.insert(tk.to_string(), close);

                if !trad.value(i) {
                    rep.tradable_false += 1;
                }
                let halted = consec.value(i) > 5;
                if halted {
                    rep.halted_rows += 1;
                    if up.value(i) {
                        rep.halted_leaks += 1;
                    }
                }
                let exp_pp = close >= 5.0;
                let a = adv.value(i);
                let exp_ap = !a.is_nan() && a >= 10_000_000.0;
                let exp_up = exp_pp && exp_ap && !halted;
                if pp.value(i) != exp_pp || ap.value(i) != exp_ap || up.value(i) != exp_up {
                    rep.gate_mismatches += 1;
                }
                if up.value(i) {
                    rep.universe_pass += 1;
                }
            }
        }
        eprintln!("[verify] RG {rg}/{out_rgs} checked");
    }

    println!("HEAD: {head:?}");
    println!("TAIL: {tail:?}");
    Ok(rep)
}
