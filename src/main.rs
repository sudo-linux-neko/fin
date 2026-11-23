// Author: vaibhav (final authority over all project decisions)
// SPDX-License-Identifier: GPL-3.0-or-later
//
// fin-data: price cleaning (§4a) + universe filtering (§4b).
//   fin-data adjust --source data/all_stock_data.parquet \
//       --tmp data/tmp_adj --out data/prices_adjusted_universe.parquet \
//       --stats data/prices_adjusted_universe.stats.json
//   fin-data verify --csv data/all_stock_data.csv \
//       --src data/all_stock_data.parquet \
//       --out data/prices_adjusted_universe.parquet

mod price;
mod universe;
mod verify;

use std::fs::File;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(Parser)]
#[command(
    name = "fin-data",
    about = "price cleaning + universe filtering (spec §4)"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Vendor-adjustment adoption (§4a, see D11) then halt/universe filter (§4b).
    Adjust {
        #[arg(long, default_value = "data/all_stock_data.parquet")]
        source: String,
        #[arg(long, default_value = "data/tmp_adj")]
        tmp: String,
        #[arg(long, default_value = "data/prices_adjusted_universe.parquet")]
        out: String,
        #[arg(long, default_value = "data/prices_adjusted_universe.stats.json")]
        stats: String,
    },
    /// Independently re-verify the output parquet.
    Verify {
        #[arg(long, default_value = "data/all_stock_data.csv")]
        csv: String,
        #[arg(long, default_value = "data/all_stock_data.parquet")]
        src: String,
        #[arg(long, default_value = "data/prices_adjusted_universe.parquet")]
        out: String,
    },
}

fn num_row_groups(path: &str) -> Result<usize> {
    let f = File::open(path).with_context(|| format!("open {path}"))?;
    let r = ParquetRecordBatchReaderBuilder::try_new(f)?;
    Ok(r.metadata().num_row_groups())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Commands::Adjust {
            source,
            tmp,
            out,
            stats,
        } => {
            let mut dict = price::TickerDict::new();
            // Pass A: vendor-adjustment adoption (no re-adjustment, see D11).
            let pa = price::passthrough_pass(&source, &tmp, &mut dict)?;
            let astats = pa.stats;
            // Temp RGs are 1:1 with source RGs.
            let n_rgs = num_row_groups(&source)?;
            let ustats = universe::forward_pass(&tmp, n_rgs, &dict, &pa.missed, &out)?;
            let amb: Vec<_> = pa
                .ambiguous
                .iter()
                .map(|a| {
                    serde_json::json!({
                        "ticker": a.ticker, "date": a.date, "k": a.k,
                        "prev_close": a.prev_close, "event_close": a.event_close,
                    })
                })
                .collect();
            // Repair schedule for §5 PIT discipline: rows with date < T carry
            // knowledge of the event at T, so the dataset must apply each
            // factor only when its as-of date is >= T (see DECISIONS.md D13).
            let names = &dict.names;
            let mut missed_list: Vec<_> = pa
                .missed
                .iter()
                .flat_map(|(&tid, ev)| {
                    ev.iter().map(move |&(d, k)| {
                        serde_json::json!({
                            "ticker": names[tid as usize],
                            "date": d,
                            "factor": 1.0 / k,
                        })
                    })
                })
                .collect();
            missed_list.sort_by(|a, b| {
                a["ticker"]
                    .as_str()
                    .cmp(&b["ticker"].as_str())
                    .then(a["date"].as_i64().cmp(&b["date"].as_i64()))
            });
            std::fs::write(
                "data/repair_events.json",
                serde_json::to_string_pretty(&missed_list)?,
            )?;
            let json = serde_json::json!({
                "rows_in": astats.rows_in,
                "rows_out": astats.rows_out,
                "adjustment": "NONE — vendor pre-adjusted (see DECISIONS.md D11)",
                "split_events": astats.split_events,
                "split_nan_inf": astats.split_nan_inf,
                "split_ok_vendor": astats.split_ok_vendor,
                "split_missed_repaired": astats.split_missed,
                "split_ambiguous": astats.split_ambiguous,
                "ambiguous_events": amb,
                "zero_close": astats.zero_close,
                "div_events": astats.div_events,
                "div_events": astats.div_events,
                "tickers": dict.names.len(),
                "universe_rows": ustats.rows,
                "tradable_false": ustats.tradable_false,
                "halted_out": ustats.halted_out,
                "price_pass": ustats.price_pass,
                "adv_pass": ustats.adv_pass,
                "universe_pass": ustats.universe_pass,
                "repaired_rows": ustats.repaired_rows,
                "mcap_gate": "PENDING — no shares-outstanding in price data (see DECISIONS.md D5)",
            });
            std::fs::write(&stats, serde_json::to_string_pretty(&json)?)?;
            println!("{json:#}");
        }
        Commands::Verify { csv, src, out } => {
            let rep = verify::verify(&csv, &src, &out)?;
            println!(
                "csv={} src={} out={} fwd_splits={} fwd_violations={} (universe={}) max_fwd_jump={:.4} \
                 rev_splits={} rev_big={} \
                 halted={} halted_leaks={} gate_mismatches={} universe={} ({:.2}%) tradable_false={} date_regressions={}",
                rep.csv_rows,
                rep.src_rows,
                rep.out_rows,
                rep.fwd_events,
                rep.fwd_violations,
                rep.fwd_violations_universe,
                rep.max_fwd_jump,
                rep.rev_events,
                rep.rev_big,
                rep.halted_rows,
                rep.halted_leaks,
                rep.gate_mismatches,
                rep.universe_pass,
                100.0 * rep.universe_pass as f64 / rep.out_rows as f64,
                rep.tradable_false,
                rep.date_regressions,
            );
            if rep.fwd_violations_universe > 0
                || rep.halted_leaks > 0
                || rep.gate_mismatches > 0
                || rep.date_regressions > 0
            {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
