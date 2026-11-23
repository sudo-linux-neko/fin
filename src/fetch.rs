// Author: vaibhav (final authority over all project decisions)
// SPDX-License-Identifier: GPL-3.0-or-later
//
// fin-fetch: SEC EDGAR downloader with lossless pause/resume, auto-retry
// through outages, and a live progress UI (spec §4c, DECISIONS.md D14).
//
//   fin-fetch smoke    # end-to-end test: ticker map + 3 CIKs, validates all
//   fin-fetch bulk     # nightly bulk zips (~3GB): Range-resumable
//   fin-fetch fetch    # per-CIK submissions+companyfacts for universe tickers
//   fin-fetch verify   # re-validate everything, requeue corrupt/missing
//
// Lossless resume: every completed file is validated BEFORE it is marked
// done; state saves atomically (tmp+rename) every 25 files, at end, and on
// Ctrl-C. Re-running any command resumes exactly where it stopped — no
// completed byte is ever re-downloaded. Network loss never aborts: per-file
// exponential backoff + a global watchdog that sleeps through outages.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// ---------------------------------------------------------------- constants

/// Contact-bearing UA (SEC requirement) + Chrome token (user direction:
// Chrome-faced UAs get blocked less on bot-sensitive hosts). Identity
// encoding everywhere so Range-resume stays byte-exact (D14).
const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 fin-research/0.1 (Vaibhav; mailto:nekomeowpurr@proton.me)";
const URL_MAP: &str = "https://www.sec.gov/files/company_tickers.json";
const URL_BULK_FACTS: &str = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip";
const URL_BULK_SUBS: &str =
    "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip";
const ROOT: &str = "data/sec_edgar";
/// Minimum gap between API request starts: <= ~6.6 rps, under SEC's 10 rps.
const PACE_MS: u64 = 150;
const CHUNK_TIMEOUT_S: u64 = 60;
const MAX_ATTEMPTS: u32 = 40;
/// Consecutive failures before the watchdog sleeps through a suspected outage.
const WATCHDOG_TRIP: u32 = 25;

// ---------------------------------------------------------------- state

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Status {
    Pending,
    Done,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileEntry {
    url: String,
    path: String,
    /// map | submissions | companyfacts | bulk
    kind: String,
    status: Status,
    attempts: u32,
    size: Option<u64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    version: u32,
    files: HashMap<String, FileEntry>,
}

impl State {
    fn path() -> PathBuf {
        PathBuf::from(ROOT).join("state.json")
    }
    fn load() -> Result<Self> {
        let p = Self::path();
        if !p.exists() {
            return Ok(Self {
                version: 1,
                files: HashMap::new(),
            });
        }
        let raw = std::fs::read_to_string(&p).context("read state.json")?;
        serde_json::from_str(&raw).context("parse state.json")
    }
    /// Atomic: write tmp + rename. Lossless pause depends on this.
    fn save(&self) -> Result<()> {
        let p = Self::path();
        if let Some(dir) = p.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = p.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_string_pretty(self)?)?;
        std::fs::rename(&tmp, &p)?;
        Ok(())
    }
}

// ---------------------------------------------------------------- context

struct Ctx {
    client: reqwest::Client,
    mp: MultiProgress,
    state: Mutex<State>,
    saved_counter: Mutex<u32>,
    last_start: Mutex<Instant>,
    shutdown: Arc<AtomicBool>,
    watchdog: Mutex<u32>,
    bulk_mode: bool,
}

impl Ctx {
    fn new(mp: MultiProgress, shutdown: Arc<AtomicBool>, bulk_mode: bool) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(UA)
            .timeout(Duration::from_secs(CHUNK_TIMEOUT_S + 30))
            .build()
            .context("build http client")?;
        Ok(Self {
            client,
            mp,
            state: Mutex::new(State::load()?),
            saved_counter: Mutex::new(0),
            last_start: Mutex::new(Instant::now() - Duration::from_secs(10)),
            shutdown,
            watchdog: Mutex::new(0),
            bulk_mode,
        })
    }

    /// Pace API requests under the SEC rate limit. Bulk exempt (2 files).
    async fn pace(&self) {
        if self.bulk_mode {
            return;
        }
        loop {
            let wait = {
                let mut last = self.last_start.lock().await;
                let el = last.elapsed();
                if el >= Duration::from_millis(PACE_MS) {
                    *last = Instant::now();
                    None
                } else {
                    Some(Duration::from_millis(PACE_MS) - el)
                }
            };
            match wait {
                None => break,
                Some(d) => tokio::time::sleep(d).await,
            }
        }
    }

    async fn note_success(&self) {
        *self.watchdog.lock().await = 0;
        let mut n = self.saved_counter.lock().await;
        *n += 1;
        if *n % 25 == 0 {
            let st = self.state.lock().await;
            let _ = st.save();
        }
    }

    /// Returns true if the run should keep going (false only on Ctrl-C).
    async fn note_failure(&self, id: &str, why: &str) -> bool {
        let mut w = self.watchdog.lock().await;
        *w += 1;
        self.mp
            .println(format!("[retry] {id}: {why} (outage streak {})", *w))
            .ok();
        if *w >= WATCHDOG_TRIP {
            *w = 0;
            drop(w);
            self.mp
                .println("[watchdog] many failures in a row — likely no connection. Sleeping 60s, then auto-resuming…")
                .ok();
            for _ in 0..60 {
                if self.shutdown.load(Ordering::Relaxed) {
                    return false;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        true
    }

    fn shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------- download

fn style_total() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner:.green} {msg} [{wide_bar:.cyan/blue}] {pos}/{len} eta:{eta}",
    )
    .unwrap()
    .progress_chars("=>-")
}

fn style_file() -> ProgressStyle {
    ProgressStyle::with_template(
        "  {msg} [{bar:30.green/yellow}] {bytes}/{total_bytes} {bytes_per_sec} eta:{eta}",
    )
    .unwrap()
    .progress_chars("=>-")
}

fn style_spin() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.yellow} {msg}").unwrap()
}

/// Download one entry. Bulk zips resume via Range; data.sec.gov JSON
/// ignores Range (measured: 200 + full body), so those restart per attempt.
/// A file is marked Done only after validation — never on bytes alone.
async fn download_one(ctx: Arc<Ctx>, id: String) -> Result<bool> {
    let (url, path, kind, resumable) = {
        let st = ctx.state.lock().await;
        let e = st.files.get(&id).context("missing queue entry")?;
        (
            e.url.clone(),
            PathBuf::from(&e.path),
            e.kind.clone(),
            e.kind == "bulk",
        )
    };
    if let Some(dir) = path.parent() {
        tokio::fs::create_dir_all(dir).await?;
    }
    let part = path.with_extension("part");
    // Already complete from a previous run.
    if path.exists() {
        let ok = validate_file(&path, &kind).is_ok();
        let mut st = ctx.state.lock().await;
        if let Some(e) = st.files.get_mut(&id) {
            e.status = if ok { Status::Done } else { Status::Pending };
        }
        if ok {
            ctx.note_success().await;
            return Ok(true);
        }
        let _ = tokio::fs::remove_file(&path).await;
    }

    let fb = ctx.mp.add(ProgressBar::new_spinner());
    fb.set_style(style_spin());
    fb.set_message(id.to_string());

    let mut attempt: u32 = 0;
    loop {
        if ctx.shutting_down() {
            fb.abandon_with_message(format!("{id} paused"));
            return Ok(false);
        }
        attempt += 1;
        if attempt > MAX_ATTEMPTS {
            fb.abandon_with_message(format!("{id} failed after {MAX_ATTEMPTS} attempts"));
            let mut st = ctx.state.lock().await;
            if let Some(e) = st.files.get_mut(&id) {
                e.status = Status::Failed;
                e.attempts = attempt;
            }
            let _ = st.save();
            return Ok(true); // failed, but run continues with the rest
        }
        if attempt > 1 {
            let backoff = std::cmp::min(1u64 << std::cmp::min(attempt, 6), 60);
            fb.set_message(format!("{id} retry {attempt} in {backoff}s…"));
            for _ in 0..backoff {
                if ctx.shutting_down() {
                    fb.abandon_with_message(format!("{id} paused"));
                    return Ok(false);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        ctx.pace().await;

        // Resume offset: only bulk (server honors Range there).
        let have: u64 = if resumable && part.exists() {
            tokio::fs::metadata(&part)
                .await
                .map(|m| m.len())
                .unwrap_or(0)
        } else {
            let _ = tokio::fs::remove_file(&part).await;
            0
        };
        let mut req = ctx.client.get(&url);
        if resumable && have > 0 {
            req = req.header("Range", format!("bytes={have}-"));
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                if !ctx.note_failure(&id, &format!("connect: {e}")).await {
                    fb.abandon_with_message(format!("{id} paused"));
                    return Ok(false);
                }
                continue;
            }
        };
        let status = resp.status();
        // Server ignored Range (API JSON always does): restart cleanly.
        let (mut out, downloaded) = if have > 0 && status.as_u16() != 206 {
            let _ = tokio::fs::remove_file(&part).await;
            (
                tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&part)
                    .await?,
                0u64,
            )
        } else {
            (
                tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .append(true)
                    .open(&part)
                    .await?,
                if status.as_u16() == 206 { have } else { 0 },
            )
        };
        if !status.is_success() && status.as_u16() != 206 {
            if !ctx.note_failure(&id, &format!("http {status}")).await {
                fb.abandon_with_message(format!("{id} paused"));
                return Ok(false);
            }
            continue;
        }
        let total = resp.content_length().map(|l| l + downloaded);
        fb.set_style(style_file());
        if let Some(t) = total {
            fb.set_length(t);
            fb.set_position(downloaded);
        }
        fb.set_message(id.clone());

        let mut stream = resp.bytes_stream();
        let mut ok = true;
        // 1s poll slices: Ctrl-C stays responsive (<=1s) even if the server
        // stalls mid-stream; 60 consecutive empty slices = stall retry.
        let mut idle_slices = 0u32;
        use tokio::io::AsyncWriteExt;
        loop {
            if ctx.shutting_down() {
                ok = false;
                break;
            }
            let chunk = match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
                Ok(Some(Ok(c))) => {
                    idle_slices = 0;
                    Some(c)
                }
                Ok(Some(Err(e))) => {
                    fb.set_message(format!("{id} chunk error, resuming…"));
                    if !ctx.note_failure(&id, &format!("chunk: {e}")).await {
                        fb.abandon_with_message(format!("{id} paused"));
                        return Ok(false);
                    }
                    ok = false;
                    break;
                }
                Ok(None) => break, // stream done
                Err(_) => {
                    idle_slices += 1;
                    if idle_slices >= CHUNK_TIMEOUT_S as u32 {
                        fb.set_message(format!("{id} stalled 60s, resuming…"));
                        if !ctx.note_failure(&id, "stall").await {
                            fb.abandon_with_message(format!("{id} paused"));
                            return Ok(false);
                        }
                        ok = false;
                        break;
                    }
                    continue;
                }
            };
            let chunk = match chunk {
                Some(c) => c,
                None => break,
            };
            if out.write_all(&chunk).await.is_err() {
                ok = false;
                break;
            }
            fb.inc(chunk.len() as u64);
        }
        if ctx.shutting_down() {
            let _ = out.flush().await;
            drop(out);
            fb.abandon_with_message(format!("{id} paused"));
            return Ok(false);
        }
        if !ok {
            continue; // retry (bulk resumes from .part)
        }
        let _ = out.flush().await;
        drop(out);
        // Validate BEFORE marking done: corrupt bytes never checkpoint.
        match validate_file(&part, &kind) {
            Ok(size) => {
                tokio::fs::rename(&part, &path).await?;
                fb.finish_and_clear();
                let mut st = ctx.state.lock().await;
                if let Some(e) = st.files.get_mut(&id) {
                    e.status = Status::Done;
                    e.attempts = attempt;
                    e.size = Some(size);
                }
                ctx.note_success().await;
                return Ok(true);
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&part).await;
                fb.set_message(format!("{id} invalid ({e}), re-downloading…"));
                if !ctx.note_failure(&id, &format!("invalid: {e}")).await {
                    fb.abandon_with_message(format!("{id} paused"));
                    return Ok(false);
                }
            }
        }
    }
}

/// Validation gate: JSON must parse AND carry the keys §4c needs;
/// zips must open with entries. Returns file size on success.
fn validate_file(path: &Path, kind: &str) -> Result<u64> {
    let size = std::fs::metadata(path)?.len();
    match kind {
        "bulk" => {
            let f = std::fs::File::open(path)?;
            let z = zip::ZipArchive::new(f).context("zip unreadable")?;
            if z.is_empty() {
                anyhow::bail!("zip has no entries");
            }
        }
        "map" => {
            let raw = std::fs::read_to_string(path)?;
            let v: serde_json::Value = serde_json::from_str(&raw)?;
            if v.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                anyhow::bail!("ticker map empty");
            }
        }
        "submissions" => {
            let raw = std::fs::read_to_string(path)?;
            let v: serde_json::Value = serde_json::from_str(&raw)?;
            let ok =
                v.get("cik").is_some() && v.get("filings").and_then(|f| f.get("recent")).is_some();
            if !ok {
                anyhow::bail!("submissions missing cik/filings.recent");
            }
        }
        "companyfacts" => {
            let raw = std::fs::read_to_string(path)?;
            let v: serde_json::Value = serde_json::from_str(&raw)?;
            let ok =
                v.get("cik").is_some() && v.get("entityName").is_some() && v.get("facts").is_some();
            if !ok {
                anyhow::bail!("companyfacts missing cik/entityName/facts");
            }
        }
        _ => anyhow::bail!("unknown kind {kind}"),
    }
    Ok(size)
}

// ---------------------------------------------------------------- queue

fn ensure_entry(st: &mut State, id: &str, url: &str, path: &str, kind: &str) {
    st.files.entry(id.to_string()).or_insert(FileEntry {
        url: url.to_string(),
        path: path.to_string(),
        kind: kind.to_string(),
        status: Status::Pending,
        attempts: 0,
        size: None,
    });
}

/// Distinct tickers from the adjusted parquet.
/// filtered=true (default): only tickers with universe_pass on at least one
/// day — the strict §4b panel (price ≥ $5, 30d ADV ≥ $10M, not halted-out).
/// Tickers that can never enter the book/training are never downloaded.
fn universe_tickers(parquet: &str, filtered: bool) -> Result<Vec<String>> {
    use arrow::array::{BooleanArray, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let f = std::fs::File::open(parquet).context("open universe parquet")?;
    let probe = ParquetRecordBatchReaderBuilder::try_new(f)?;
    let n = probe.metadata().num_row_groups();
    drop(probe);
    let mut ever = std::collections::HashSet::new();
    let mut all = std::collections::HashSet::new();
    for rg in 0..n {
        let f = std::fs::File::open(parquet)?;
        let r = ParquetRecordBatchReaderBuilder::try_new(f)?
            .with_row_groups(vec![rg])
            .build()?;
        for b in r {
            let b = b?;
            let col = b
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("ticker col")?;
            if filtered {
                let up = b
                    .column(14)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .context("universe_pass col")?;
                for i in 0..b.num_rows() {
                    if up.value(i) {
                        ever.insert(col.value(i).to_string());
                    }
                }
            } else {
                for i in 0..b.num_rows() {
                    all.insert(col.value(i).to_string());
                }
            }
        }
    }
    let mut v: Vec<String> = if filtered {
        ever.into_iter().collect()
    } else {
        all.into_iter().collect()
    };
    v.sort();
    Ok(v)
}

/// Left-pad a CIK with zeros to 10 digits.
fn pad10(d: &str) -> String {
    let mut s = d.to_string();
    while s.len() < 10 {
        s.insert(0, '0');
    }
    s
}

/// (ticker, 10-digit CIK) pairs plus tickers with no CIK mapping.
type TickerMap = (Vec<(String, String)>, Vec<String>);

/// ticker -> 10-digit CIK from the downloaded map. Returns (mapped, unmapped).
fn map_tickers(map_path: &Path, tickers: &[String]) -> Result<TickerMap> {
    let raw = std::fs::read_to_string(map_path)
        .context("read ticker map (run fetch --map-first or smoke)")?;
    let v: serde_json::Value = serde_json::from_str(&raw)?;
    let mut t2c: HashMap<String, String> = HashMap::new();
    if let Some(obj) = v.as_object() {
        for e in obj.values() {
            if let (Some(t), Some(c)) = (
                e.get("ticker").and_then(|x| x.as_str()),
                e.get("cik_str").and_then(|x| x.as_u64()),
            ) {
                t2c.insert(t.to_uppercase(), format!("{c:010}"));
            }
        }
    }
    let mut mapped = Vec::new();
    let mut unmapped = Vec::new();
    for t in tickers {
        match t2c.get(&t.to_uppercase()) {
            Some(c) => mapped.push((t.clone(), c.clone())),
            None => unmapped.push(t.clone()),
        }
    }
    Ok((mapped, unmapped))
}

// ---------------------------------------------------------------- run

async fn run_queue(
    ctx: Arc<Ctx>,
    ids: Vec<String>,
    concurrency: usize,
    label: &str,
) -> Result<bool> {
    use futures::stream::StreamExt;
    let total = ctx.mp.add(ProgressBar::new(ids.len() as u64));
    total.set_style(style_total());
    total.set_message(label.to_string());
    let stream = futures::stream::iter(ids.into_iter().map(|id| {
        let ctx = ctx.clone();
        let total = total.clone();
        async move {
            if ctx.shutting_down() {
                return false;
            }
            let r = download_one(ctx.clone(), id).await;
            total.inc(1);
            matches!(r, Ok(true))
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<bool>>()
    .await;
    // Honest total: report files actually finished, not just visited.
    let finished = stream.iter().filter(|&&x| x).count();
    if ctx.shutting_down() {
        total.abandon_with_message(format!(
            "{label} paused ({finished}/{} files)",
            stream.len()
        ));
    } else {
        total.finish_with_message(format!(
            "{label} finished ({finished}/{} files)",
            stream.len()
        ));
    }
    {
        let st = ctx.state.lock().await;
        let _ = st.save();
    }
    Ok(!ctx.shutting_down() && stream.iter().all(|&x| x))
}

/// Save state, print the summary table, and exit 130 on Ctrl-C pause so
/// shell `&&` chains do NOT continue onto the next command after a pause.
async fn exit_after(ctx: &Arc<Ctx>) -> ! {
    let paused = ctx.shutdown.load(Ordering::Relaxed);
    let st = ctx.state.lock().await;
    let _ = st.save();
    print_summary(&ctx.mp, &st, paused);
    drop(st);
    if paused {
        std::process::exit(130);
    }
    std::process::exit(0);
}

fn print_summary(mp: &MultiProgress, ctx_state: &State, paused: bool) {
    // Clear live bars first so the table never interleaves with progress UI.
    mp.clear().ok();
    let (mut done, mut pend, mut fail, mut bytes) = (0, 0, 0, 0u64);
    for e in ctx_state.files.values() {
        match e.status {
            Status::Done => {
                done += 1;
                bytes += e.size.unwrap_or(0);
            }
            Status::Pending => pend += 1,
            Status::Failed => fail += 1,
        }
    }
    println!("+----------+-------+");
    println!("| done     | {done:>5} |");
    println!("| pending  | {pend:>5} |");
    println!("| failed   | {fail:>5} |");
    if bytes >= 1_000_000_000 {
        println!("| bytes    | {:>4.1} GB |", bytes as f64 / 1e9);
    } else {
        println!("| bytes    | {:>4.1} MB |", bytes as f64 / 1e6);
    }
    println!("+----------+-------+");
    if paused {
        println!("PAUSED losslessly (Ctrl-C). Re-run the same command to resume —");
        println!("completed files are never re-downloaded.");
    } else {
        println!(
            "Resume any time by re-running the same command — completed files are never re-downloaded."
        );
    }
}

// ---------------------------------------------------------------- cli

#[derive(Parser)]
#[command(
    name = "fin-fetch",
    about = "SEC EDGAR downloader: checkpointed, auto-resuming, progress UI (spec §4c)"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Nightly bulk zips (~3GB): companyfacts.zip + submissions.zip.
    /// Always sequential: parallel bulk streams get throttled mid-download
    /// (measured chunk resets). Range-resumable.
    Bulk,
    /// Per-CIK submissions + companyfacts (checkpointed). Strict by default:
    /// only tickers passing the §4b universe gates are fetched.
    Fetch {
        #[arg(long, default_value = "data/prices_adjusted_universe.parquet")]
        universe: String,
        /// Fetch ALL tickers in the file, including ones that never pass
        /// the §4b gates (default: only ever-passing tickers).
        #[arg(long)]
        all: bool,
        /// Only fetch first N CIKs (smoke-scale test of the queue).
        #[arg(long)]
        limit: Option<usize>,
        /// Explicit CIKs (unpadded ok), bypasses the universe mapping.
        #[arg(long)]
        ciks: Option<String>,
        #[arg(long, default_value_t = 6)]
        concurrency: usize,
    },
    /// Re-validate everything on disk; corrupt/missing requeued to Pending.
    Verify,
    /// End-to-end test: ticker map + AAPL/MSFT/DHR, validates + prints proof.
    Smoke,
}

fn shutdown_flag() -> Arc<AtomicBool> {
    let f = Arc::new(AtomicBool::new(false));
    let g = f.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        g.store(true, Ordering::Relaxed);
    });
    f
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    println!(
        "fin-fetch 0.1 — SEC EDGAR downloader (vaibhav) | Ctrl-C pauses losslessly, re-run resumes"
    );
    match cli.cmd {
        Commands::Bulk => {
            let mp = MultiProgress::new();
            let ctx = Arc::new(Ctx::new(mp, shutdown_flag(), true)?);
            let ids = vec![
                "bulk:companyfacts".to_string(),
                "bulk:submissions".to_string(),
            ];
            {
                let mut st = ctx.state.lock().await;
                ensure_entry(
                    &mut st,
                    &ids[0],
                    URL_BULK_FACTS,
                    &format!("{ROOT}/bulk/companyfacts.zip"),
                    "bulk",
                );
                ensure_entry(
                    &mut st,
                    &ids[1],
                    URL_BULK_SUBS,
                    &format!("{ROOT}/bulk/submissions.zip"),
                    "bulk",
                );
                // Skip already-validated files.
                let _ = st.save();
            }
            let _ = run_queue(ctx.clone(), ids, 1, "bulk zips").await?;
            exit_after(&ctx).await;
        }
        Commands::Fetch {
            universe,
            all,
            limit,
            ciks,
            concurrency,
        } => {
            let mp = MultiProgress::new();
            let ctx = Arc::new(Ctx::new(mp, shutdown_flag(), false)?);
            // 1. Ticker map first (single file, needed for CIK mapping).
            {
                let mut st = ctx.state.lock().await;
                ensure_entry(
                    &mut st,
                    "map:company_tickers",
                    URL_MAP,
                    &format!("{ROOT}/company_tickers.json"),
                    "map",
                );
                let _ = st.save();
            }
            let _ = run_queue(
                ctx.clone(),
                vec!["map:company_tickers".to_string()],
                1,
                "ticker map",
            )
            .await?;
            // 2. Build the CIK queue.
            let pairs: Vec<(String, String)> = if let Some(c) = ciks {
                c.split(',')
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .map(|x| {
                        let digits: String = x.chars().filter(|c| c.is_ascii_digit()).collect();
                        (digits.clone(), pad10(&digits))
                    })
                    .collect()
            } else {
                println!(
                    "Scanning universe tickers (strict §4b panel: {})…",
                    if all {
                        "OFF — all tickers"
                    } else {
                        "ON — ever-passing only"
                    }
                );
                let ticks = universe_tickers(&universe, !all)?;
                println!("Universe tickers: {}", ticks.len());
                let (mapped, unmapped) =
                    map_tickers(Path::new(&format!("{ROOT}/company_tickers.json")), &ticks)?;
                println!(
                    "Mapped to CIK: {} | unmapped (delisted/pre-EDGAR/OTC, expected): {}",
                    mapped.len(),
                    unmapped.len()
                );
                mapped
            };
            let take = limit.unwrap_or(pairs.len()).min(pairs.len());
            let mut ids = Vec::new();
            {
                let mut st = ctx.state.lock().await;
                for (_, c) in pairs.iter().take(take) {
                    let sid = format!("sub:{c}");
                    let fid = format!("facts:{c}");
                    ensure_entry(
                        &mut st,
                        &sid,
                        &format!("https://data.sec.gov/submissions/CIK{c}.json"),
                        &format!("{ROOT}/submissions/CIK{c}.json"),
                        "submissions",
                    );
                    ensure_entry(
                        &mut st,
                        &fid,
                        &format!("https://data.sec.gov/api/xbrl/companyfacts/CIK{c}.json"),
                        &format!("{ROOT}/companyfacts/CIK{c}.json"),
                        "companyfacts",
                    );
                    ids.push(sid);
                    ids.push(fid);
                }
                // Only queue what still needs work. Sort+dedup: several
                // tickers (share classes) map to the same CIK.
                ids.sort();
                ids.dedup();
                let keep: Vec<String> = {
                    ids.into_iter()
                        .filter(|id| {
                            st.files
                                .get(id)
                                .map(|e| e.status != Status::Done)
                                .unwrap_or(true)
                        })
                        .collect()
                };
                let _ = st.save();
                ids = keep;
            }
            println!(
                "Queued files needing download: {} (ticker {})",
                ids.len(),
                pairs.len().min(take)
            );
            let _ = run_queue(ctx.clone(), ids, concurrency.clamp(1, 8), "per-CIK").await?;
            exit_after(&ctx).await;
        }
        Commands::Verify => {
            let mut st = State::load()?;
            let (mut ok, mut requeued, mut missing) = (0, 0, 0);
            for e in st.files.values_mut() {
                let p = Path::new(&e.path);
                if e.status == Status::Done && p.exists() {
                    if validate_file(p, &e.kind).is_ok() {
                        ok += 1;
                    } else {
                        let _ = std::fs::remove_file(p);
                        e.status = Status::Pending;
                        requeued += 1;
                        println!("[verify] corrupt, requeued: {}", e.path);
                    }
                } else if e.status == Status::Done {
                    e.status = Status::Pending;
                    missing += 1;
                }
            }
            let _ = st.save();
            println!("valid={ok} corrupt-requeued={requeued} missing-requeued={missing}");
            let mp = MultiProgress::new();
            print_summary(&mp, &st, false);
        }
        Commands::Smoke => {
            let mp = MultiProgress::new();
            let ctx = Arc::new(Ctx::new(mp, shutdown_flag(), false)?);
            {
                let mut st = ctx.state.lock().await;
                ensure_entry(
                    &mut st,
                    "map:company_tickers",
                    URL_MAP,
                    &format!("{ROOT}/company_tickers.json"),
                    "map",
                );
                for c in ["0000320193", "0000789019", "0000313616"] {
                    ensure_entry(
                        &mut st,
                        &format!("sub:{c}"),
                        &format!("https://data.sec.gov/submissions/CIK{c}.json"),
                        &format!("{ROOT}/submissions/CIK{c}.json"),
                        "submissions",
                    );
                    ensure_entry(
                        &mut st,
                        &format!("facts:{c}"),
                        &format!("https://data.sec.gov/api/xbrl/companyfacts/CIK{c}.json"),
                        &format!("{ROOT}/companyfacts/CIK{c}.json"),
                        "companyfacts",
                    );
                }
                let _ = st.save();
            }
            let ids: Vec<String> = {
                let st = ctx.state.lock().await;
                let mut v: Vec<String> = st
                    .files
                    .iter()
                    .filter(|(k, e)| {
                        e.status != Status::Done
                            && (k.starts_with("map:")
                                || k.starts_with("sub:0000320193")
                                || k.starts_with("facts:0000320193")
                                || k.starts_with("sub:0000789019")
                                || k.starts_with("facts:0000789019")
                                || k.starts_with("sub:0000313616")
                                || k.starts_with("facts:0000313616"))
                    })
                    .map(|(k, _)| k.clone())
                    .collect();
                v.sort();
                v
            };
            let _ = run_queue(ctx.clone(), ids, 4, "smoke").await?;
            // Proof: parse + print schema evidence for each smoke file.
            println!("\n--- smoke proof ---");
            let mut fail = 0;
            let raw_map = std::fs::read_to_string(format!("{ROOT}/company_tickers.json"))?;
            let map_v: serde_json::Value = serde_json::from_str(&raw_map)?;
            println!(
                "ticker map: {} entries, AAPL CIK={}",
                map_v.as_object().map(|o| o.len()).unwrap_or(0),
                map_v
                    .as_object()
                    .and_then(|o| o
                        .values()
                        .find(|e| e.get("ticker").and_then(|t| t.as_str()) == Some("AAPL")))
                    .and_then(|e| e.get("cik_str"))
                    .map(|c| c.to_string())
                    .unwrap_or("?".into())
            );
            for (t, c) in [
                ("AAPL", "0000320193"),
                ("MSFT", "0000789019"),
                ("DHR", "0000313616"),
            ] {
                for kind in ["submissions", "companyfacts"] {
                    let p = format!("{ROOT}/{kind}/CIK{c}.json");
                    match std::fs::read_to_string(&p) {
                        Ok(raw) => match serde_json::from_str::<serde_json::Value>(&raw) {
                            Ok(v) => {
                                if kind == "submissions" {
                                    let n = v
                                        .get("filings")
                                        .and_then(|f| f.get("recent"))
                                        .and_then(|r| r.get("form"))
                                        .and_then(|f| f.as_array())
                                        .map(|a| a.len())
                                        .unwrap_or(0);
                                    let name =
                                        v.get("name").and_then(|x| x.as_str()).unwrap_or("?");
                                    println!(
                                        "{t} submissions: name={name} recent_filings={n} bytes={}",
                                        raw.len()
                                    );
                                } else {
                                    let tax = v
                                        .get("facts")
                                        .and_then(|f| f.as_object())
                                        .map(|o| o.len())
                                        .unwrap_or(0);
                                    let tags: usize = v
                                        .get("facts")
                                        .and_then(|f| f.as_object())
                                        .map(|o| {
                                            o.values()
                                                .filter_map(|t| t.as_object())
                                                .map(|o| o.len())
                                                .sum()
                                        })
                                        .unwrap_or(0);
                                    let name =
                                        v.get("entityName").and_then(|x| x.as_str()).unwrap_or("?");
                                    println!("{t} companyfacts: entity={name} taxonomies={tax} tags={tags} bytes={}", raw.len());
                                }
                            }
                            Err(e) => {
                                println!("{t}/{kind}: JSON INVALID ({e})");
                                fail += 1;
                            }
                        },
                        Err(e) => {
                            println!("{t}/{kind}: missing ({e})");
                            fail += 1;
                        }
                    }
                }
            }
            if fail > 0 {
                let st = ctx.state.lock().await;
                let _ = st.save();
                print_summary(&ctx.mp, &st, false);
                anyhow::bail!("smoke failed on {fail} files");
            }
            println!("SMOKE PASS");
            exit_after(&ctx).await;
        }
    }
    Ok(())
}
