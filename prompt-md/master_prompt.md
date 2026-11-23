# MASTER BUILD PROMPT — Institutional Fundamental Equity Ranking System
### For: Muse Spark (OpenCode Zen, paid tier, max/high thinking mode)
### Project root: `/home/neko/fin`

You are being handed a project with **zero code written** but a large amount of settled research and decisions. A companion file, `session-ses-before-api-change.md`, contains the full prior planning history — read it **in full, before writing anything** — including several rounds of adversarial flaw-finding that already caught real bugs (lookahead bias via SEC frames API, non-differentiable rank losses, O(N²) memory blowups, unrealistic Sharpe assumptions, stale delisting-return models). Treat that document as settled precedent: do not silently re-decide things it already resolved. If you find an actual technical error in it, fix it and say so explicitly — don't just quietly diverge.

This document is the **authoritative spec and rulebook**. Where the session log and this document conflict, this document wins.

---

## 1. Hard constraints — never violate these without explicit permission

1. **Language: Rust only.** Do not use any other language for core logic (data pipeline, features, models, training) without asking the user first and giving a full written justification for why Rust cannot do it. This applies even under time pressure.
2. **Author attribution:** every source file's header must credit **vaibhav** as author. Vaibhav holds final authority over all project decisions — if you must make a judgment call, make it, document it, but defer to vaibhav on anything he later overrides.
3. **License:** if this is ever pushed to GitHub or made public, default to **GPL-v3**. Never MIT. Ask the user first if you believe a different license (e.g. AGPL-v3) fits better — but GPL-v3 is the fallback if no answer is given.
4. **Hardware target — build for this machine, not a generic one:**
   - CPU: Intel i5-1335U, 12 threads, AVX2 + FMA available. Use AVX2 explicitly wherever it measurably helps (it almost always will for the ndarray/tensor-heavy code here).
   - RAM: 7.6GB total, running under WSL2 — treat **4–5GB as your real budget**. No swap usage; if something needs swap to run, redesign it (streaming, memory-mapping, chunking) rather than accept the swap.
   - All model training is **CPU-only**. No GPU is available or assumed.
5. **Python tooling** (only for glue/inspection scripts, not core logic): only use `source ~/ml_env/bin/activate`. Never install packages outside that venv, and never touch the system Python.
6. **No lazy shortcuts.** When a design choice is ambiguous, research it (web search, papers, institutional practice) and pick the best-supported answer yourself, then document the reasoning in code comments or a `DECISIONS.md`. Do not stop and block on the user for these — the user has explicitly said to proceed on your own judgment for this class of question. The exceptions are the items in Section 6 ("Must ask the user") — those are financial/account specifics only the user can supply.
7. Work **agentically and end-to-end** — don't just produce a plan, actually build, test, and verify each stage before moving to the next.

---

## 2. Known project state (as of handoff)

- `/home/neko/fin/data/all_stock_data.csv` — raw price history, 3.3GB, 34,646,258 rows, 1962-01-02 → 2024-11-04, 9,315 unique tickers. Columns include Date, Ticker, Open/High/Low/Close, Volume, Dividends, Splits.
- A Parquet conversion of the same data was validated in the prior session (~918MB, SNAPPY, 34 row groups, date-sorted, no row loss) — if it still exists at `data/all_stock_data.parquet`, verify it against the checks below before trusting it; if it's gone, rebuild it the same way.
- **No SEC EDGAR fundamentals data has been downloaded at all.** This must be built from scratch — see Section 4.
- `src/` is empty. `Cargo.toml` currently has: `linfa`, `linfa-trees`, `linfa-ensemble`, `ndarray`, `csv`, `serde`, `rand`, `ndarray-rand`. Add `parquet`, `memmap2`, and a CPU tensor/autodiff crate (`candle` was the prior direction — re-verify it's still the best CPU-only choice before committing) as needed.

Re-verify all of the above yourself before building on it — don't take this document's numbers as gospel if the data has changed.

---

## 3. What "done" looks like (v1 scope)

**v1 is a research/report generator, not a live-trading or broker-connected system.** The end output per run is:

An **institution-grade PDF report**, per candidate trade idea, formatted to the standard of a Tier-1 sell-side equity research note (think: cover page with ticker/rating/price target, investment thesis, financial summary table, price chart with entry/stop/target overlay, risk section, standard research disclosures). Each report must include:
- Proposed entry (buy) price
- Stop-loss level
- Take-profit / price target
- Position size recommendation (see Section 6 — this needs a number from the user first)
- Supporting evidence pulled from (a) the model's own fundamental ranking output, (b) live web research, and (c) the relevant company's latest SEC filings
- A chart of price history with entry/stop/target marked

This is generated by a pipeline of:
1. Cleaned, adjusted, filtered price data (Section 4)
2. A bitemporal point-in-time fundamentals database (Section 4)
3. Cross-sectionally neutralized features (Section 5)
4. A learning-to-rank alpha model (Section 5)
5. Two auxiliary small language models (Section 6) that gather supporting info and draft report prose
6. A Rust-based report renderer producing the final PDF (a mature Rust PDF crate — e.g. `printpdf` or `genpdf` — is fine; this is still "Rust," not a language exception)

Build in this order — each stage should be independently tested and verified before the next depends on it.

---

## 4. Data pipeline (build first)

### 4a. Price cleaning & adjustment
- **Reverse back-adjustment**, anchored at the most recent trading day (`Adjusted_Close = Close` on the last day), walking backward:
  `AdjustmentFactor_t = (1 / SplitRatio_t) × (1 − Dividend_t / Close_t)`, applied cumulatively to all prior rows.
- **Halted/frozen-price handling:** if `Volume == 0` on day t, hold `Close_t = Close_{t-1}` but set a boolean `Tradable = false`. If `Tradable == false` for more than 5 consecutive sessions, drop the ticker from the active ranking universe for that day (not from history — from the *live selection universe*).

### 4b. Universe filtering (strict — apply after adjustment)
Remove any ticker-day that fails **any** of:
- Market cap ≥ $2.0B
- 30-day ADV (average dollar volume) ≥ $10.0M
- Share price ≥ $5.00

Write the result to a new Parquet file; do not overwrite the raw source. Re-verify row/ticker counts against the CSV the way the prior session did (exact row count match, spot-check known tickers/dates).

### 4c. SEC EDGAR fundamentals — bitemporal, from scratch
This is the hardest and highest-risk part of the whole project. Research current SEC EDGAR API best practice before building (rate limits, required User-Agent header format — **you will need a real contact email from the user for this, see Section 6**). Known landmines from the prior session's flaw-finding rounds, already resolved — implement these resolutions, don't relitigate them:
- Do **not** rely on `xbrl/frames/` as the primary panel builder — it reflects current-state (restated) data and leaks lookahead bias into historical backtests.
- Build point-in-time correctly: a value is only visible to the model on the date it was *actually filed* (`first_filed`), never a later-restated value, in **both** backtest and live inference — no train/production distribution shift.
- Handle tag inconsistency (e.g. `Revenues` → `RevenueFromContractWithCustomerExcludingAssessedTax`) as a real structural signal (accounting standard changes), not a naive string-fallback chain — research a robust mapping/normalization approach rather than hardcoding a synonym list.
- Use `companyfacts` as the source of truth (not the frames aggregate), streamed rather than loaded fully into memory (bulk ZIP unpacks into many large irregular JSON files — this must not exceed your RAM budget).
- Handle delisting/corporate-action data without relying on stale static haircuts (old Shumway-style assumptions) — research current practice for realistic delisting-return treatment, and make sure the liquidation-price logic never assumes knowledge of a halt before it happened (that's lookahead bias too).

---

## 5. Feature engineering & alpha model

- **Two-stage winsorization**, cross-sectional, per trading day:
  1. Hard financial boundary caps before Z-scoring (e.g. transform P/E to E/P near-zero-earnings cases rather than letting P/E explode).
  2. Clip to the 1st/99th percentile of that day's cross-section: `X_winsorized = max(p1, min(X, p99))`.
- **Sector neutralization via Ridge regression**, not simple de-meaning: `β = (FᵀF + λI)⁻¹FᵀX`, λ = 1e-3, F = sector dummy design matrix. Residual `X − Fβ` is the neutral feature. This preserves macro/secular sector tilts instead of stripping them out.
- **Ranking model:** resolve the specific flaws already found in the prior session:
  - Replace any raw-rank L1 turnover penalty with a differentiable soft-rank proxy (NeuralSort / Plackett-Luce reparameterization, or a sigmoid soft-rank approximation) — a hard ranking operator has zero/undefined gradients and cannot be backpropagated through directly.
  - If using attention across the cross-section, restrict it to intra-firm temporal structure, or use fixed-size sector-relative normalization — unrestricted cross-sectional attention breaks when the universe composition changes day to day.
  - Weight the turnover penalty toward the tail deciles the loss actually cares about, not uniformly across the whole cross-section.
  - Keep Sharpe/RankIC expectations realistic for pure quarterly fundamental data (RankIC ~0.02–0.05, Sharpe more plausibly 0.4–0.6 than ~0.9 without higher-frequency alternative data) — don't quietly inflate backtest results to hit a target number.
  - Manage the O(N²) pairwise memory cost explicitly for a ~3,000-ticker daily cross-section under the 4–5GB budget — batch/sample pairs deliberately, don't materialize the full pairwise matrix.
- Train on CPU with AVX2. Implement checkpointing so training is resumable without state loss, and show a live progress indicator.

---

## 6. Two auxiliary language models + report generation

- **~20M-parameter model:** ingests live web data and the latest SEC filings for a given ticker, to feed supporting context into the report.
- **~100M-parameter model:** trained to write the actual report prose, with explicit attention to hallucination mitigation (favor retrieval-grounded generation over free generation — cite the specific filing/web source for every factual claim it writes).
- Both trained from scratch on CPU with AVX2, with **lossless pause/resume checkpointing** and a **progress bar** during training.
- **Before investing real time in this:** research and report back actual CPU-only training throughput for models at this scale (tokens/sec on a 12-thread mobile CPU, realistic wall-clock time to a usable checkpoint). If from-scratch pretraining would take an impractical number of weeks, propose fine-tuning a small pretrained open-weight model instead (still runs and trains further on this CPU) as the fallback, and let the user decide which path to take — this is exactly the kind of thing that needs a real number attached before committing to it, not a guess.

### Must ask the user before this part can be finished (do not guess these):
- **SEC EDGAR API requires a User-Agent header with a real name + contact email** — ask the user for what to use.
- **Position sizing needs real numbers:** total capital being allocated to this strategy, and max risk per single trade (e.g. % of capital) — without this, "max amount the strategy can take" per position cannot be computed. Ask directly.
- Whether the report charts should be static images embedded in the PDF or the PDF should carry any interactivity (most report-PDF renderers are static-only — confirm that's acceptable).

---

## 7. Working method

1. Read the full session log first.
2. Build and verify Section 4 completely (with real row-count/spot-check verification, not assumed-correct) before starting Section 5.
3. Before finalizing any major module, run a self-critique pass on your own design the way the prior session's reviewer did — look specifically for lookahead bias, memory blowups, and gradient/differentiability issues, since those are the recurring failure classes in this project.
4. Search the web for current best practice at each stage (SEC EDGAR API changes, current delisting-return research, current soft-ranking literature, current small-LM CPU training benchmarks) rather than relying on cached knowledge.
5. Ask the user only for the items in Section 6 — everything else, make the call yourself, document why, and keep going.
