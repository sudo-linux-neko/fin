# DECISIONS.md — judgment calls (Author: vaibhav, final authority)
SPDX-License-Identifier: GPL-3.0-or-later

Spec: `prompt-md/master_prompt.md` (wins on conflict).
History: `session/session-ses-before-api-change.md` (settled precedent).

## D0. Divergences found in the handoff doc (stated explicitly, per spec)
1. **Master prompt says `Cargo.toml` (linfa/ndarray/csv/serde/...) and `src/` exist — they do not.**
   Verified 2026-09-19: `/home/neko/fin` contains only `data/`, `prompt-md/`,
   `session/`, `session-ses-before-api-change.md`. No `Cargo.toml`, no `src/`,
   no `target/`. Rebuilding the workspace from scratch (this commit).
   ML deps (linfa/candle) will be added at Section 5, not before.
2. **License: prior session leaned AGPL-v3; master prompt says GPL-v3 default.**
   Master wins: `GPL-3.0-or-later` in Cargo.toml + LICENSE. Ask vaibhav before
   any public push if AGPL-v3 fits better. Never MIT.
3. **Parquet verified as claimed**: 34,646,258 rows, 34 row groups, 9 cols,
   9,315 tickers, date-sorted, SNAPPY. Matches CSV (`wc -l` − header) exactly.

## D1. `Stock Splits` semantics (measured, not assumed)
Sampled nonzero rows: values are the **split multiplier** (new shares per old
share): 2.0 = 2-for-1, 1.25, 1.05/1.03/1.02 = small stock dividends, and
0.10/0.05/0.04 = **reverse** splits (1-for-10 etc.). So
`AdjustmentFactor_t = (1/SplitRatio_t)` with `SplitRatio_t = value or 1.0`,
guarded: values ≤ 0 treated as 1.0 (data error, never invert-garbage).
Reverse splits get factor > 1 (e.g. ×10) — correct: pre-reverse prices scale up.

## D2. Dividend guard
`Dividends` are cash amounts on the ex-date. Factor `(1 − Div/Close)` is
clamped: ratio to [0, 0.99], i.e. per-event factor floor 0.01. A ratio ≥ 1 is
a data error (or an unhandled spin-off); without a clamp one bad row zeroes
all history before it. Special dividends >50% are rare; the clamp only binds
on garbage. Logged, not silent: `verify` reports rows where the clamp bound.

## D3. `Open == 0.0` means missing (measured: 1962 rows, e.g. ED/CVX day one)
Kept as 0.0 through adjustment (0×f = 0) and documented. Consumers must use
Close. No invented opens — fabricating them would be lookahead-neutral but
still fabrication.

## D4. ADV uses RAW dollar volume, not adjusted
Dollar volume = raw Close × raw Volume. Splits change price and share-count
in opposite directions, so raw dollar volume is already corporate-action
invariant; using adjusted prices would inject the cumulative factor into a
liquidity measure for no reason. Window: trailing 30 **trading rows**
(~30 sessions), causal only. Gate needs ≥ 30 rows of history: young tickers
(≤30 rows) fail ADV until seasoned — conservative, zero lookahead.

## D5. Market-cap gate: WIRED BUT PENDING (no shares-outstanding anywhere)
The CSV/parquet has no shares-outstanding column, so market cap is
**uncomputable** from price data alone. Rather than faking it:
- `universe_pass = price_pass && adv_pass && !halted_out` (fully enforced),
- the mcap ≥ $2B gate is implemented as a separate nullable predicate that
  resolves when Section 4c lands SEC shares-outstanding (`shares_outstanding
  × adj_close`), at which point `universe_pass &= mcap_pass` with no rework.
Until then every report states the universe is price+ADV filtered only.

## D6. Halt rule exact semantics (spec §4a)
Volume == 0 → `Tradable=false`, Close forward-filled from previous **adjusted**
close. `consec_untradable > 5` → excluded from the *selection universe that
day* (`universe_pass=false`), history rows retained. Uses only past data —
no halt lookahead by construction.

## D7. No frames for values; PIT on `filed` date (carried from prior session)
Recorded here so Section 4c cannot regress: `xbrl/frames/` reflects restated
state → lookahead. Source of truth is `companyfacts` + `submissions`
filing dates; live inference consumes the same as-of snapshot as backtest.
Delisting: Shumway–Warther imputes still current best practice per 2025 SAFE
preprocessed-CRSP doc (−30% NYSE/Amex, −55% Nasdaq performance delists with
missing dlret); liquidation logic keys off filing/effectiveness dates only.

## D11. CORRECTION: vendor prices are already back-adjusted — do NOT re-adjust
(Found 2026-09-19 by the verify gate; this supersedes D1/D2's formula
application, not the formula itself, which remains correct FOR RAW DATA.)
Evidence:
- AAPL printed **124.20 on 2020-08-28** (Friday before the 4:1 ex-date
  2020-08-31) — the post-split price, not the ~$497 raw print. The 4.0 event
  row sits on a continuous series.
- NVDA's 10:1 (ex 2024-06-10) has **no event row at all**, yet prices are
  continuous — events are inconsistently recorded, so self-adjustment would
  also be incomplete as well as double.
- IP 1962-11-19: a 9.3%-yield dividend (+1.02 stock dividend) with NO ex-div
  drop (close rose 0.7049 → 0.7135) — dividend-adjusted too.
- First attempt (applying the §4a formula) produced adjusted ratios ≈ k
  across recorded k:1 events — the signature of double-adjustment. The gate
  caught it; the adjustment was removed, not tuned.
Consequences:
- `adj_*` = vendor values verbatim. `raw_close` (same values) feeds ADV so
  the column stays meaningful if a raw source is ever swapped in.
- verify.rs's forward-split continuity check now proves the VENDOR's
  adjustment (hard gate ±35% on ≥$1 forward splits) instead of ours.
- Anomaly ledger: 3x `inf` splits (SBR×2, WES), ~697 NaN split markers,
  91,215 rows (0.26%) with Close ≤ 0 (excluded from universe by the $5 gate).
- If a future raw (unadjusted) source is added, the §4a formula + guards
  from git history apply — to that source only, never this file.

## D12. Split-gate residuals: 23 idiosyncratic vendor defects, 3 audited allowlist
After vendor-adoption + 77 missed-split repairs, 23/4649 forward events still
trip the ±35% gate. Audited windows show three families, none a missed split:
1. **Stale zero-volume prints** (e.g. INDB/CHCO/TMP 1.5x on Vol=0, reverting
   next session): already neutralized by the halt rule (forward-filled in
   output); most don't trip the gate on output values at all.
2. **Genuine explosive moves on spurious-marker days** (LIVE +37% on 1.4M sh,
   TLSA +41% on 29.9M sh, both with continuation): real price discovery,
   markers coincidental. Kept as-is; "repairing" would fabricate history.
3. **DHR 2016-07-05 Fortive spin-off** (+61% mega-cap print, 1 FTV per 2 DHR
   per Danaher 8-K, $24.56 distribution recorded): vendor spin handling
   deficient, level-shift direction unrecoverable from price data alone, so
   NO repair is applied. Bounded by 1/99 winsorization (1 ticker-day of
   7.28M universe rows). Correct fix (CRSP-style distribution-value
   adjustment using recorded dividend + FTV series) deferred to §4c/§5.
Gate policy: non-universe violations are reported but inert downstream
(§5 consumes universe_pass rows only); universe violations fail unless on
the audited allowlist in verify.rs (LIVE/TLSA/DHR, 3 rows, rationale inline).

## D13. Repair PIT discipline (§5 contract — no lookahead via repairs)
`repair_factor` bakes event-at-T knowledge into rows dated < T. The §5
dataset must therefore apply each factor in `data/repair_events.json`
({ticker, date, factor}) ONLY when its as-of date is >= the event date —
i.e. reconstruct the contemporary-visible series per as-of cutoff, never
read the stored adjusted value for dates before a not-yet-occurred event.
Magnitude if violated: tiny (77 tickers' level history), but the rule is
structural and free. Companion rule: all signals execute with >= 1-day lag
(rebalance on close T trades at T+1 or later).

## D14. SEC fetch design (`fin-fetch`, measured 2026-09-19)
- Bulk zips (rebuilt nightly ~3am ET): companyfacts.zip **1.41GB**,
  submissions.zip **1.56GB**, both `accept-ranges: bytes` → true Range resume.
- Per-CIK JSON on data.sec.gov **ignores Range** (measured: Range request
  returned 200 + full 3.79MB AAPL body) → resume there is per-FILE restart
  (files are MB-scale, fine); checkpointing is per completed+validated file.
- `Accept-Encoding: identity` everywhere: with auto-gzip, bytes-on-wire ≠
  file bytes and Range-resume of bulk zips would be invalid.
- UA: Chrome token + contact (D10) — SEC requires contact info; user
  directed Chrome-facing UA.
- Pacing: ≥150ms between API request starts (≤6.6 rps < SEC 10 rps),
  6 concurrent per-CIK, bulk strictly sequential (2 files).
- Lossless: Done only after validation (JSON parses + required keys, zips
  open); atomic state (tmp+rename) every 25 files + on Ctrl-C (exit 130).
  Tested live: SIGINT mid-bulk kept .parts + Pending state; re-run appended
  (12→21MB, 6→22MB), never restarted.
- Outages: per-file backoff (cap 60s, 40 attempts → Failed, run continues),
  global watchdog sleeps 60s after 25 consecutive failures, forever — the run
  never needs a manual restart for network loss. `verify` requeues corrupt.
- Strict §4b by default: `fetch` downloads only the 4,401 ever-passing
  tickers (→ 3,464 distinct CIKs, 6,928 files), never the full 9,315 panel.
  `--all` overrides. (mcap gate still pending §4c; panel shrinks further.)

## D8. Tensor/autodiff choice deferred to Section 5
Prior session favored Candle over tch-rs (pure Rust, no 100MB+ libtorch,
`?`-errors not segfaults). Master prompt orders re-verification before
committing — doing that at Section 5, not now. Section 4 needs only
parquet+arrow+rayon.

## D9. Throughput model for Section 6 LMs (analytical; MEASURE before building)
Train FLOPs ≈ 6·N·D/token. i5-1335U sustained ≈ 100 GFLOPS f32 (conservative
for AVX2+FMA, memory-bound): 100M-param → ~6e8 FLOP/tok → ~150 tok/s;
20M-param → ~800 tok/s. 100M tokens one epoch: ~7–8 days (100M model),
~1.5 days (20M). Verdict preview: from-scratch pretraining at 100M is
borderline-impractical; fine-tuning a small open-weight model is the likely
fallback. vaibhav decides at Section 6 after a **measured** micro-benchmark,
not this estimate.

## D10. User-supplied values (2026-09-19)
- SEC UA: `fin-research/0.1 (Vaibhav; mailto:nekomeowpurr@proton.me)` with a
  Chrome token appended for non-SEC hosts that block non-browser UAs.
  Fallback contact: nekocodecool@proton.me.
- Position sizing: engine finds best capital allocation for max return at
  least risk; hard cap 10% per single stock; portfolio size asked at run
  start, default $1,000,000, changeable.
- Reports: static PDF (portable: Windows/Android/macOS, embedded PNG charts)
  PLUS an interactive report (recompute on new inputs; risk/position-size/
  simulation widgets). Monte-Carlo replacement to be research-selected at
  Section 6 (candidates: Sobol quasi-MC, historical bootstrap, Cornish-Fisher).
