# CONTEXT_GRAPH — telegram_data_bot

## What this is
Telegram financial bot (Python 3.14): `/regime` bull/bear report with volume
profile for HK tickers, `/recommend` daily long-only index research candidates
(HSI/N225/NDX/SPX/DJI). Two processes: `app.py` (Telegram bot) and
`signal_runner.py` (SQLite snapshot worker, refresh loop).

## Module map
- `analytics/regime.py` — Kalman local-level on log returns, **EWMA(λ=0.94)
  causal vol scaling**, bull prob Φ(m/s), parametric bootstrap CI, persistence
  (steady-state Kalman gain `K=(√(q²+4q)−q)/2`, shock half-life; boundary fit
  ⇒ half-life None, flagged not rejected).
- `analytics/regime_series.py` — **causal historical bull-prob series** for
  features: expanding-window MLE refits every `refit_every` sessions + exact
  manual Kalman updates between refits. Only past closes. NaN before first fit.
- `analytics/recommendations.py` — `daily-logistic-v2` L2 logistic. Features:
  6 base close-return + Parkinson/GK 20/60 (needs `ohlc` frame) + 2 per other
  index (backward `merge_asof` on UTC timestamps — no timezone lookahead) +
  regime prob. `regime_gate_probability` gates trade selection. Strict
  finite-features-after-warmup (missing ⇒ raise). Walk-forward, purge-safe
  labels (entry t+1 close, exit t+6 close).
- `analytics/volume_profile.py` — volume-at-close histogram + weighted KDE,
  S/R zones; optional `decay_halflife` (session-age geometric decay) and
  `value_area` (POC-outward expansion, VAL/VAH).
- `analytics/regime_report.py` — matplotlib PNG + caption (persistence line,
  VA dotted band, decay label).
- `api_data_service/index_history.py` — Yahoo chart endpoint, returns
  **OHLC DataFrame** (open/high/low/close validated + body-bracket check),
  contiguous completed sessions only (close+30min), fail-closed.
- `api_data_service/price_history.py` — bot's own price API adapter
  (`/equity/getTickerHistData`), HK dates, volume modes per_bar/cumulative.
- `recommendation_service.py` — config (`config/recommendations.json`:
  watchlist, cost_bps 35, threshold 0.6, `regime_refit_every` 21,
  `regime_gate_probability` 0.5), SQLite atomic snapshots, input hash covers
  own OHLC + other indices' closes, cache reuse on identical inputs.
- `app.py` — python-telegram-bot 22 async; `/regime` one-at-a-time via
  semaphore, blocking work in threads; passes `regime_volume` config
  (sessions/bins/bandwidth/prominence/decay_halflife/value_area).

## Data flow
Yahoo OHLC (5 indices) → run_scan → per index: others' closes + regime series
→ evaluate_recommendation → SQLite → bot reads cache only.

## Conventions
- Fail closed on data problems; reject, never fill. Documented uncertainty
  everywhere (README is the model spec — keep it in sync).
- Fixed seeds (bootstrap 1729); determinism tested.
- Tests: real numerical fits, offline Telegram transport; `pytest tests -q`.
- venv `.venv` (python3.14), deps via `requirements-dev.txt`.

## Known caveats
- Yahoo OHLC null on a completed session ⇒ index data_error until vendor
  finalizes (pre-existing close behavior).
- Boundary (constant-mean) fits are common on near-martingale indices;
  regime prob then ≈ z-test of sample mean.
- Volume profile is bar-close approximation; index volume not executable.
