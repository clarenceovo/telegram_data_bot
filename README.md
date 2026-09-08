# Telegram financial data bot

## Regime report

`/regime 700` (or `/regime HK.00700`) returns a one-year price chart and the
current bull/bear return regime. `/regime` defaults to `HK.HSImain`.

The existing configured price API supplies the history. No new vendor credentials
are required. The untracked `config/config.json` must contain `api_endpoint`
(host and optional port, as expected by the existing bot) and Telegram tokens.
IG configuration is no longer used.

### Statistical definition

The chart displays closing prices, but **prices are not the model input**.
For consecutive observed dates, `r[t] = ln(close[t] / close[t-1])`, using natural
logarithms (NumPy `log`, base e). The Kalman local-level model is:

```
r[t]  = mean[t] + observation_noise[t]
mean[t] = mean[t-1] + state_noise[t]
```

Both noise terms are independent, zero-mean Gaussian variables with variances
estimated by maximum likelihood. The filter estimates a changing average return;
there is no overlapping moving-average preprocessing. Returns are scaled by
their sample standard deviation for numerical conditioning, without demeaning.
Reported mean returns are restored to natural-log-return units.

Given the latest filtered mean `m` and standard deviation `s`:

- Bull probability: `Phi(m / s)`, meaning the latent current mean is positive.
- Bear probability: `1 - bull_probability`.
- Bull/bear ratio: `bull_probability / bear_probability` (odds, displayed as x:1).
- Mean-return 95% state interval: `m +/- 1.96*s`, conditional on fitted parameters.

These are model probabilities about the **current mean**, not next-day up/down
odds, investor sentiment, or a validated profitable trading signal. Extreme
probabilities are clipped to `[1e-12, 1-1e-12]` to keep numerical odds finite.

The ratio's approximate 95% interval uses 100 seeded parametric bootstrap fits.
Synthetic return histories use the fitted noise variances and fixed initial
mean. Each refit supplies new noise parameters to filter the original observed
history. The 2.5th and 97.5th percentiles of the resulting bull probabilities are
transformed into odds. This measures approximate **parameter uncertainty** under
the assumed model; nominal coverage is unvalidated, tails are coarse with 100
draws, and model misspecification and initial-state uncertainty are excluded.
At least 80% of bootstrap fits must succeed or the command reports failure.

Parameters use the supplied history only to describe its latest date. No
historical regime probabilities or trading-performance claims are displayed.

### Data and operation

- Calls `/equity/getTickerHistData` with `ticker`, `startDate`, and `endDate`.
- Requires a `data` list of records containing `time` and positive finite `close`.
- Naive API event timestamps are assumed to be Hong Kong time; aware timestamps
  are converted to Hong Kong time. Today is excluded to avoid unfinished bars.
- Uses the last observed price per HK date. Official close status, corporate
  action adjustments, trading-calendar completeness, and futures roll
  adjustments are unverified. In particular, roll or split jumps can affect the
  regime. Returns span consecutive **observed sessions**, including any gaps.
- Does not forward-fill; rejects conflicting duplicate timestamps. Requires at
  least 120 observed dates, history starting within 45 days of the one-year
  boundary, and a latest price no more than seven calendar days old.
- HTTP connect/read timeouts are 5/30 seconds. Bootstrap fits have bounded
  iterations; one report runs at a time per bot instance in a Telegram worker.
- Logs elapsed time, observation count, and bootstrap successes. API and fitting
  failures produce a short user-facing error and release the report lock.

### Python 3.14 setup

The runtime targets CPython **3.14**. `requirements.txt` records direct dependency
versions; `requirements.lock` pins the full runtime dependency graph, including
transitive packages. `requirements-dev.txt` installs that lock plus test tools.
Obsolete Tornado, APScheduler, Windows-only certificate, and zoneinfo-backport
pins have been removed. `tzdata` supplies the HK timezone database in slim images.

```sh
python3.14 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
cp config/config.example.json config/config.json
# Edit config/config.json with your API endpoint and Telegram tokens.
ENVIRONMENT=UAT .venv/bin/python app.py
.venv/bin/python -m pytest tests -q
```

The Telegram integration uses python-telegram-bot 22's `Application` and async
callbacks. All replies are awaited. Blocking market API calls and model fitting
run in background threads; `/regime` remains nonblocking with one active report
per instance. The polling entry point owns an explicit event loop using
`asyncio.Runner`, as required by Python 3.14's event-loop behavior. Existing
HTTP requests now have 5-second connect and 30-second read timeouts.

### Docker and Compose

```sh
docker compose config
docker compose up --build -d
docker compose logs -f bot
```

Compose defaults to UAT. Use `ENVIRONMENT=PROD docker compose up --build -d` for
the production token. Direct `python app.py` retains its existing default of
production when `ENVIRONMENT` is not UAT.

The image uses `python:3.14-slim`, installs binary wheels from the runtime lock,
runs `pip check`, and runs as UID 10001. Compose mounts your existing
`config/config.json` read-only; it must be readable by that container user.
A missing config file fails the mount instead of silently creating a directory.
The image copies only application code. `.dockerignore` excludes local config,
Git metadata, virtual environments, and environment files. No inbound port is
needed because Telegram uses outbound polling.

Tests exercise real numerical fits, PNG rendering, Telegram async dispatch with
an offline request transport, and startup event-loop ownership. Live Telegram
and market API calls are not exercised by tests. Docker itself must be installed
to run the image; resolving Linux wheels does not replace a container smoke test.

Model reference: [statsmodels local-level/state-space models](https://www.statsmodels.org/stable/generated/statsmodels.tsa.statespace.structural.UnobservedComponents.html).

### Volume-based support and resistance

`/regime` also displays a volume-at-close histogram, a smoothed KDE curve, point
of control (POC), and up to two candidate support/resistance zones on each side
of the latest close. The closing-price chart still covers one year; the default
volume window is the latest **60 observed completed HK dates**. Shaded zones are
drawn only across that window and describe the latest report, not signals that
were available historically.

The API adapter retains intraday records before deriving daily closing prices.
Each record's volume is assigned to its closing price. This is a **bar-close
approximation**, especially coarse for daily bars; it does not reconstruct
actual trades at each price. Volume units must be consistent within the window.
Corporate-action, futures-roll, and missing-session adjustments remain unverified.

Optional settings in the existing untracked `config/config.json`:

```json
"regime_volume": {
  "mode": "per_bar",
  "sessions": 60,
  "bins": 48,
  "bandwidth": 0.2,
  "prominence": 0.1
}
```

`mode` must be `per_bar` or `cumulative`. The default assumes incremental bar
volume and is labeled in the report; verify this against your API vendor.
Cumulative mode differences counters within each HK calendar date. It treats the
first observed counter as that bar's volume and rejects subsequent decreases;
an incomplete session can therefore attribute earlier unseen volume to the
first observed close. These HK-date boundaries may differ from futures trading
sessions. No automatic inference of the volume convention is attempted.

Model details:

- Histogram weights sum to one. POC is the midpoint of the maximum-volume bin;
  an exact tie selects the lowest-price bin.
- Volume-weighted Gaussian KDE uses SciPy's scalar covariance factor `bandwidth`.
  Peak prominence must exceed `prominence` times the maximum density. Minimum
  peak separation is one KDE standard deviation in price units.
- Zone boundaries are the peak's half-prominence width. Support zones lie wholly
  below the latest close; resistance zones lie wholly above. Qualifying zones
  are ordered by proximity, with at most two on each side. No peak is forced
  when price lies outside the profile or inside a zone.
- Zone volume share is observed volume at closes within those boundaries divided
  by total window volume. It is not estimated from KDE area or a holding probability.
- “Persists” means a nearby peak survives bandwidths of 0.8 and 1.2 times the
  configured factor, within the larger of the base KDE standard deviation and
  the zone half-width. “Sensitive” means this check failed. This is a descriptive
  sensitivity check, not a statistical confidence interval. Lookback changes
  can also change zones; no stability across all windows is claimed.

Supported settings: 20–252 sessions, 8–256 bins, bandwidth 0.01–2.0, and prominence
0.001–1.0. Flat prices, zero volume, insufficient sessions, or unusably concentrated
volume omit the profile with a reason while retaining the return-regime report.
Missing, nonfinite, negative, boolean, or conflicting duplicate volumes also
disable the profile. Volume validation currently covers all returned records
inside the requested year, even when the selected profile window is shorter.

Tests cover volume conservation, multimodal peak discovery, optional-volume
failure, cumulative resets, duplicate handling, lookback isolation, and rendered
Telegram output. Levels are descriptive until separately evaluated in a
chronological strategy study with a defined touch/rejection rule and costs.

References: [SciPy weighted KDE](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.gaussian_kde.html)
and [peak detection](https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.find_peaks.html).

## Daily index recommendations

`/recommend` lists qualifying long-only research candidates for **HSI, N225, NDX,
SPX and DJI**. `/recommend SPX` shows an individual index's evidence and rejection
reasons. An empty shortlist is a valid result. The command reads the worker's
SQLite snapshot; it does not fetch market data or fit models on demand.

Start the independent worker with:

```sh
.venv/bin/python signal_runner.py --once  # fetch, evaluate, save and exit
.venv/bin/python signal_runner.py         # keep refreshing every 30 minutes
# Deploy both processes with persistent shared storage:
docker compose up -d --build
```

The worker needs no Telegram credentials. The bot still requires a valid
`config/config.json`. Configure the watchlist, assumed round-trip costs (default
35 bps), score threshold (0.60), minimum historical trades (30), refresh interval
and cache age in `config/recommendations.json`. Both processes must use matching
configuration. `RECOMMEND_CONFIG` overrides that path; `RECOMMEND_DB` overrides
`data/recommendations.sqlite3`. Worker `--config` takes precedence over the
configuration environment variable. A singleton file lock prevents two workers
from writing the same database. Stop with SIGTERM/Ctrl-C; completed scans publish
atomically, and failed indices replace their prior candidates with error status.
No proactive Telegram messages or orders are sent.

Data comes from Yahoo's **unofficial** public chart endpoint, for example
`https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?range=5y&interval=1d`.
Symbols are `^HSI`, `^N225`, `^NDX`, `^GSPC`, and `^DJI`. The adapter checks symbol,
timezone, finite positive closes, conflicting duplicates, and contiguous completed
exchange sessions. It excludes today's incomplete bar until 30 minutes after the
calendar close, including half days. Known HKEX full-day weather closures in
September 2023 supplement the exchange calendar. Unexpected missing sessions fail
closed; this public endpoint has no service guarantee. Only daily closes are used
for recommendations; index volume is not treated as executable market volume.

The fixed regularized logistic model uses **natural-log daily returns**, trailing
5/20/60-session average returns, and 20/60-session volatility. Prices are never
model features. Scaling and fitting use only past observations whose five-session
outcomes have already matured, with a maximum 504 labeled observations and at least
252 by default. Decision is after session t closes; proxy entry is **t+1 close**,
and exit is **t+6 close**. Displayed entry/exit dates follow each exchange calendar.

Qualification requires a current score at least 0.60, at least 30 completed
nonoverlapping selected trades, a positive lower bound of the approximate 95%
block-bootstrap mean net-return interval, and an out-of-sample Brier score better
than the historical training-frequency baseline. The interface reports the score,
hit rate, trade count, mean net index move and its interval, compounded trade-close
drawdown, Brier comparison, and evaluation dates. Drawdown does not measure intratrade
risk. Scores are uncalibrated; bootstrap intervals do not include model-selection
uncertainty. These gates are research screens, not evidence of a guaranteed edge.
Changing thresholds after looking at results requires fresh untouched validation.

Cash indices cannot be traded directly. Results subtract a configurable assumed
cost from index moves, and do not simulate ETF tracking, futures rolls, financing,
slippage, or fills. SPX, NDX and DJI overlap substantially. Repeated daily candidates
are not position-aware; historical evaluation uses nonoverlapping trades. Select
an executable instrument and validate its costs and risk before using a signal.

Each scan fetches fresh history, reusing calculations only when the complete input
and configuration are unchanged. Snapshots older than two hours, mismatched
configuration, a newly completed exchange session, or a passed entry close hide
old candidates. The first live five-index scan returned no qualifying ideas due
to insufficient selected-trade evidence; thresholds were left unchanged.
