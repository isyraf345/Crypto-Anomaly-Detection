# 07 — Isolation Forest Detector

The ML detector. This doc covers how the `if_detector` service actually works — features, training, restart behavior, and tuning. For the conceptual "what is Isolation Forest" math, see [03-anomaly-detection.md](03-anomaly-detection.md).

---

## What the service does

Reads 1-second OHLCV bars from Postgres, computes 9 features per bar, scores each bar against an Isolation Forest model, and writes flagged bars into the `anomalies` table.

Unlike the rule detector (which consumes from Kafka), this service is **Postgres-only** — both training data and scoring input come from the `ohlcv_1s` table. That makes the train/score paths identical and lets retraining use the same data the dashboard sees.

---

## Lifecycle

```
startup
  │
  ▼
wait until ohlcv_1s has ≥ MIN_TRAINING_BARS (1200)   ← ~20 min cold start
  │
  ▼
train_model() on bars from last TRAIN_HOURS (6h)
  │
  ▼
scoring loop:
  ├─ poll new bars since last_seen
  ├─ compute features, score with current model
  ├─ insert anomalies with direction + score + features + 60-bar context
  └─ every RETRAIN_INTERVAL_SEC (30 min): retrain on fresh 6h window
```

### Cold start vs warm start

The 20-minute wait is a check against `COUNT(*) FROM ohlcv_1s`, not a timer. So:

- **Fresh Postgres volume** (`docker compose down -v` then `up`) — waits ~20 min for the aggregator to produce 1200 bars, then trains on whatever fits in the 6h window (≈ 20 min of data).
- **Persisted Postgres volume** (`docker compose down` then `up`) — the count check passes immediately, and training runs on `NOW() - INTERVAL '6 hours'` from Postgres. Pre-restart bars older than 6 hours are excluded.
- **Stopped for longer than 6h** — count check passes but the 6h window returns nothing. Training fails (`MIN_TRAINING_BARS` not met against the feature rows) and the service waits, retrying on the retrain cadence until new bars accumulate.

---

## The 9 features

Computed in `compute_features()`:

| # | Feature | Formula |
|---|---|---|
| 1 | `log_return` | `ln(close / prev_close)` |
| 2 | `return_zscore` | `log_return / pstdev(last 60 returns)` |
| 3 | `log_volume` | `ln(1 + volume)` |
| 4 | `volume_ratio` | `volume / median(last 300 volumes)` |
| 5 | `aggressor_ratio` | `buy_volume / (buy_volume + sell_volume)` |
| 6 | `log_trade_count` | `ln(1 + trade_count)` |
| 7 | `realized_vol` | `pstdev(last 60 returns)` |
| 8 | `vwap_dev` | `(close - vwap) / vwap` |
| 9 | `price_range` | `(high - low) / close` |

Features 1, 2, 4, 7 depend on rolling buffers (60 returns, 300 volumes). During training, the first ~10 bars of any window produce no feature row (`returns_buf` / `volumes_buf` not yet warm) and are silently skipped.

Log-scaling on `volume` and `trade_count` compresses their long right tails so a single 1000-trade burst doesn't dominate every tree split.

---

## Training

```python
SELECT_TRAIN_SQL:
  bucket >= NOW() - INTERVAL '6 hours' ORDER BY bucket ASC

IsolationForest(
    n_estimators=100,
    contamination=0.01,
    random_state=42,
)
```

`contamination=0.01` means the model assumes ~1% of training bars are anomalies and calibrates its internal threshold so `predict()` returns `-1` for the bottom 1% of scores. **This is a calibration assumption, not ground truth** — if the last 6 hours were unusually calm or unusually wild, the threshold shifts accordingly. That's a feature (adapts to regime) and a hazard (a flash-crash inside the training window will train the model to consider crashes "normal").

`random_state=42` makes retrains deterministic given identical training data.

### Why 6 hours

Short enough to follow regime changes (Asia → London → NY sessions look different), long enough that `MIN_TRAINING_BARS=1200` is easily satisfied (6h × 3600s = 21,600 bars maximum).

### Why retrain every 30 min

Cost vs freshness. Fitting on ~20k rows with 100 trees takes a few seconds; doing it twice an hour is cheap. More often than that and the threshold starts to jitter; less often and the model lags the regime.

---

## Scoring

For each new bar:

```python
score = model.decision_function([features])[0]   # continuous, negative = anomalous
pred  = model.predict([features])[0]             # +1 normal, -1 anomaly
if pred == -1:
    insert anomaly with score = abs(score), direction from log_return sign
```

`window_start = bar.bucket - 60s`, `window_end = bar.bucket`. The 60 preceding bars are serialized into `raw_context` (JSONB) for after-the-fact inspection in the dashboard.

---

## Restart and resumption

The service tracks `last_seen` from the `anomalies` table:

```sql
SELECT COALESCE(MAX(window_end), 'epoch'::timestamptz)
FROM anomalies WHERE symbol = %s AND detector = 'isolation_forest'
```

On restart it picks up scoring from there — bars produced while the service was down get scored when it comes back (assuming they're still in Postgres). The rolling buffers (`returns_buf`, `volumes_buf`) are rebuilt from training data, so they're warm before scoring resumes.

---

## Tuning knobs (env vars)

| Var | Default | Effect |
|---|---|---|
| `TRAIN_HOURS` | `6` | Training window length |
| `RETRAIN_INTERVAL_SEC` | `1800` | Seconds between retrains |
| `MIN_TRAINING_BARS` | `1200` | Cold-start gate and training-set floor |
| `CONTAMINATION` | `0.01` | Expected anomaly rate; raises/lowers flag count |
| `POLL_INTERVAL_SEC` | `1.0` | How often to query Postgres for new bars |
| `SYMBOL` | `btcusdt` | Which symbol to score |

Hardcoded (edit the source if you need to change):
- `WINDOW_RETURNS = 60` — returns buffer length
- `WINDOW_VOLUME = 300` — volumes buffer length
- `CONTEXT_BARS = 60` — bars saved with each anomaly
- `N_ESTIMATORS = 100` — number of trees

### Practical tuning

- **Too many flags** → lower `CONTAMINATION` to `0.005` or `0.001`.
- **Too few flags** → raise `CONTAMINATION` to `0.02` or `0.05`.
- **Model feels stale during fast regime changes** → drop `RETRAIN_INTERVAL_SEC` to `900` (15 min) and/or `TRAIN_HOURS` to `3`.
- **Model whipsaws between calm/wild calibrations** → raise `TRAIN_HOURS` to `12` for more stability.

---

## Known limitations

1. **`contamination` is a guess, not a truth.** The model will always flag ~1% of bars regardless of whether anomalies are actually present.
2. **Cold start with `down -v` requires waiting.** No model = no flags for the first ~20 minutes after a fresh volume.
3. **Single symbol per process.** Multi-symbol would need a model per symbol; not implemented.
4. **No persistence of the trained model.** On restart, the model is rebuilt from Postgres — fast enough that this isn't worth fixing.
5. **Features are bar-local.** No cross-bar memory beyond what the rolling buffers and `return_zscore` / `volume_ratio` already encode.

---

## Cross-references

- [03-anomaly-detection.md](03-anomaly-detection.md) — math foundations (z-scores, log returns, IF intuition)
- [04-dashboard-guide.md](04-dashboard-guide.md) — how IF anomalies render in Grafana
- [05-operations.md](05-operations.md) — logs and queries for debugging
- [db/README.md](../db/README.md) — `ohlcv_1s` and `anomalies` schemas
