# 03 — Anomaly Detection: the Math

How each detector decides "this bar is unusual." We have four detectors total — three rule-based and one ML.

---

## What is an "anomaly" in our context?

Vague but useful definition: **a 1-second bar that doesn't look like the recent past.**

The "recent past" is captured by **rolling windows** (e.g., the last 60 seconds for price stats, last 300 seconds for volume stats). When a new bar arrives, we compare it to its rolling-window peers using one of several techniques.

Every anomaly we flag is stored in the `anomalies` table with:
- `detector` — which detector fired (`price_zscore`, `volume_spike`, `aggressor_imbalance`, `isolation_forest`)
- `direction` — `pump` (price moved up), `dump` (price moved down), `unknown`
- `score` — strength of the signal (interpretation depends on detector)
- `features` — JSONB with the values that led to flagging
- `raw_context` — JSONB with the last 60 bars for after-the-fact investigation

---

## Foundation: rolling windows

A **rolling window** is the last *N* observations as they slide forward in time. Example: rolling 60-second window over volume means "the volumes of the last 60 bars I've seen."

We maintain these in memory as Python `collections.deque` with a max length:

```python
volumes_buf = deque(maxlen=300)  # last 5 minutes of volume
volumes_buf.append(new_volume)   # drops the oldest automatically
```

Why rolling? Because the "normal range" of crypto price/volume shifts during the day. What's a 10× volume spike at 3 AM might be totally normal at the US market open. A rolling window adapts.

---

## Foundation: log returns

We don't compute price changes as raw differences. We use **log returns**:

$$
r_t = \ln\left(\frac{P_t}{P_{t-1}}\right)
$$

Where $P_t$ is the close price of the current bar and $P_{t-1}$ is the close of the previous bar.

### Why log returns?

1. **Additive over time** — $\ln(P_3/P_0) = \ln(P_3/P_2) + \ln(P_2/P_1) + \ln(P_1/P_0)$. So a 1-hour return is the sum of 60 one-minute returns. Clean.
2. **Symmetric** — a +10% move followed by a −10% move doesn't get you back to where you started in regular returns, but in log returns +0.0953 + (−0.1054) ≈ −0.01 reflects the actual small loss correctly.
3. **Approximately normal in idealized markets** — z-scores on log returns are statistically more meaningful than on raw returns.

For tiny moves, $\ln(1 + x) \approx x$, so log return ≈ percent return. The difference only matters for big moves — which is exactly when we care about precision.

---

## Foundation: z-score

The **z-score** measures how many standard deviations a value is from its mean:

$$
z = \frac{x - \mu}{\sigma}
$$

Where:
- $\mu$ = mean of the rolling window
- $\sigma$ = standard deviation of the rolling window
- $x$ = the new value being scored

Interpretation:
- $|z| < 2$ — typical
- $|z| \approx 3$ — uncommon (~0.3% of samples in a normal distribution)
- $|z| > 4$ — rare, possibly anomalous (~0.006%)
- $|z| > 6$ — extremely rare, almost certainly a real signal

Financial returns have **fat tails** (more extreme values than a normal distribution would predict), so a z=4 in real markets is not as rare as the normal distribution suggests. Still, it's a sane threshold for flagging unusual behavior.

We use **population std dev** (`statistics.pstdev`) since we're treating the rolling window as the entire population at that moment.

---

## Detector 1 — `price_zscore`

**Fires when:** the latest 1-second log return is more than 4 std devs from the rolling 60-second mean.

**Math:**
```
returns = rolling_window(60) of log returns
mu      = mean(returns)
sigma   = pstdev(returns)
z       = (last_return - mu) / sigma

if |z| >= 4 AND |last_return| >= 1e-4:  flag
```

The `|last_return| >= 1e-4` guard prevents false flags during dead-quiet periods. When the market is barely moving, std dev shrinks to near-zero, and any tiny tick becomes a "huge" z-score in proportional terms. We require the actual move to be at least 0.01% before we care.

**Score:** $|z|$ — bigger = stronger signal.

**Direction:** `pump` if $r > 0$, `dump` if $r < 0$.

---

## Detector 2 — `volume_spike`

**Fires when:** the volume of the current bar is more than 10× the rolling 5-minute median, AND volume exceeds an absolute minimum.

**Math:**
```
volumes      = rolling_window(300) of bar volumes
vol_median   = max( median(volumes), 0.05 BTC )       # floor to prevent absurd ratios
spike_ratio  = current_volume / vol_median

if spike_ratio >= 10 AND current_volume >= 0.05 BTC:  flag
```

**Why median, not mean?** Volumes are spiky and right-skewed. A few huge bars would yank the mean way up, making it harder to detect spikes. The median is robust to outliers — exactly what we want.

**Why the floor on median?** During very quiet periods (e.g., 3 AM weekend) the median can drop to near zero, and any normal bar would register as a 10,000× spike. Flooring the median at 0.05 BTC keeps the metric sane.

**Score:** the spike ratio itself (e.g., `82.5` = volume was 82.5× the median).

**Direction:** `pump`/`dump` from the sign of the same bar's log return; `unknown` if return was zero.

---

## Detector 3 — `aggressor_imbalance`

**Fires when:** one side of the market dominated the bar's volume by more than 95%.

**Math:**
```
agg_ratio = buy_volume / (buy_volume + sell_volume)

if agg_ratio >= 0.95 AND volume >= 0.5 BTC:  flag (pump)
if agg_ratio <= 0.05 AND volume >= 0.5 BTC:  flag (dump)
```

**The intuition:** in a healthy market, both buyers and sellers are active. When you see *only* market buys hitting the book for a full second AND the volume is meaningful, that's directional pressure — someone wants in (or out) urgently.

The `volume >= 0.5 BTC` guard prevents flagging tiny single-trade bars where the ratio is trivially 0 or 1.

**Score:** the ratio of dominant side (e.g., `0.97` = 97% of volume was one-sided).

**Direction:** `pump` if buy-dominated, `dump` if sell-dominated.

---

## Detector 4 — `isolation_forest` (ML)

The first three detectors look at one statistic at a time. The Isolation Forest detector looks at the **multivariate pattern** of multiple features at once, and can catch combinations that no individual rule would flag.

### How Isolation Forest works (conceptually)

Imagine you have a 2D scatter plot of data points. To "isolate" any individual point, you draw random splits (vertical or horizontal lines) until that point is alone in its own region.

- **Normal points** are surrounded by neighbors → take **many splits** to isolate.
- **Anomalies** are far from other points → take **few splits** to isolate.

Isolation Forest builds many random binary trees (an "ensemble") that try to isolate points by random feature/threshold splits. The number of splits needed becomes an anomaly score — fewer splits = more anomalous.

This works in high dimensions too. We use 9 features per bar.

### Our 9 features per bar

| # | Feature | What it captures |
|---|---|---|
| 1 | `log_return` | Direction and magnitude of price move |
| 2 | `return_zscore` | How unusual that move is vs recent |
| 3 | `log_volume` | Trading size (log-scaled to compress range) |
| 4 | `volume_ratio` | Current volume / rolling median |
| 5 | `aggressor_ratio` | Buy share of total volume |
| 6 | `log_trade_count` | How many distinct trades happened |
| 7 | `realized_vol` | Std dev of recent returns (volatility) |
| 8 | `vwap_dev` | `(close − vwap) / vwap` — how far close is from fair price |
| 9 | `price_range` | `(high − low) / close` — intra-bar movement |

### Training

Every 30 minutes we:
1. Pull the last 6 hours of bars from Postgres (~21,600 bars)
2. Compute the 9 features for each
3. Drop rows where the rolling buffers weren't warm yet
4. Fit `IsolationForest(n_estimators=100, contamination=0.01)`

`contamination=0.01` means: "I expect roughly 1% of training data to be anomalies." The model picks an internal threshold so that ~1% of training scores fall below it. The threshold then applies to new bars.

### Scoring at runtime

For each new bar:
1. Compute the 9 features
2. `model.predict([features])` returns `1` (normal) or `-1` (anomaly)
3. If `-1`, flag and write to anomalies table

`decision_function([features])` gives the continuous score (negative = anomalous). We store both.

### Why retrain?

Market regime changes — volatility cycles, time-of-day patterns, news-driven shifts. A model trained at 3 AM might be miscalibrated by 9 AM. Retraining every 30 min on the rolling 6-hour window keeps it current.

### Why both rules AND ML?

They complement each other:
- **Rules** are interpretable (you can explain exactly *why* a bar flagged) and reliable for known patterns
- **ML** can catch combinations the rules miss (e.g., "modest price move + huge buy aggression + low volatility background" might not trip any single rule but is unusual)

When both agree on the same bar, confidence is high. In our current data, the IF is agreeing with rules ~100% of the time on the most extreme bars — which validates that the rules are correctly tuned to catch the most outlying events. The IF can also flag subtler combos the rules don't.

---

## Direction labeling

For each anomaly:
- `pump` — price moved up (positive log return) AND (buy-aggressor heavy OR spike on positive move)
- `dump` — price moved down AND (sell-aggressor heavy OR spike on negative move)
- `unknown` — price didn't move (volume_spike on a flat-price bar with even buy/sell split)

This labeling is heuristic. A bar flagged as `pump` is the system's best guess, not a guarantee of manipulation.

---

## Why these specific thresholds?

| Threshold | Value | Why |
|---|---|---|
| Price z-score | 4 | Above 4σ is rare enough to be interesting, common enough that we get signals |
| Min absolute return | 1e-4 (0.01%) | Filters out z-score noise during ultra-quiet periods |
| Volume spike ratio | 10× | A 10× spike in a single second is decisively unusual |
| Volume floor | 0.05 BTC | Prevents division-by-near-zero blowups |
| Aggressor ratio | 0.95 / 0.05 | One-sided thresholds — 95% of volume from one side |
| Aggressor min volume | 0.5 BTC | Don't flag tiny one-trade bars |
| IF contamination | 0.01 | Calibrates IF to flag ~1% of training bars |
| IF retrain interval | 30 min | Balances freshness vs CPU cost |
| Training window | 6 hours | Enough to capture recent regime, short enough to adapt |

All thresholds are environment variables in the respective services — easy to tune as you watch the dashboard. If you're flagging too many anomalies, raise the thresholds. Too few, lower them.

---

## What the system can't tell you

- **Why** a flagged event happened (a news headline? a whale? coordinated manipulation?)
- **Whether** it predicts future price movement
- **Intent** — large legitimate trades and deliberate manipulation can look identical at this resolution

Anomaly detection is a *flagging* tool, not a prediction or causation tool. It surfaces what's interesting; humans interpret.
