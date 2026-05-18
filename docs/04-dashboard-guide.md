# 04 — Reading the Grafana Dashboard

How to look at the dashboard and actually understand what you're seeing.

---

## Getting in

1. Make sure the stack is running: `docker compose ps` should show all services healthy.
2. Open http://localhost:3000
3. Login: `admin` / `admin` (skip the "change password" prompt or change it)
4. Navigate: left sidebar → **Dashboards** → **Crypto Anomaly Detection — BTCUSDT**

Direct link: http://localhost:3000/d/crypto-anomaly

The dashboard auto-refreshes every 5 seconds.

---

## The three panels

### Panel 1: BTCUSDT — Price (close & VWAP)

A line chart at the top.

- **Two lines:**
  - `close` — the closing price of each 1-second bar
  - `vwap` — volume-weighted average price of each bar (see [crypto primer](01-crypto-primer.md))
- **Y-axis:** price in USDT
- **X-axis:** time

#### What to look for

- **Both lines tracking together** = normal market. Most of the time they're nearly indistinguishable.
- **VWAP and close diverging** = a small number of trades pulled the close away from where most volume traded. Often a hint of low-liquidity moments or last-second spikes.
- **Sharp vertical moves** = fast price action. Combine with the volume panel below to see if it was high-volume (real) or low-volume (thin).

#### Annotations

You'll see vertical colored lines on this chart — these are **anomaly markers** overlaid from the `anomalies` table:

- **Green vertical line** = a `pump` was flagged at that moment
- **Red vertical line** = a `dump` was flagged

Hover over an annotation to see the detector name and score. Multiple detectors at the same instant = multiple stacked annotations (e.g., a real pump often shows green from `price_zscore`, `volume_spike`, AND `aggressor_imbalance` simultaneously).

---

### Panel 2: Volume — buy vs sell (BTC)

A stacked bar chart in the middle.

- **Green bars** = `buy_volume` (trades where the aggressor was the buyer — market buys)
- **Red bars** = `sell_volume` (trades where the aggressor was the seller — market sells)
- **Bar height** = total volume in BTC for that 1-second window
- **Stacking** = total bar height shows total volume; the green/red proportion shows directional pressure

#### What to look for

| Pattern | What it means |
|---|---|
| Roughly equal green and red, small bars | Normal balanced trading |
| Tall bars with mostly green | Buying pressure — possibly the start of a pump |
| Tall bars with mostly red | Selling pressure — possibly the start of a dump |
| One single huge bar (pure green or red) | Whale market order or coordinated burst |
| Sustained imbalance over many bars | Directional move in progress |

This panel is often the **most useful for understanding why an anomaly fired**. If you see a red marker on the price chart, look down at the volume panel at that timestamp — you'll usually see a tall red bar.

---

### Panel 3: Recent anomalies (table)

The table at the bottom lists the most recent flagged anomalies.

Columns:

| Column | Meaning |
|---|---|
| `time` | When the anomaly was detected |
| `detector` | Which detector flagged it: `price_zscore`, `volume_spike`, `aggressor_imbalance`, or `isolation_forest` |
| `direction` | `pump` (green cell), `dump` (red cell), or `unknown` (blue cell) |
| `score` | Strength — interpretation depends on detector (see below) |
| `close` | Price at that moment |
| `vol_spike` | Volume as a multiple of rolling median |
| `agg_ratio` | Aggressor ratio (1.0 = all buys, 0.0 = all sells, 0.5 = balanced) |
| `return_z` | Z-score of the price return |

#### Interpreting `score` per detector

| Detector | Score meaning | Typical values |
|---|---|---|
| `price_zscore` | $|z|$ — std devs from rolling mean | 4 (threshold) – 50+ |
| `volume_spike` | Multiple of rolling median | 10 (threshold) – several hundred |
| `aggressor_imbalance` | Ratio of dominant side | 0.95 – 1.000 |
| `isolation_forest` | $|$decision_function$|$ — distance below IF's threshold | 0.01 – 0.10 |

(IF scores are not directly comparable to rule-based scores — they're on a different scale.)

---

## Patterns to recognize

### Healthy market

- Price line gently meandering
- Both VWAP and close on top of each other
- Volume bars small and roughly green/red balanced
- Anomalies table has only a few entries, low scores

### A real pump

- Sharp vertical move up on the price chart
- Tall green bars on the volume panel at the same moment
- Multiple detectors firing at the same instant (clusters of green annotation lines)
- High scores in the table: `vol_spike` in the hundreds, `agg_ratio` near 1.0, `return_z` > 4

Example from a real cluster we caught: at one second, three rule detectors fired simultaneously with `vol_spike=164`, `agg_ratio=1.000`, `return_z=+7.68`, and price moved $50 in 50 seconds. All three independent signals agreeing = high confidence.

### A real dump

Mirror image of a pump:
- Sharp move down on the price chart
- Tall red bars on the volume panel
- Multiple red annotation lines clustered
- `agg_ratio` near 0.0, `return_z` < -4

### "Unknown" direction

- Volume spiked but price didn't move (sometimes happens when buy and sell aggression balance out perfectly)
- Or a single huge trade where direction couldn't be inferred

These are usually less actionable than pump/dump but still represent unusual activity.

### Cold-start noise (first 10 minutes)

When you first start the stack, the detectors are warming up their rolling buffers. You may see spuriously high `vol_spike` scores until the median stabilizes. This is normal and clears up within a few minutes once buffers are full.

The IF detector requires ~30 minutes of data before it trains at all — so during cold start you'll only see rule-based flags. After training kicks in, `isolation_forest` rows appear.

---

## Changing time range

Default is `now-30m` to `now` with 5-second auto-refresh.

To investigate a past event:
- Click the time picker (top right)
- Pick "Last 6 hours" or set a custom range
- Disable auto-refresh if you want to study a static moment

The annotations and table will automatically filter to the selected time range.

---

## Cross-referencing with raw data

To dig into a specific anomaly:

```bash
docker exec -it postgres psql -U crypto -d crypto
```

```sql
-- Get full details on a recent anomaly including its features and raw context
SELECT * FROM anomalies WHERE detected_at > NOW() - INTERVAL '5 minutes' ORDER BY detected_at DESC LIMIT 1;

-- Get the surrounding 60 bars of OHLCV around the anomaly
SELECT * FROM ohlcv_1s WHERE bucket BETWEEN '2026-05-17 04:13:00' AND '2026-05-17 04:14:00' ORDER BY bucket;
```

The `raw_context` JSONB column on each anomaly contains the 60 bars leading up to the event — useful for after-the-fact investigation without re-querying.

---

## What good vs bad detector tuning looks like

- **Too many flags** (>5% of bars): thresholds too sensitive. The dashboard becomes noise. Raise z-score threshold, raise spike multiplier, raise IF contamination.
- **Too few flags** (no flags in hours of moderate volatility): thresholds too strict. Lower the same parameters.
- **Healthy regime**: ~1-3% of bars flagged. Major moves consistently flagged by multiple detectors. Quiet periods have no flags.

Watch the dashboard during different market hours (Asia session, EU session, US session, weekend) — activity varies a lot and you'll get a feel for what's normal.
