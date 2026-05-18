# Database Schema Reference

PostgreSQL 16 with the **TimescaleDB** extension. Two tables: one for time-series OHLCV bars, one for anomaly events.

The schema is defined in [`init.sql`](init.sql) and runs automatically the **first time** the Postgres container starts (via Docker's `/docker-entrypoint-initdb.d/` mechanism). It does **not** rerun on container restart — only on a fresh volume.

---

## Connecting

| From | Host | Port | DB | User | Password |
|---|---|---|---|---|---|
| Your Windows machine (DBeaver, psql.exe) | `localhost` | **5433** | `crypto` | `crypto` | `cryptopass` |
| Other Docker containers (Grafana, services) | `postgres` | `5432` | `crypto` | `crypto` | `cryptopass` |

Inline SQL shell:
```bash
docker exec -it postgres psql -U crypto -d crypto
```

---

## What TimescaleDB adds

TimescaleDB is a Postgres extension that turns a regular table into a **hypertable** — a table that's automatically partitioned into time-based "chunks" under the hood. Benefits:

- **Faster time-range queries** — Postgres only scans the chunks that overlap the time filter
- **Efficient inserts at high write rates** — chunks rotate, indexes stay small per chunk
- **Time-series functions** — `time_bucket()`, `first()`, `last()`, `locf()` (last observation carried forward)

We use it on the `ohlcv_1s` table. The `anomalies` table stays a regular Postgres table because it's small (kilobytes per day).

---

## Table 1 — `ohlcv_1s`

One row per **1-second time window** per symbol. Holds aggregated trade statistics for that second.

### Schema

```sql
CREATE TABLE ohlcv_1s (
    symbol       TEXT             NOT NULL,
    bucket       TIMESTAMPTZ      NOT NULL,
    open         DOUBLE PRECISION NOT NULL,
    high         DOUBLE PRECISION NOT NULL,
    low          DOUBLE PRECISION NOT NULL,
    close        DOUBLE PRECISION NOT NULL,
    volume       DOUBLE PRECISION NOT NULL,
    quote_volume DOUBLE PRECISION NOT NULL,
    buy_volume   DOUBLE PRECISION NOT NULL,
    sell_volume  DOUBLE PRECISION NOT NULL,
    vwap         DOUBLE PRECISION NOT NULL,
    trade_count  INTEGER          NOT NULL,
    PRIMARY KEY (symbol, bucket)
);

SELECT create_hypertable('ohlcv_1s', 'bucket', if_not_exists => TRUE);

CREATE INDEX ohlcv_1s_symbol_bucket_idx ON ohlcv_1s (symbol, bucket DESC);
```

### Column-by-column

| Column | Type | What it means |
|---|---|---|
| `symbol` | TEXT | Trading pair, always uppercase. e.g. `BTCUSDT` |
| `bucket` | TIMESTAMPTZ | Start of the 1-second window, UTC. e.g. `2026-05-17 04:13:14+00` |
| `open` | DOUBLE | Price of the **first** trade in the window |
| `high` | DOUBLE | **Highest** trade price in the window |
| `low` | DOUBLE | **Lowest** trade price in the window |
| `close` | DOUBLE | Price of the **last** trade in the window |
| `volume` | DOUBLE | Total BTC traded (sum of all trade quantities) |
| `quote_volume` | DOUBLE | Total USDT traded (sum of price × quantity) |
| `buy_volume` | DOUBLE | BTC where the **aggressor was the buyer** (market buys) |
| `sell_volume` | DOUBLE | BTC where the **aggressor was the seller** (market sells) |
| `vwap` | DOUBLE | Volume-weighted average price = `quote_volume / volume` |
| `trade_count` | INTEGER | Number of distinct trades in the window |

By construction `buy_volume + sell_volume == volume` (every trade has exactly one aggressor side).

### Primary key

`(symbol, bucket)` — natural unique key. Allows multiple symbols in the future without table changes, and prevents duplicate bars.

### Index

`(symbol, bucket DESC)` — supports "latest N bars for a symbol" queries (most common pattern):
```sql
SELECT * FROM ohlcv_1s WHERE symbol='BTCUSDT' ORDER BY bucket DESC LIMIT 100;
```

### Upserts

The aggregator inserts via `ON CONFLICT (symbol, bucket) DO UPDATE`. So if it re-processes the same trades (e.g., after a restart with backlog), the bar gets overwritten with the recomputed values rather than duplicated.

### Typical size

- ~86,400 bars per symbol per day (1 per second)
- ~200 bytes per row (12 columns mostly numeric)
- **~17 MB/day per symbol**

---

## Table 2 — `anomalies`

One row per detected anomaly event. Multiple detectors can fire on the same bar — that produces multiple rows, one per detector.

### Schema

```sql
CREATE TABLE anomalies (
    id           BIGSERIAL PRIMARY KEY,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol       TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    detector     TEXT        NOT NULL,
    direction    TEXT        NOT NULL DEFAULT 'unknown',
    score        DOUBLE PRECISION,
    features     JSONB       NOT NULL,
    raw_context  JSONB
);

CREATE INDEX anomalies_symbol_time_idx ON anomalies (symbol, detected_at DESC);
```

### Column-by-column

| Column | Type | What it means |
|---|---|---|
| `id` | BIGSERIAL | Auto-incrementing primary key |
| `detected_at` | TIMESTAMPTZ | Wall-clock time when the detector flagged this. Defaults to `NOW()` |
| `symbol` | TEXT | Trading pair, e.g. `BTCUSDT` |
| `window_start` | TIMESTAMPTZ | Start of the analysis window (currently `bucket - 60s`) |
| `window_end` | TIMESTAMPTZ | End of the analysis window — equals the `bucket` of the flagged bar |
| `detector` | TEXT | Which detector fired: `price_zscore`, `volume_spike`, `aggressor_imbalance`, `isolation_forest` |
| `direction` | TEXT | `pump`, `dump`, or `unknown` |
| `score` | DOUBLE | Strength of the signal. **Scale depends on detector** (see below) |
| `features` | JSONB | Feature values at detection time (variable keys — depends on detector) |
| `raw_context` | JSONB | Array of the 60 OHLCV bars leading up to the event |

### `score` interpretation by detector

| Detector | What `score` represents | Typical range |
|---|---|---|
| `price_zscore` | $\|z\|$ — absolute z-score of the return | 4 (threshold) to 50+ |
| `volume_spike` | Volume / rolling median multiplier | 10 (threshold) to several hundred |
| `aggressor_imbalance` | Dominant-side ratio (1.0 = pure one-sided) | 0.95 to 1.000 |
| `isolation_forest` | $\|\text{decision\_function}\|$ — how far below the IF's threshold | 0.01 to 0.10 |

Scores are **not comparable across detectors**. Each is meaningful only within its own scale.

### `features` JSONB schema

The exact keys depend on the detector. Common shape:

```json
{
  "close": 78212.49,
  "log_return": 0.00076,
  "return_zscore": 7.68,
  "volume": 12.3,
  "volume_median_5m": 0.075,
  "volume_spike_ratio": 164.0,
  "aggressor_ratio": 1.000,
  "trade_count": 87
}
```

For `isolation_forest` it also includes:
```json
{
  "if_score": -0.0153,
  "log_return": -0.0008,
  "return_zscore": -8.26,
  "log_volume": 2.51,
  "volume_ratio": 278.36,
  "aggressor_ratio": 0.000,
  "log_trade_count": 3.99,
  "realized_vol": 0.00012,
  "vwap_dev": -0.00004,
  "price_range": 0.00021
}
```

Stored as JSONB so we can evolve the feature set without schema migrations. Query individual fields with `->>`:

```sql
SELECT (features->>'volume_spike_ratio')::numeric AS vol_spike
FROM anomalies WHERE detector='volume_spike' LIMIT 10;
```

### `raw_context` JSONB schema

An array of up to 60 OHLCV bars immediately preceding (and including) the flagged event. Each element:

```json
{
  "bucket": "2026-05-17T04:13:14+00:00",
  "open": 78320.10,
  "high": 78326.21,
  "low": 78319.81,
  "close": 78326.21,
  "volume": 4.83,
  "quote_volume": 378234.5,
  "buy_volume": 4.81,
  "sell_volume": 0.02,
  "vwap": 78324.2,
  "trade_count": 36
}
```

Useful for after-the-fact investigation without re-querying `ohlcv_1s`:

```sql
SELECT jsonb_array_length(raw_context) FROM anomalies WHERE id = 1;
-- 60

SELECT raw_context->0->>'close' AS oldest_close
FROM anomalies WHERE id = 1;
```

### Index

`(symbol, detected_at DESC)` — supports the most common query pattern: "show me recent anomalies for BTCUSDT."

---

## Common queries

### Counts by detector and direction
```sql
SELECT detector, direction, COUNT(*)
FROM anomalies
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Latest 10 anomalies with key features
```sql
SELECT detected_at,
       detector,
       direction,
       ROUND(score::numeric, 2)                                   AS score,
       (features->>'close')::numeric                              AS close,
       ROUND((features->>'volume_spike_ratio')::numeric, 2)       AS vol_spike,
       ROUND((features->>'aggressor_ratio')::numeric, 3)          AS agg,
       ROUND((features->>'return_zscore')::numeric, 2)            AS ret_z
FROM anomalies
ORDER BY detected_at DESC
LIMIT 10;
```

### Co-firing detectors (anomalies where multiple fired on the same bucket)
```sql
SELECT window_end, COUNT(*) AS detectors_fired,
       string_agg(DISTINCT detector, ', ' ORDER BY detector) AS which
FROM anomalies
WHERE symbol = 'BTCUSDT'
GROUP BY window_end
HAVING COUNT(*) >= 3
ORDER BY window_end DESC
LIMIT 20;
```

### Largest pumps by price-z score
```sql
SELECT detected_at,
       (features->>'close')::numeric AS close,
       ROUND((features->>'return_zscore')::numeric, 2) AS z
FROM anomalies
WHERE detector='price_zscore' AND direction='pump'
ORDER BY score DESC
LIMIT 10;
```

### Bars in the last hour with their hourly average
```sql
SELECT time_bucket('1 minute', bucket) AS minute,
       AVG(close) AS avg_close,
       SUM(volume) AS minute_volume
FROM ohlcv_1s
WHERE symbol='BTCUSDT' AND bucket > NOW() - INTERVAL '1 hour'
GROUP BY minute
ORDER BY minute;
```

(That's `time_bucket()` — a TimescaleDB function that snaps timestamps to fixed intervals. The native way to downsample 1-second bars into 1-minute bars without manual interval math.)

### Bars around an anomaly
```sql
WITH a AS (
    SELECT window_end FROM anomalies WHERE id = 1
)
SELECT *
FROM ohlcv_1s
WHERE symbol='BTCUSDT'
  AND bucket BETWEEN (SELECT window_end - INTERVAL '60 seconds' FROM a)
                 AND (SELECT window_end + INTERVAL '60 seconds' FROM a)
ORDER BY bucket;
```

---

## Maintenance

### Wipe anomalies (e.g. after tuning thresholds)
```sql
TRUNCATE anomalies RESTART IDENTITY;
```

### Wipe everything but keep the schema
```sql
TRUNCATE ohlcv_1s, anomalies RESTART IDENTITY;
```

### Nuke the entire database (volume reset)
From the project root:
```bash
docker compose down
docker volume rm cryptoanomalydetection_postgres_data
docker compose up -d
```
`init.sql` re-runs on the next start.

### Disk usage
```sql
SELECT pg_size_pretty(pg_total_relation_size('ohlcv_1s'))   AS ohlcv_size,
       pg_size_pretty(pg_total_relation_size('anomalies'))  AS anomalies_size,
       pg_size_pretty(pg_database_size('crypto'))           AS db_size;
```

### Hypertable chunk inspection (TimescaleDB-specific)
```sql
SELECT chunk_name, range_start, range_end,
       pg_size_pretty(pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name)::regclass)) AS size
FROM timescaledb_information.chunks
WHERE hypertable_name = 'ohlcv_1s'
ORDER BY range_start;
```

---

## Schema evolution notes

If you change `init.sql`, it does **not** auto-apply to an existing database — Postgres only runs the init scripts on a fresh volume. To apply changes:

1. **Option A** (preserves data) — connect with `psql` and run the new statements manually
2. **Option B** (wipes data) — `docker compose down -v && docker compose up -d`

For ongoing migrations on a production-style setup you'd want a proper migration tool (Alembic, Flyway, etc.). For this project, Option B is fine since the data is reproducible from Binance.
