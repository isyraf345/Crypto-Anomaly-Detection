# 02 — Architecture & Tech Stack

How the pieces fit together, what each one does, and why we chose each tool.

---

## High-level data flow

```
┌─────────────────────────┐
│  Binance Exchange       │  Real-time WebSocket feed
│  wss://stream.binance...│  ~1-1000 trades/sec depending on activity
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  producer (Python)      │  Connects, parses JSON, publishes to Kafka
│  Container: producer    │  Handles reconnects with exponential backoff
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  Kafka                  │  Stream buffer / commit log
│  Container: kafka       │  Topic: trades.btcusdt (retention: 4 hours)
│  + Zookeeper            │  Decouples producer from all consumers
└───────────┬─────────────┘
            │ (Kafka consumer group: aggregator-v1)
            ▼
┌─────────────────────────┐
│  aggregator (Python)    │  Bundles trades into 1-second OHLCV bars
│  Container: aggregator  │  Computes buy/sell split, VWAP, trade count
└───────────┬─────────────┘
            │
            ▼
┌──────────────────────────────────────────┐
│  PostgreSQL + TimescaleDB                │
│  Container: postgres                     │
│  ┌─────────────────┐  ┌────────────────┐ │
│  │ ohlcv_1s        │  │ anomalies      │ │
│  │ (hypertable)    │  │                │ │
│  └─────────────────┘  └────────────────┘ │
└─────┬────────────────────────┬───────────┘
      │ polls for new bars     │ inserts anomaly rows
      │                        │
      ▼                        ▼
┌──────────────────┐    ┌──────────────────┐
│ detector         │    │ if_detector      │
│ (rule-based)     │    │ (Isolation Forest│
│                  │    │  + sklearn)      │
│ price z-score    │    │ Retrains every   │
│ volume spike     │    │ 30 min on last   │
│ aggressor split  │    │ 6 hours of bars  │
└──────────────────┘    └──────────────────┘
                  ↑
                  │ reads from anomalies + ohlcv_1s
                  │
        ┌─────────────────────────────────┐
        │  Grafana                        │
        │  Container: grafana             │
        │  http://localhost:3000          │
        │                                 │
        │  - Price + VWAP chart           │
        │  - Buy/sell volume bars         │
        │  - Anomaly markers + table      │
        └─────────────────────────────────┘
```

---

## Component-by-component

### 1. Binance WebSocket (external)
The source of truth. Binance's exchange publishes a JSON message every time a trade executes on BTCUSDT. We connect to `wss://stream.binance.com:9443/ws/btcusdt@trade`. No authentication required for public trade data; no cost.

A trade message looks like:
```json
{
  "e": "trade",
  "E": 1778954696530,    // event time (ms since epoch)
  "s": "BTCUSDT",
  "t": 6300977013,       // trade ID
  "p": "78185.90000000", // price as string
  "q": "0.00079000",     // quantity as string
  "T": 1778954696529,    // trade time (ms)
  "m": false             // is buyer the maker?
}
```

### 2. Producer service (`producer/`)
- **Language:** Python 3.11 (async)
- **Libraries:** `websockets`, `confluent-kafka`, `certifi`
- **Job:** maintain the WebSocket connection, parse each trade, publish to Kafka

The raw JSON message is forwarded verbatim to Kafka (no transformation here). This way the producer stays minimal and any downstream consumer gets the original data.

Reconnect logic: exponential backoff (1s, 2s, 4s, ..., capped at 30s) so we don't hammer Binance after a hiccup.

### 3. Kafka + Zookeeper
- **Image:** `confluentinc/cp-kafka:7.5.0`, `confluentinc/cp-zookeeper:7.5.0`
- **Topic:** `trades.btcusdt`
- **Retention:** 4 hours (raw trades are ephemeral by design — we don't persist them long-term)

**Why Kafka and not just direct producer→DB writes?** See [the Kafka section below](#why-kafka-specifically).

### 4. Aggregator service (`aggregator/`)
- **Language:** Python 3.11 (sync)
- **Libraries:** `confluent-kafka`, `psycopg[binary]`
- **Job:** consume trades, group by 1-second bucket, write completed bars to Postgres

Algorithm:
1. For each trade, compute `bucket = floor(trade_time_ms / 1000)`
2. Update the in-memory bucket object (running open/high/low/close, volume sums, aggressor split)
3. Once `current_time - bucket > 2 sec` (grace period for out-of-order trades), upsert the bucket to Postgres and free it

Grace period exists because trades can arrive slightly out of order. After 2 seconds we consider a bucket "closed."

### 5. PostgreSQL + TimescaleDB
- **Image:** `timescale/timescaledb:latest-pg16`
- **Two tables:** `ohlcv_1s` (hypertable, partitioned by time) and `anomalies` (regular table with JSONB columns)

TimescaleDB is a Postgres extension that adds:
- **Hypertables** — automatically partition large time-series tables into chunks for fast queries
- **Continuous aggregates** — pre-compute downsampled views (we don't use these yet but could)
- **Native time-series functions** — `time_bucket()`, `first()`, `last()`

Why not InfluxDB? Postgres lets us mix relational (anomalies metadata) with time-series, and Grafana's Postgres datasource is rock-solid.

### 6. Rule-based detector (`detector/`)
- **Language:** Python 3.11
- **Libraries:** `psycopg[binary]` only (no ML deps)
- **Job:** poll Postgres for new bars, compute rolling features, apply threshold rules, write anomalies

See [docs/03-anomaly-detection.md](03-anomaly-detection.md) for the math and thresholds.

### 7. ML detector (`if_detector/`)
- **Language:** Python 3.11
- **Libraries:** `psycopg[binary]`, `scikit-learn`, `numpy`
- **Job:** train an Isolation Forest on the last 6 hours of bars, score new bars, write anomalies; retrain every 30 min

See [docs/03-anomaly-detection.md](03-anomaly-detection.md) for how Isolation Forest works.

### 8. Grafana
- **Image:** `grafana/grafana:latest`
- **Provisioning:** datasource and dashboard JSON are baked in via `grafana/provisioning/`
- **URL:** http://localhost:3000 (admin / admin)

See [docs/04-dashboard-guide.md](04-dashboard-guide.md) for how to read the panels.

---

## Why Kafka specifically

Kafka is a **distributed commit log** — the simplest way to think of it is: an append-only file that many producers can write to and many consumers can read from, at their own pace.

In this project, Kafka sits between the producer and everything else. We could in principle skip it and have the producer write directly to Postgres. But Kafka gives us four concrete benefits:

| Benefit | Why it matters here |
|---|---|
| **Decoupling** | If Postgres restarts, the producer keeps streaming. Aggregator just resumes from where it left off when DB is back. |
| **Replay** | Want to add a third consumer (e.g. a Slack alerter)? Point it at the topic, set `auto.offset.reset=earliest`, and it reads the last 4 hours without changes to anything upstream. |
| **Backpressure absorption** | Bursts of 1000 trades/sec don't overwhelm the slow consumer (DB inserts). Kafka holds the surge. |
| **Multiple consumers, independent state** | Aggregator, rule detector, ML detector could all consume the same topic if we restructured — each tracking its own progress with its own consumer group. |

### Kafka concepts as used here

| Concept | What it is | Our value |
|---|---|---|
| **Broker** | A Kafka server | 1 broker (single-node, fine for dev) |
| **Topic** | Named stream of messages | `trades.btcusdt` |
| **Partition** | Sub-stream for parallelism | 1 (we don't need parallelism yet) |
| **Producer** | Publishes messages | Our `producer/` service |
| **Consumer** | Reads messages | Our `aggregator/` service |
| **Consumer group** | Group of consumers sharing work on a topic | `aggregator-v1` |
| **Offset** | Position in the topic | Kafka tracks per consumer group |
| **Retention** | How long messages live | 4 hours |

Zookeeper is a coordinator Kafka uses for cluster metadata. Newer Kafka can run without it (KRaft mode), but Confluent's 7.5 image still uses Zookeeper by default, so we keep it.

---

## Why local-first (no AWS)

The original brief was to deploy the database to AWS RDS (free tier, 20GB). We decided to defer that:

- The streaming + ML core is the interesting part. Cloud is just packaging.
- AWS free tier doesn't support managed Kafka (MSK isn't free), so the cloud version would still need Kafka somewhere.
- Local-first means one-command bring-up, full reproducibility, zero spend.

The `anomalies` table is small (anomaly events + features blob, megabytes per day), so it's a clean "push to cloud later" candidate when desired.

---

## Storage cost calculation

The brief mentioned 7-10 GB/day if we stored raw trades. Our storage strategy collapses this:

| Tier | What's stored | Daily size estimate |
|---|---|---|
| Hot — Kafka | Raw trades | Ephemeral (4 hr) |
| Warm — Postgres `ohlcv_1s` | 1-sec bars (86,400/day, ~200 bytes each) | ~17 MB/day |
| Cold — Postgres `anomalies` | Anomaly events + features JSON + 60-bar context | <1 MB/day at current rates |

Total: **~20 MB/day in persistent storage**, vs 7-10 GB/day raw. A 20 GB free-tier disk would last ~3 years.

---

## Docker Compose layout

All services run as containers on a single Docker bridge network. Service-to-service communication uses the container name as hostname:

```
zookeeper  ─► kafka:9092 (internal)
kafka      ─► localhost:29092 (host-exposed for debugging)
postgres   ─► localhost:5433 (host-exposed; 5432 conflicted with native Win Postgres)
grafana    ─► localhost:3000 (host-exposed; the only HTTP-browseable service)
```

Inside the Docker network: `kafka:9092`, `postgres:5432`. From your Windows host: ports above.

See [docs/05-operations.md](05-operations.md) for the actual commands to start, stop, and inspect things.
