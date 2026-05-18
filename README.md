# Real-Time Crypto Anomaly Detection

A streaming data pipeline that watches live Bitcoin trades on Binance, detects unusual market activity (pumps, dumps, volume spikes) with both rule-based and machine-learning detectors, and visualizes everything in a live Grafana dashboard.

Built end-to-end with Kafka, TimescaleDB, scikit-learn, and Docker. Runs entirely on your laptop — no cloud required.

---

## What it actually does

```
Binance Exchange  ──►  We connect to their live trade feed (WebSocket)
       │
       ▼               Every trade that happens on BTCUSDT (~1-40 per second)
                       gets pushed to us in real time.
       │
       ▼
   Kafka              Acts as a buffer / log between the data source and
                      everything that consumes it. Decouples the pipeline.
       │
       ├──►  Aggregator    Bundles raw trades into 1-second OHLCV bars
       │                   (open, high, low, close, volume + buy/sell split)
       │                   and saves them to Postgres.
       │
       ├──►  Rule Detector   Watches each new bar and flags it if:
       │                     - price moved unusually (z-score > 4)
       │                     - volume spiked (>10× rolling median)
       │                     - one-sided aggression (>95% buys or sells)
       │
       └──►  ML Detector     Isolation Forest trained on recent history.
                             Flags bars whose multivariate feature signature
                             looks unlike "normal" market behavior.
       │
       ▼
   Postgres + TimescaleDB    Stores bars and anomalies for the dashboard.
       │
       ▼
   Grafana                   Live dashboard at http://localhost:3000
                             - Price chart with anomaly markers
                             - Buy vs sell volume bars
                             - Recent anomalies table
```

---

## Where to learn more

If you have **zero background** in any of this, read the docs in this order:

| # | Doc | What it covers |
|---|---|---|
| 1 | [docs/01-crypto-primer.md](docs/01-crypto-primer.md) | Crypto and market terminology from scratch — what's a trade, what's OHLCV, what's a pump/dump |
| 2 | [docs/02-architecture.md](docs/02-architecture.md) | The full system, what each component does, what Kafka is and why we use it |
| 3 | [docs/03-anomaly-detection.md](docs/03-anomaly-detection.md) | The math behind every detector — z-scores, rolling windows, Isolation Forest |
| 4 | [docs/04-dashboard-guide.md](docs/04-dashboard-guide.md) | How to read each chart, what to look for, examples of real anomalies |
| 5 | [docs/05-operations.md](docs/05-operations.md) | Day-to-day commands: start/stop, watch logs, query the DB, troubleshoot |
| 6 | [db/README.md](db/README.md) | Database schema reference — tables, columns, JSONB structures, common queries |

---

## Tech stack at a glance

| Layer | Tool | Version | Why |
|---|---|---|---|
| Data source | Binance WebSocket API | — | Free, public, real-time |
| Stream broker | Apache Kafka | 7.5 (Confluent) | Decouples producer from consumers, replayable |
| Storage | PostgreSQL + TimescaleDB | PG 16 | Time-series superpowers on familiar SQL |
| Dashboards | Grafana | latest | Best-in-class time-series UI, free |
| Services | Python | 3.11 | Standard for data + ML work |
| Kafka client | `confluent-kafka` | 2.x | Fast (librdkafka-based) |
| DB client | `psycopg` v3 | 3.2+ | Modern Postgres driver |
| WS client | `websockets` | 12+ | Async-native |
| ML | `scikit-learn` | 1.5+ | Isolation Forest |
| Orchestration | Docker Compose | — | One-command bring-up |

---

## Quick start

You need **Docker Desktop** running. That's the only prerequisite.

```bash
# from the project root
docker compose up -d
```

Wait ~30 seconds for everything to settle, then open:

- **Grafana dashboard:** http://localhost:3000 (login: `admin` / `admin`)
- **Postgres:** `localhost:5433`, user `crypto`, password `cryptopass`, db `crypto`
- **Kafka:** `localhost:29092` (from host) or `kafka:9092` (from inside Docker)

To stop everything:

```bash
docker compose down
```

To stop AND delete all stored data:

```bash
docker compose down -v
```

---

## Project layout

```
.
├── docker-compose.yml         # All 7 services wired together
├── .env                       # Default credentials (gitignored)
├── db/
│   └── init.sql               # Postgres schema (ohlcv_1s, anomalies tables)
├── producer/                  # Binance WS → Kafka
├── aggregator/                # Kafka → 1-sec OHLCV bars in Postgres
├── detector/                  # Rule-based anomaly detector
├── if_detector/               # Isolation Forest anomaly detector
├── grafana/
│   ├── provisioning/          # Auto-configured datasource + dashboard provider
│   └── dashboards/            # Dashboard JSON
└── docs/                      # The explainer docs — start with 01
```

---

## Status

All five planned phases are complete. The system is running live data right now if you have `docker compose up` going.

| Phase | Component | Done |
|---|---|---|
| 1 | Infrastructure (Kafka, Postgres, Grafana) | yes |
| 2 | Binance WS producer | yes |
| 3 | 1-second OHLCV aggregator | yes |
| 4 | Rule-based anomaly detector | yes |
| 5 | Grafana dashboard | yes |
| 6 | Isolation Forest detector | yes |
| 7 | Multi-symbol (ETH alongside BTC) | future |
| 8 | AWS RDS deployment | deferred |
