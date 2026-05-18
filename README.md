# Real-Time Crypto Anomaly Detection

A streaming data pipeline that watches **live Bitcoin trades on Binance**, detects unusual market activity (pumps, dumps, volume spikes) with both **rule-based and machine-learning** detectors, and visualizes everything in a live **Grafana** dashboard.

Built end-to-end with **Apache Kafka, TimescaleDB, scikit-learn, and Docker**. Runs entirely on your laptop — no cloud account required.

> **Project context:** built as a portfolio / learning project to practice end-to-end streaming data engineering and applied ML.

---

## Highlights

- **Real-time only** — connects to Binance's public WebSocket. Not CSV replay.
- **End-to-end streaming** — Binance → Kafka → Postgres → Grafana, fully containerized.
- **Four anomaly detectors** running in parallel: three interpretable rules + one Isolation Forest. They cross-validate each other.
- **Storage-efficient** — raw trades (7-10 GB/day) stay ephemeral in Kafka; only 1-second aggregated bars and anomaly events persist (~20 MB/day).
- **One-command bring-up** — `docker compose up -d` and everything works.
- **Six documentation files** covering crypto terminology, architecture, the math behind every detector, and how to read the dashboard.

---

## What it does

```
Binance Exchange  ──►  Live WebSocket feed of every BTCUSDT trade
       │
       ▼
   Kafka              Decouples producer from all consumers
       │
       ├──►  Aggregator     Bundles raw trades into 1-second OHLCV bars
       │                    (open/high/low/close/volume + buy/sell aggressor split)
       │
       ├──►  Rule Detector  Flags bars where:
       │                    - price moved unusually (|z-score| > 4)
       │                    - volume spiked (>10× rolling median)
       │                    - one-sided aggression (>95% buys or sells)
       │
       └──►  ML Detector    Isolation Forest, retrained every 30 min on the
                            rolling last 6 hours. Catches multivariate patterns
                            the rules can't.
       │
       ▼
   Postgres + TimescaleDB   Hypertable for bars, regular table for anomalies
       │
       ▼
   Grafana                  Live dashboard:
                            - Price + VWAP chart with anomaly markers
                            - Stacked buy vs sell volume bars
                            - Recent anomalies table
```

---

## Example: a real anomaly cluster the system caught

During development, the detector cluster fired on a real BTCUSDT move:

| Time | Detectors firing | Close | Volume vs median | Aggressor ratio | Return z-score |
|---|---|---|---|---|---|
| 18:42:51 | price_zscore + volume_spike + aggressor_imbalance | $78,212 | 164× | 1.000 (all buys) | +7.68 |
| 18:43:35 | price_zscore + volume_spike + aggressor_imbalance | $78,232 | 132× | 1.000 | +6.24 |
| 18:43:37 | volume_spike + aggressor_imbalance | $78,245 | 81× | 0.998 | +3.52 |
| 18:43:38 | volume_spike + aggressor_imbalance | $78,262 | 18× | 1.000 | +3.89 |

All three independent rule detectors agreeing on the same buckets, all labeled `pump`, price climbed $78,212 → $78,262 in ~50 seconds with sustained 100%-buy aggression. The Isolation Forest independently agreed on the same buckets.

---

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Data source | Binance WebSocket API | Free, public, real-time |
| Stream broker | Apache Kafka 7.5 (Confluent) | Decouples pipeline, replayable |
| Storage | PostgreSQL 16 + TimescaleDB | Hypertables on familiar SQL |
| Dashboards | Grafana (latest) | Best-in-class time-series UI |
| Services | Python 3.11 | Standard for data + ML |
| Kafka client | `confluent-kafka` | Fast (librdkafka-based) |
| DB driver | `psycopg` v3 | Modern Postgres driver |
| WebSocket client | `websockets` | Async-native |
| ML library | `scikit-learn` | Isolation Forest |
| Orchestration | Docker Compose | One-command bring-up |

---

## Quick start

### Prerequisites
- **Docker Desktop** (Mac / Windows / Linux) running

### Clone and run

```bash
git clone https://github.com/<your-username>/<your-repo-name>.git
cd <your-repo-name>

# Copy environment template (defaults are fine for local dev)
cp .env.example .env

# Bring up everything
docker compose up -d
```

First boot pulls images and takes a few minutes. Once settled, the producer connects to Binance and data starts flowing within seconds.

### Open the dashboard
- **Grafana:** http://localhost:3000 (login: `admin` / `admin`)
- Navigate: left sidebar → **Dashboards** → **Crypto Anomaly Detection — BTCUSDT**

### Inspect raw data
- **Postgres:** `localhost:5433` (user `crypto` / pass `cryptopass` / db `crypto`)
- **Kafka:** `localhost:29092` (host) or `kafka:9092` (in-network)

### Stop / clean up

```bash
docker compose down       # stop, keep data
docker compose down -v    # stop and wipe data volumes (fresh start)
```

> The Isolation Forest detector needs ~20 minutes of data before it begins training. Rule-based detectors start firing almost immediately. Expect quiet for the first few minutes while everything warms up.

---



---

## Project layout

```
.
├── docker-compose.yml          # All services wired together
├── .env.example                # Environment template (copy to .env)
├── .gitignore
│
├── db/
│   ├── README.md               # Database schema reference
│   └── init.sql                # Postgres schema (auto-runs on first start)
│
├── producer/                   # Binance WebSocket → Kafka
├── aggregator/                 # Kafka → 1-second OHLCV bars in Postgres
├── detector/                   # Rule-based anomaly detector
├── if_detector/                # Isolation Forest anomaly detector
│
├── grafana/
│   ├── provisioning/           # Auto-configured datasource + dashboard provider
│   └── dashboards/             # Dashboard JSON
│
└── docs/                       # Explainer documentation (start with 01)
```
