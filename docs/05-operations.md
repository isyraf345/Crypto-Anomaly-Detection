# 05 — Operations & Troubleshooting

Day-to-day commands for running the system, inspecting state, and fixing common issues.

---

## Starting and stopping

All commands run from the project root directory.

### Start everything
```bash
docker compose up -d
```
The `-d` runs in detached mode (background). First run takes a few minutes to pull images.

### Stop everything (keeps data)
```bash
docker compose down
```

### Stop and wipe all data (start fresh)
```bash
docker compose down -v
```
The `-v` removes Docker volumes — Postgres data and Grafana state both get nuked. Use when you want a clean slate.

### Restart a single service
```bash
docker compose restart detector
```
Useful after editing detection thresholds in environment variables.

### Rebuild a service after code changes
```bash
docker compose up -d --build aggregator
```

---

## Checking status

### Are all containers up and healthy?
```bash
docker compose ps
```
You want to see `Up X (healthy)` for `zookeeper`, `kafka`, `postgres`. Other services don't have healthchecks but should show `Up`.

### Watch logs of one service
```bash
docker compose logs -f producer
```
`-f` follows new log lines. Ctrl+C to stop watching.

### Logs from all services
```bash
docker compose logs -f
```

### Just the last 50 lines
```bash
docker logs --tail 50 detector
```

---

## Inspecting Kafka

### List topics
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```
You should see `trades.btcusdt`.

### Watch live trade messages flowing
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic trades.btcusdt
```
Ctrl+C to stop.

### Count messages in a topic
```bash
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic trades.btcusdt
```

### Check consumer group lag
```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group aggregator-v1
```
`LAG` should be near 0. If it's growing, the aggregator can't keep up.

---

## Inspecting Postgres

### Open a SQL shell
```bash
docker exec -it postgres psql -U crypto -d crypto
```
Once inside, `\q` to quit.

### Useful queries

```sql
-- How many bars do we have?
SELECT COUNT(*) FROM ohlcv_1s;

-- Latest 10 bars
SELECT bucket, close, volume, buy_volume, sell_volume, trade_count
FROM ohlcv_1s ORDER BY bucket DESC LIMIT 10;

-- How many anomalies, by detector and direction?
SELECT detector, direction, COUNT(*)
FROM anomalies GROUP BY 1,2 ORDER BY 3 DESC;

-- Most recent anomalies with their key features
SELECT detected_at, detector, direction, ROUND(score::numeric,2) AS score,
       (features->>'close')::numeric AS close,
       ROUND((features->>'volume_spike_ratio')::numeric,2) AS vol_spike,
       ROUND((features->>'aggressor_ratio')::numeric,3) AS agg,
       ROUND((features->>'return_zscore')::numeric,2) AS ret_z
FROM anomalies ORDER BY detected_at DESC LIMIT 20;

-- Full detail for one anomaly including features and raw context
SELECT * FROM anomalies WHERE id = 1;
```

### Wipe just the anomalies table (e.g. after tuning thresholds)
```sql
TRUNCATE anomalies RESTART IDENTITY;
```

### Wipe everything but keep the schema
```sql
TRUNCATE ohlcv_1s, anomalies RESTART IDENTITY;
```

---

## Connecting from a desktop SQL client (DBeaver, pgAdmin)

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | **`5433`** (not 5432 — see note below) |
| Database | `crypto` |
| User | `crypto` |
| Password | `cryptopass` |

The Docker Postgres is on host port `5433` because the user's Windows machine has a native Postgres on `5432` causing a conflict. Inside the Docker network it's still `postgres:5432`.

---

## Tuning detector thresholds

All thresholds live as constants in the detector code AND as environment variables you can override in `docker-compose.yml`.

### Rule-based detector (`detector/detector.py`)

Edit these constants and rebuild, OR set them as `environment:` overrides in compose:
- `PRICE_Z_THRESHOLD` — default 4.0
- `VOLUME_SPIKE_FACTOR` — default 10.0
- `AGGRESSOR_RATIO_HIGH` / `_LOW` — default 0.95 / 0.05
- `WARMUP_BARS` — default 30

### IF detector (`if_detector/if_detector.py`)

Override via compose env vars:
- `MIN_TRAINING_BARS` — default 1200 (≈20 min)
- `TRAIN_HOURS` — default 6
- `RETRAIN_INTERVAL_SEC` — default 1800 (30 min)
- `CONTAMINATION` — default 0.01

After changing, rebuild and restart:
```bash
docker compose up -d --build detector if_detector
```

---

## Common issues

### "Can't access localhost:5433 / 29092 from a browser"
Browsers speak HTTP. Postgres (5433) and Kafka (29092) use binary protocols. Only Grafana (3000) is browseable. Use `psql` for Postgres and Kafka CLI tools for Kafka.

### "Docker says port is already allocated"
Something else on your machine is holding the port:
- 5432 conflict → already handled (we use 5433)
- 3000 conflict → another app using it, change `3000:3000` to `3001:3000` in compose
- 29092 conflict → unlikely, change to another high port

### "Producer can't reach Binance (SSL errors)"
Most likely cause: antivirus or corporate proxy is doing SSL inspection. The producer runs in Docker (where the cert store is clean), so this shouldn't bite you. If running anything Python directly on the host that hits Binance, it will fail with SSL cert errors. Solution: run in Docker.

### "Aggregator says postgres not ready"
The aggregator auto-retries every 2 seconds. Postgres may take 10-20 seconds to fully start. If it persists, check `docker logs postgres` for errors.

### "Grafana dashboard is empty"
- Check time range (top right) — default is `now-30m`
- Check Grafana can reach Postgres: Sidebar → Connections → Data sources → Crypto-Postgres → Test
- Check aggregator is producing bars: `SELECT COUNT(*) FROM ohlcv_1s;` should be growing

### "IF detector won't start / says waiting for data"
It needs at least 1200 bars (~20 min of live data) before training. Check progress:
```bash
docker exec postgres psql -U crypto -d crypto -tAc "SELECT COUNT(*) FROM ohlcv_1s;"
```

### "Detector caught 12% of bars as anomalies, way too many"
Volume median can be tiny during quiet markets, causing inflated spike ratios. Two fixes already in code:
1. `VOLUME_MEDIAN_FLOOR` (0.05 BTC) — prevents division by near-zero
2. Volume warmup requirement (`WINDOW_VOLUME // 2 = 150 bars`) before volume_spike fires

If still too noisy, raise `VOLUME_SPIKE_FACTOR` from 10 to 20 or 30.

---

## File structure reference

```
.
├── docker-compose.yml          # All service definitions
├── .env                        # Default credentials
├── .gitignore
├── README.md
│
├── db/
│   └── init.sql                # Postgres schema, runs on first volume init
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py             # Binance WS → Kafka
│
├── aggregator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── aggregator.py           # Kafka → ohlcv_1s
│
├── detector/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── detector.py             # Rule-based anomaly detector
│
├── if_detector/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── if_detector.py          # Isolation Forest anomaly detector
│
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/postgres.yml
│   │   └── dashboards/dashboards.yml
│   └── dashboards/
│       └── crypto.json
│
└── docs/                       # The explainer docs
    ├── 01-crypto-primer.md
    ├── 02-architecture.md
    ├── 03-anomaly-detection.md
    ├── 04-dashboard-guide.md
    └── 05-operations.md        # ← this file
```

---

## Resetting to a known-good state

If something gets weird, this is the nuclear reset:

```bash
docker compose down -v
docker compose build
docker compose up -d
```

You'll lose all stored bars and anomalies and have to wait for the system to re-accumulate data, but every component will be fresh.
