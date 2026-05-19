# Real-Time Crypto Anomaly Detection

Streaming pipeline that watches live Bitcoin trades on Binance, flags unusual activity (pumps, dumps, volume spikes), and shows it on a Grafana dashboard.

Stack: Kafka, TimescaleDB, scikit-learn, Docker.

## Pipeline

```
Binance WebSocket → Kafka → Aggregator (1s OHLCV bars) → Postgres → Grafana
                              ↓
                          Rule detector + Isolation Forest
```

Four detectors run in parallel: price z-score, volume spike, aggressor imbalance, and an Isolation Forest retrained every 30 min on the last 6 hours.

## Run it

```bash
cp .env.example .env
docker compose up -d
```

- Grafana: http://localhost:3000 (`admin` / `admin`) → **Crypto Anomaly Detection — BTCUSDT**
- Postgres: `localhost:5433` (`crypto` / `cryptopass` / `crypto`)

Isolation Forest needs ~20 min of data before it starts firing. Rule detectors start almost immediately.

```bash
docker compose down       # stop
docker compose down -v    # stop and wipe data
```

## Docs

- [01-crypto-primer.md](docs/01-crypto-primer.md) — crypto/market terminology
- [02-architecture.md](docs/02-architecture.md) — system components and data flow
- [03-anomaly-detection.md](docs/03-anomaly-detection.md) — the math behind each detector
- [04-dashboard-guide.md](docs/04-dashboard-guide.md) — reading the Grafana panels
- [05-operations.md](docs/05-operations.md) — commands, logs, troubleshooting
- [06-kafka.md](docs/06-kafka.md) — Kafka config and CLI
- [07-isolation-forest.md](docs/07-isolation-forest.md) — IF detector internals, training, and tuning
- [db/README.md](db/README.md) — database schema
