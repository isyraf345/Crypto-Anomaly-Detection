# 06 — Kafka in This Project

Everything you need to know about Kafka's role here: what it is, why we use it, how every configuration line is tuned for our needs, and how to inspect/debug it.

---

## 1. What Kafka is (and isn't)

Kafka is a **distributed commit log** — a system you talk to over the network that lets:

- One or more **producers** write messages to a named stream (a **topic**)
- One or more **consumers** read messages from that topic at their own pace
- Messages stay on disk for a configurable retention period, after which they're deleted

That's the entire core idea. Everything else — partitioning, replication, consumer groups — is mechanism for scaling and reliability.

### What Kafka isn't

| Common misconception | Reality |
|---|---|
| "Kafka is a Python library" | Kafka is a **server** (Java application). We *connect* to it via a client library. |
| "Kafka is a database" | It's a log, not a queryable store. You can replay it sequentially but can't `SELECT WHERE id=42`. |
| "Kafka is a message queue like RabbitMQ" | Closer to "durable append-only file." Messages aren't deleted on read; consumers track their own position. |
| "Once consumed, messages are gone" | Messages stay until retention expires. Multiple consumers can read the same messages independently. |

### What's running in our project

When you do `docker compose up`, the `kafka` service starts a Kafka **broker** — that's the actual Kafka server. It listens on TCP port 9092 inside the Docker network. Our Python code (producer, aggregator) connects to it like a client connects to a database. We didn't write Kafka — we just rent it.

---

## 2. Why Kafka for this project

The system has a particular shape that maps almost perfectly onto what Kafka is built for.

### The problem

- **Source we don't control:** Binance pushes trades whenever they happen. We can't ask them to slow down.
- **Bursty rate:** 1-5 trades/sec when quiet, 500-1000/sec during volatility — a 200× swing.
- **Multiple consumers might want the same data:** today the aggregator buckets into OHLCV; tomorrow we might add a Slack alerter, a raw-trade archiver, a different ML model.
- **Downstream services can fail:** Postgres can lock up. The aggregator can crash. The producer can't stop receiving from Binance just because something downstream hiccuped.
- **We don't want 7-10 GB/day of raw trades persisted forever**, but we do want a buffer so we can recover from short outages.

### What Kafka solves

| Problem | How Kafka helps |
|---|---|
| Bursty input rate | Acts as a shock absorber. Trades pile up in Kafka safely while consumers drain at their own pace. |
| Uncontrollable source | Producer just writes to Kafka — never blocked by slow consumers. |
| Multiple readers | Each consumer group is independent. The aggregator reading doesn't affect anything else. |
| Downstream failures | Postgres goes down → aggregator pauses → trades pile up in Kafka safely → aggregator resumes when Postgres is back. Zero data loss. |
| Short-term buffer only | Retention auto-deletes raw messages after N hours. |

### If we didn't have Kafka

If the producer wrote directly to Postgres:
- Postgres restart = lost trades (no buffer)
- Adding a second consumer requires modifying the producer (tight coupling)
- A 1000 trades/sec burst can outpace Postgres inserts (no shock absorber)
- The producer becomes responsible for retry logic, batching, queuing — all the things Kafka already does

Kafka is the **seam** that lets us evolve each side independently.

---

## 3. Kafka concepts in this project

| Concept | What it is | Our value |
|---|---|---|
| **Broker** | A Kafka server process | 1 broker (single-node, fine for dev) |
| **Cluster** | Group of brokers | 1 broker = 1-node cluster |
| **Topic** | Named append-only stream | `trades.btcusdt` |
| **Partition** | Sub-stream for parallelism within a topic | 1 (no parallelism yet) |
| **Offset** | Per-message sequence number within a partition | Kafka assigns, consumers track |
| **Producer** | Client that writes messages | Our `producer/` service |
| **Consumer** | Client that reads messages | Our `aggregator/` service |
| **Consumer group** | Multiple consumers sharing a topic, each partition assigned to one | `aggregator-v1` |
| **Retention** | How long messages stay before deletion | 4 hours |
| **Zookeeper** | Cluster coordination (cluster metadata, leader election) | One container, port 2181 |

### Topic shape in this project

```
Topic: trades.btcusdt
├── Partition 0
│   ├── offset 0: {"e":"trade","t":6300000001,"p":"78100","q":"0.001",...}
│   ├── offset 1: {"e":"trade","t":6300000002,"p":"78100.5","q":"0.05",...}
│   ├── ...
│   └── offset 14,287,341: <newest trade>
```

One topic, one partition, growing over time. Older messages get garbage-collected after 4 hours.

---

## 4. Configuration walkthrough — every line explained

### From `docker-compose.yml`

```yaml
kafka:
  image: confluentinc/cp-kafka:7.5.0
  container_name: kafka
  depends_on:
    zookeeper:
      condition: service_healthy
  ports:
    - "29092:29092"
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    KAFKA_LOG_RETENTION_HOURS: 4
  healthcheck:
    test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1"]
    interval: 10s
    timeout: 5s
    retries: 10
```

### `image: confluentinc/cp-kafka:7.5.0`
**The Confluent distribution** of Kafka — same Apache Kafka core, packaged with helpful tools like `cub zk-ready`. Most-used Kafka image in production. Pinned to 7.5.0 for reproducibility.

### `depends_on: zookeeper: service_healthy`
Kafka 7.5 still uses Zookeeper for cluster metadata. This prevents a startup race where Kafka tries to connect to Zookeeper before Zookeeper is ready. (Kafka 4.x supports KRaft mode without Zookeeper — newer setups can drop this dependency.)

### `ports: "29092:29092"`
Exposes **only the host-facing port** to your Windows machine. The internal port `9092` is intentionally *not* exposed because nothing on the host should use it — see the dual-listener explanation below.

### `KAFKA_BROKER_ID: 1`
Every broker needs a unique ID. We have one broker = `1`. Production would have 3+ brokers with unique IDs.

### `KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181`
Kafka asks Zookeeper "where are the topic partitions? who's the cluster leader?" `zookeeper` is the Docker network name of the Zookeeper container.

### The listener pair (the tricky one)

```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
```

This pair is necessary because of an awkward fact: clients connecting **from inside Docker** and clients connecting **from your Windows host** need to use *different addresses* for the same broker.

Kafka has a quirk: when a client connects, the broker tells it **"reach me at this address from now on"**. If the broker tells an in-Docker client "reach me at `localhost:29092`", the client tries `localhost` *inside its own container* and fails. If it tells a Windows client "reach me at `kafka:9092`", Windows has no idea what `kafka` means.

The fix is **two named listeners**:

| Listener name | Advertised address | Used by |
|---|---|---|
| `PLAINTEXT` | `kafka:9092` | Other Docker containers (producer, aggregator) |
| `PLAINTEXT_HOST` | `localhost:29092` | Your Windows machine via the mapped port |

`PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT` means "both listeners use unencrypted plaintext" (fine for local dev; production would use TLS — `SASL_SSL`).

If you ever see Python client errors like `Connection refused` or `No address associated with hostname`, this dual-listener config is the first thing to check.

### `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`
Kafka stores consumer offsets in an internal topic called `__consumer_offsets`. Default replication is 3 (data is mirrored across 3 brokers). We have 1 broker, so 3-way replication is impossible. Setting to 1 lets Kafka start. **Production: 3.**

### `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"`
When the producer publishes to `trades.btcusdt` for the first time, Kafka auto-creates the topic. **Convenient for development, dangerous in production** — a typo silently creates a new empty topic instead of erroring. We accept this tradeoff for simplicity.

### `KAFKA_LOG_RETENTION_HOURS: 4`
**The most project-specific setting.** Default retention is 168 hours (7 days). We set 4.

Reasoning:
- At peak ~1000 trades/sec, 4 hours ≈ 14M messages — plenty of buffer if the aggregator is offline for hours.
- Beyond 4 hours, we don't care about raw trades — the 1-second OHLCV bars in Postgres preserve everything we need.
- Default 7-day retention = ~600M messages on disk = bloat.

This single line implements the storage strategy from the original project brief: **raw ephemeral, aggregates persistent.**

### Healthcheck

```yaml
healthcheck:
  test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1"]
```

Asks the broker to report its supported API versions. If that responds, the broker is alive and accepting connections. Docker uses this to delay starting dependent services until Kafka is truly ready (not just "process started").

---

## 5. Producer-side tuning

In `producer/producer.py`:

```python
Producer({
    "bootstrap.servers": BOOTSTRAP,
    "linger.ms": 10,
    "compression.type": "lz4",
    "acks": "1",
    "enable.idempotence": False,
})
```

| Setting | Value | Why |
|---|---|---|
| `bootstrap.servers` | `kafka:9092` (in-Docker) | Initial discovery endpoint; client gets the rest of the cluster from here |
| `linger.ms` | `10` | Wait up to 10ms to batch messages before sending. Tiny latency cost, much higher throughput. |
| `compression.type` | `lz4` | ~50% smaller on the wire and on disk. Negligible CPU cost. |
| `acks` | `"1"` | "Leader acknowledges, don't wait for replicas." Fast. Acceptable since we have only one broker anyway. |
| `enable.idempotence` | `False` | We tolerate occasional duplicates (the aggregator's upserts handle them). Idempotence costs ~5% throughput. |

### Message structure

The producer publishes each Binance trade verbatim:

```python
producer.produce(
    TOPIC,                          # "trades.btcusdt"
    key=str(trade["t"]).encode(),   # trade ID as key
    value=raw,                      # raw JSON bytes from Binance
    callback=delivery_report,
)
```

Using trade ID as the **key** matters: Kafka guarantees that messages with the same key always go to the same partition. With 1 partition that's moot, but if we ever scale to multiple partitions, all data for a given trade ID stays in order.

### Production tradeoffs (not changed)

| Setting | Dev | Production-style |
|---|---|---|
| `acks` | `1` | `"all"` (wait for all replicas) |
| `enable.idempotence` | `False` | `True` |
| `retries` | default | high value with `delivery.timeout.ms` set |
| `max.in.flight.requests.per.connection` | default 5 | 5 + idempotence (or 1 without) for ordering guarantees |

---

## 6. Consumer-side tuning

In `aggregator/aggregator.py`:

```python
Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "aggregator-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
})
consumer.subscribe([TOPIC])
```

| Setting | Value | Why |
|---|---|---|
| `bootstrap.servers` | `kafka:9092` | In-Docker connection |
| `group.id` | `aggregator-v1` | Consumer group name. Kafka tracks our consumed offset *per group*. |
| `auto.offset.reset` | `earliest` | If we have no saved offset (new group), start from the oldest message. Lets a fresh aggregator backfill all bars from Kafka's 4-hour buffer. |
| `enable.auto.commit` | `True` | Commit offsets in the background. Simple, acceptable for our workload. |

### Why version the group ID

`aggregator-v1` is intentionally versioned. If we ever change the aggregation logic incompatibly (e.g., switch from 1-sec bars to 5-sec bars), bumping to `aggregator-v2` gives the new consumer a clean slate:
- Kafka treats `v2` as a brand-new consumer group
- `auto.offset.reset=earliest` kicks in → replays all available data
- Old `v1` consumers (if any still running) keep their separate offsets

It's the cheapest possible migration story.

### The poll loop

```python
while not stopping:
    msg = consumer.poll(1.0)   # wait up to 1 second for a new message
    if msg is None:
        # no new data; do periodic housekeeping
        continue
    # ... process trade and bucket it
```

`poll(1.0)` blocks up to 1 second waiting for a message. If a message is available immediately, it returns immediately. This is the heartbeat of any Kafka consumer.

---

## 7. The producer/broker/consumer message flow

Walking through what happens to **a single trade**:

1. **Binance** sends a WebSocket frame to the `producer` container.
2. **Producer** parses the JSON, calls:
   ```python
   producer.produce(TOPIC, key=trade_id_bytes, value=raw_bytes)
   ```
   This **buffers** the message in the client library's send queue — does NOT yet hit the network.
3. Up to 10ms later (or sooner if the buffer fills), the client library batches whatever's buffered and sends a single TCP write to the broker.
4. **Broker** appends the batch to its on-disk log for partition 0 of `trades.btcusdt`. Each message gets an offset (e.g., `14,287,341`).
5. Broker replies "got it" → producer's `delivery_report` callback fires (we only log on failure).
6. **Aggregator** is sitting in `consumer.poll(1.0)`. The broker sends it the new batch.
7. Aggregator parses each trade, updates an in-memory bucket, occasionally upserts a completed bucket to Postgres.
8. Kafka client library auto-commits the consumer's offset back to the broker every few seconds, recording "aggregator-v1 has consumed up to offset 14,287,341."
9. **4 hours later**: the broker garbage-collects this trade from disk (retention expired).

If the aggregator crashes between steps 7 and 8, the offset isn't committed, so on restart it resumes from the last committed offset — re-processing a few messages. The aggregator's `ON CONFLICT DO UPDATE` upserts make this safe.

---

## 8. Practical CLI commands

All run via `docker exec kafka <command>` because the binaries live in the broker container.

### List topics
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Inspect a topic
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic trades.btcusdt
```

Shows partitions, leader, replication, in-sync replicas.

### Tail live messages
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic trades.btcusdt
```

Ctrl-C to stop. To see only the latest 5 and exit:
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic trades.btcusdt --max-messages 5 --timeout-ms 10000
```

### See how many messages total exist in the topic
```bash
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic trades.btcusdt
```

Output looks like `trades.btcusdt:0:14287341` → partition 0, latest offset 14,287,341.

### Check consumer group state
```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group aggregator-v1
```

Shows current offset, latest offset, and `LAG` per partition. Lag near zero = consumer keeping up. Growing lag = consumer too slow.

### Reset a consumer group's offset (replay from beginning)
```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group aggregator-v1 --topic trades.btcusdt --reset-offsets --to-earliest --execute
```

The consumer must be stopped when you do this.

### Delete a topic
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic trades.btcusdt
```

### Publish a test message
```bash
docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --topic trades.btcusdt
> {"hello":"world"}
> (Ctrl-D to exit)
```

---

## 9. What we deferred (production checklist)

For a real production deployment, we'd change these:

| Concern | Dev (current) | Production |
|---|---|---|
| Number of brokers | 1 | 3+ |
| Replication factor for app topics | 1 | 3 with `min.insync.replicas=2` |
| Replication factor for `__consumer_offsets` | 1 | 3 |
| Auto-create topics | Enabled | Disabled (typos shouldn't silently create topics) |
| Authentication | None (PLAINTEXT) | SASL/SCRAM or mTLS |
| Encryption | None | TLS (SASL_SSL) |
| ACLs | None | Per-topic read/write permissions per service |
| Monitoring | Just `docker logs` | JMX exporter → Prometheus → Grafana |
| Retention strategy | Time-based (4 hr) | Time + size (e.g., `log.retention.bytes`) |
| Cluster coordination | Zookeeper | KRaft (Kafka 4.x+) |
| Producer guarantees | `acks=1`, no idempotence | `acks=all` + idempotence |
| Schema | Raw JSON | Avro/Protobuf + Schema Registry |
| Disaster recovery | None | MirrorMaker 2 to a secondary cluster |

None of this is needed for a local single-laptop project. But it's worth knowing what the "production hardening" path looks like — these are the standard moves.

---

## 10. Glossary

| Term | Definition |
|---|---|
| **Broker** | A single Kafka server process |
| **Cluster** | A group of brokers acting as one logical Kafka |
| **Topic** | A named, append-only stream of messages |
| **Partition** | A sub-stream of a topic; messages within a partition are strictly ordered |
| **Offset** | Per-message sequence number within a partition |
| **Producer** | A client that writes messages to a topic |
| **Consumer** | A client that reads messages from a topic |
| **Consumer group** | A logical name for a set of consumers sharing the work of consuming a topic; Kafka tracks committed offset per group |
| **Replication factor** | How many copies of each message Kafka keeps across brokers |
| **Leader / follower** | For a partition, one broker is the leader (handles reads/writes); others are followers (replicate) |
| **In-sync replica (ISR)** | A follower that's caught up with the leader |
| **Retention** | How long Kafka keeps messages before garbage-collecting them |
| **Bootstrap server** | The initial address a client connects to in order to discover the cluster |
| **Zookeeper** | Cluster metadata coordinator (Kafka < 4.x uses it; KRaft replaces it) |
| **KRaft** | Kafka's built-in metadata mode (no Zookeeper) |
| **librdkafka** | The C library powering the `confluent-kafka` Python client |
| **Idempotent producer** | Kafka feature that deduplicates retries server-side |
| **At-least-once** | Default delivery semantic; messages may be processed more than once on failure |
| **Exactly-once** | Stronger semantic requiring idempotence + transactions; we don't use it here |

---

## 11. Further reading

- Apache Kafka docs: https://kafka.apache.org/documentation/
- Confluent's free Kafka course (no signup hard-sell): https://developer.confluent.io/learn-kafka/
- Why Kafka exists (the original LinkedIn paper): https://www.confluent.io/blog/event-streaming-platform-1/

But honestly — for understanding *this* project, this doc + the architecture doc is enough.
