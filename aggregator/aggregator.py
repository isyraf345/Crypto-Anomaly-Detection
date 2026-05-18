import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SYMBOL = os.getenv("SYMBOL", "btcusdt").upper()
TOPIC = f"trades.{SYMBOL.lower()}"
GROUP_ID = os.getenv("GROUP_ID", "aggregator-v1")
PG_DSN = os.getenv("PG_DSN", "postgresql://crypto:cryptopass@postgres:5432/crypto")
GRACE_SECONDS = 2
STATS_INTERVAL_SEC = 10

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("aggregator")


class Bucket:
    __slots__ = ("open", "high", "low", "close", "volume", "quote_volume",
                 "buy_volume", "sell_volume", "trade_count")

    def __init__(self) -> None:
        self.open = None
        self.high = float("-inf")
        self.low = float("inf")
        self.close = None
        self.volume = 0.0
        self.quote_volume = 0.0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self.trade_count = 0

    def add(self, p: float, q: float, is_buyer_maker: bool) -> None:
        if self.open is None:
            self.open = p
        if p > self.high:
            self.high = p
        if p < self.low:
            self.low = p
        self.close = p
        self.volume += q
        self.quote_volume += p * q
        if is_buyer_maker:
            # Aggressor was the seller -> market sell
            self.sell_volume += q
        else:
            self.buy_volume += q
        self.trade_count += 1


UPSERT_SQL = """
INSERT INTO ohlcv_1s (symbol, bucket, open, high, low, close, volume, quote_volume,
                      buy_volume, sell_volume, vwap, trade_count)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, bucket) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    quote_volume = EXCLUDED.quote_volume,
    buy_volume = EXCLUDED.buy_volume,
    sell_volume = EXCLUDED.sell_volume,
    vwap = EXCLUDED.vwap,
    trade_count = EXCLUDED.trade_count
"""


def flush_bucket(cur, symbol: str, bucket_sec: int, agg: Bucket) -> None:
    vwap = agg.quote_volume / agg.volume if agg.volume > 0 else agg.close
    bucket_ts = datetime.fromtimestamp(bucket_sec, tz=timezone.utc)
    cur.execute(
        UPSERT_SQL,
        (
            symbol, bucket_ts, agg.open, agg.high, agg.low, agg.close,
            agg.volume, agg.quote_volume, agg.buy_volume, agg.sell_volume,
            vwap, agg.trade_count,
        ),
    )


def connect_pg() -> psycopg.Connection:
    while True:
        try:
            return psycopg.connect(PG_DSN, autocommit=True)
        except psycopg.OperationalError as e:
            log.warning("postgres not ready (%s); retrying in 2s", e)
            time.sleep(2)


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC])

    conn = connect_pg()
    cur = conn.cursor()

    buckets: dict[int, Bucket] = {}
    max_bucket = 0
    flushed = 0
    last_stats = time.monotonic()

    stopping = False
    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    log.info("subscribed to %s, group=%s", TOPIC, GROUP_ID)

    try:
        while not stopping:
            msg = consumer.poll(1.0)

            if msg is not None and not msg.error():
                try:
                    trade = json.loads(msg.value())
                    p = float(trade["p"])
                    q = float(trade["q"])
                    t_ms = int(trade["T"])
                    is_buyer_maker = bool(trade["m"])
                except (KeyError, ValueError, TypeError) as e:
                    log.warning("bad trade msg: %s", e)
                    continue

                bucket_sec = t_ms // 1000
                if bucket_sec > max_bucket:
                    max_bucket = bucket_sec
                if bucket_sec not in buckets:
                    buckets[bucket_sec] = Bucket()
                buckets[bucket_sec].add(p, q, is_buyer_maker)
            elif msg is not None and msg.error():
                log.warning("consumer error: %s", msg.error())

            # Flush buckets that are old enough (use wall-clock as fallback
            # so quiet periods still close out open buckets)
            cutoff = max(max_bucket, int(time.time())) - GRACE_SECONDS
            to_flush = sorted(k for k in buckets if k <= cutoff)
            for k in to_flush:
                flush_bucket(cur, SYMBOL, k, buckets.pop(k))
                flushed += 1

            now = time.monotonic()
            if now - last_stats >= STATS_INTERVAL_SEC:
                log.info("flushed=%d open=%d max_bucket=%d",
                         flushed, len(buckets), max_bucket)
                flushed = 0
                last_stats = now
    finally:
        log.info("draining remaining buckets...")
        for k in sorted(buckets):
            flush_bucket(cur, SYMBOL, k, buckets[k])
        cur.close()
        conn.close()
        consumer.close()
        log.info("done")


if __name__ == "__main__":
    main()
