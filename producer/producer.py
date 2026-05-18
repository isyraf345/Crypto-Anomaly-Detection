import asyncio
import json
import logging
import os
import signal
import ssl
import time

import certifi
import websockets
from confluent_kafka import Producer

SSL_CTX = ssl.create_default_context(cafile=certifi.where())

SYMBOL = os.getenv("SYMBOL", "btcusdt").lower()
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade"
TOPIC = f"trades.{SYMBOL}"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
LOG_INTERVAL_SEC = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("producer")


def make_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "linger.ms": 10,
            "compression.type": "lz4",
            "acks": "1",
            "enable.idempotence": False,
        }
    )


def delivery_report(err, msg):
    if err is not None:
        log.warning("delivery failed: %s", err)


async def stream(producer: Producer, stop: asyncio.Event) -> None:
    backoff = 1
    sent = 0
    last_log = time.monotonic()

    while not stop.is_set():
        try:
            async with websockets.connect(WS_URL, ssl=SSL_CTX, ping_interval=20, ping_timeout=20) as ws:
                log.info("connected to %s", WS_URL)
                backoff = 1
                async for raw in ws:
                    if stop.is_set():
                        break
                    trade = json.loads(raw)
                    key = str(trade["t"]).encode()
                    producer.produce(TOPIC, key=key, value=raw, callback=delivery_report)
                    producer.poll(0)
                    sent += 1

                    now = time.monotonic()
                    if now - last_log >= LOG_INTERVAL_SEC:
                        rate = sent / (now - last_log)
                        log.info("sent=%d rate=%.1f msg/s", sent, rate)
                        sent = 0
                        last_log = now
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("ws error (%s); reconnecting in %ds", e, backoff)
            try:
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 30)


async def main() -> None:
    producer = make_producer()
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    try:
        await stream(producer, stop)
    finally:
        log.info("flushing producer...")
        producer.flush(10)
        log.info("done")


if __name__ == "__main__":
    asyncio.run(main())
