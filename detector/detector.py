import json
import logging
import math
import os
import signal
import statistics
import time
from collections import deque
from datetime import timedelta
from typing import Optional

import psycopg

PG_DSN = os.getenv("PG_DSN", "postgresql://crypto:cryptopass@postgres:5432/crypto")
SYMBOL = os.getenv("SYMBOL", "btcusdt").upper()
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "1.0"))

WINDOW_RETURNS = 60
WINDOW_VOLUME = 300
CONTEXT_BARS = 60

PRICE_Z_THRESHOLD = 4.0
PRICE_MIN_ABS_RETURN = 1e-4
VOLUME_SPIKE_FACTOR = 10.0
VOLUME_MIN_BTC = 0.05
VOLUME_MEDIAN_FLOOR = 0.05
AGGRESSOR_RATIO_HIGH = 0.95
AGGRESSOR_RATIO_LOW = 0.05
AGGRESSOR_MIN_VOLUME = 0.5

WARMUP_BARS = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("detector")


SELECT_SQL = """
SELECT bucket, open, high, low, close, volume, quote_volume,
       buy_volume, sell_volume, vwap, trade_count
FROM ohlcv_1s
WHERE symbol = %s AND bucket > %s
ORDER BY bucket ASC
LIMIT 500
"""

LAST_BUCKET_SQL = """
SELECT COALESCE(MAX(window_end), 'epoch'::timestamptz)
FROM anomalies WHERE symbol = %s
"""

INSERT_SQL = """
INSERT INTO anomalies (symbol, window_start, window_end, detector, direction,
                       score, features, raw_context)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


class Bar:
    __slots__ = ("bucket", "open", "high", "low", "close", "volume",
                 "quote_volume", "buy_volume", "sell_volume", "vwap", "trade_count")

    def __init__(self, row):
        (self.bucket, self.open, self.high, self.low, self.close,
         self.volume, self.quote_volume, self.buy_volume, self.sell_volume,
         self.vwap, self.trade_count) = row

    def to_dict(self):
        return {
            "bucket": self.bucket.isoformat(),
            "open": self.open, "high": self.high, "low": self.low, "close": self.close,
            "volume": self.volume, "quote_volume": self.quote_volume,
            "buy_volume": self.buy_volume, "sell_volume": self.sell_volume,
            "vwap": self.vwap, "trade_count": self.trade_count,
        }


def safe_aggressor_ratio(buy: float, sell: float) -> Optional[float]:
    total = buy + sell
    return buy / total if total > 0 else None


def detect(bar: Bar, returns: deque, volumes: deque) -> list[dict]:
    """Return list of anomaly dicts for this bar (may be empty)."""
    anomalies: list[dict] = []
    if len(returns) < WARMUP_BARS or len(volumes) < WINDOW_VOLUME // 2:
        return anomalies

    ret_mean = statistics.fmean(returns)
    ret_std = statistics.pstdev(returns) or 1e-9
    last_ret = returns[-1]
    ret_z = (last_ret - ret_mean) / ret_std

    vol_median = max(statistics.median(volumes), VOLUME_MEDIAN_FLOOR)
    vol_spike = bar.volume / vol_median

    agg_ratio = safe_aggressor_ratio(bar.buy_volume, bar.sell_volume)

    features = {
        "close": bar.close,
        "log_return": last_ret,
        "return_zscore": ret_z,
        "volume": bar.volume,
        "volume_median_5m": vol_median,
        "volume_spike_ratio": vol_spike,
        "aggressor_ratio": agg_ratio,
        "trade_count": bar.trade_count,
    }

    # Rule 1: price z-score outlier
    if abs(ret_z) >= PRICE_Z_THRESHOLD and abs(last_ret) >= PRICE_MIN_ABS_RETURN:
        anomalies.append({
            "detector": "price_zscore",
            "score": abs(ret_z),
            "direction": "pump" if last_ret > 0 else "dump",
            "features": features,
        })

    # Rule 2: volume spike
    if vol_spike >= VOLUME_SPIKE_FACTOR and bar.volume >= VOLUME_MIN_BTC:
        if last_ret > 0:
            direction = "pump"
        elif last_ret < 0:
            direction = "dump"
        else:
            direction = "unknown"
        anomalies.append({
            "detector": "volume_spike",
            "score": vol_spike,
            "direction": direction,
            "features": features,
        })

    # Rule 3: aggressor imbalance with meaningful volume
    if agg_ratio is not None and bar.volume >= AGGRESSOR_MIN_VOLUME:
        if agg_ratio >= AGGRESSOR_RATIO_HIGH:
            anomalies.append({
                "detector": "aggressor_imbalance",
                "score": agg_ratio,
                "direction": "pump",
                "features": features,
            })
        elif agg_ratio <= AGGRESSOR_RATIO_LOW:
            anomalies.append({
                "detector": "aggressor_imbalance",
                "score": 1.0 - agg_ratio,
                "direction": "dump",
                "features": features,
            })

    return anomalies


def connect_pg() -> psycopg.Connection:
    while True:
        try:
            return psycopg.connect(PG_DSN, autocommit=True)
        except psycopg.OperationalError as e:
            log.warning("postgres not ready (%s); retrying in 2s", e)
            time.sleep(2)


def main() -> None:
    conn = connect_pg()
    cur = conn.cursor()

    cur.execute(LAST_BUCKET_SQL, (SYMBOL,))
    last_seen = cur.fetchone()[0]
    log.info("starting from last_seen=%s symbol=%s", last_seen, SYMBOL)

    returns: deque[float] = deque(maxlen=WINDOW_RETURNS)
    volumes: deque[float] = deque(maxlen=WINDOW_VOLUME)
    context: deque[Bar] = deque(maxlen=CONTEXT_BARS)
    prev_close: Optional[float] = None
    processed = 0
    flagged = 0
    last_stats = time.monotonic()

    stopping = False
    def stop(_s, _f):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    while not stopping:
        cur.execute(SELECT_SQL, (SYMBOL, last_seen))
        rows = cur.fetchall()

        for row in rows:
            bar = Bar(row)
            last_seen = bar.bucket

            if prev_close is not None and prev_close > 0 and bar.close > 0:
                returns.append(math.log(bar.close / prev_close))
            volumes.append(bar.volume)
            context.append(bar)
            prev_close = bar.close
            processed += 1

            for anom in detect(bar, returns, volumes):
                window_end = bar.bucket
                window_start = window_end - timedelta(seconds=CONTEXT_BARS)
                cur.execute(
                    INSERT_SQL,
                    (
                        SYMBOL, window_start, window_end,
                        anom["detector"], anom["direction"], anom["score"],
                        json.dumps(anom["features"]),
                        json.dumps([b.to_dict() for b in context]),
                    ),
                )
                flagged += 1
                log.info(
                    "ANOMALY bucket=%s detector=%s direction=%s score=%.3f close=%.2f",
                    bar.bucket.isoformat(), anom["detector"], anom["direction"],
                    anom["score"], bar.close,
                )

        now = time.monotonic()
        if now - last_stats >= 10:
            log.info("processed=%d flagged=%d returns_buf=%d volumes_buf=%d",
                     processed, flagged, len(returns), len(volumes))
            processed = 0
            flagged = 0
            last_stats = now

        time.sleep(POLL_INTERVAL_SEC)

    cur.close()
    conn.close()
    log.info("done")


if __name__ == "__main__":
    main()
