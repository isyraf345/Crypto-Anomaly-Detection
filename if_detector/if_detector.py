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

import numpy as np
import psycopg
from sklearn.ensemble import IsolationForest

PG_DSN = os.getenv("PG_DSN", "postgresql://crypto:cryptopass@postgres:5432/crypto")
SYMBOL = os.getenv("SYMBOL", "btcusdt").upper()
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "1.0"))

WINDOW_RETURNS = 60
WINDOW_VOLUME = 300
CONTEXT_BARS = 60

TRAIN_HOURS = float(os.getenv("TRAIN_HOURS", "6"))
RETRAIN_INTERVAL_SEC = float(os.getenv("RETRAIN_INTERVAL_SEC", "1800"))
MIN_TRAINING_BARS = int(os.getenv("MIN_TRAINING_BARS", "1200"))
N_ESTIMATORS = 100
CONTAMINATION = float(os.getenv("CONTAMINATION", "0.01"))
RANDOM_STATE = 42

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("if_detector")


SELECT_NEW_BARS_SQL = """
SELECT bucket, open, high, low, close, volume, quote_volume,
       buy_volume, sell_volume, vwap, trade_count
FROM ohlcv_1s
WHERE symbol = %s AND bucket > %s
ORDER BY bucket ASC
LIMIT 500
"""

SELECT_TRAIN_SQL = """
SELECT bucket, open, high, low, close, volume, quote_volume,
       buy_volume, sell_volume, vwap, trade_count
FROM ohlcv_1s
WHERE symbol = %s AND bucket >= NOW() - INTERVAL '%s hours'
ORDER BY bucket ASC
"""

LAST_IF_BUCKET_SQL = """
SELECT COALESCE(MAX(window_end), 'epoch'::timestamptz)
FROM anomalies WHERE symbol = %s AND detector = 'isolation_forest'
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


FEATURE_NAMES = [
    "log_return", "return_zscore", "log_volume", "volume_ratio",
    "aggressor_ratio", "log_trade_count", "realized_vol",
    "vwap_dev", "price_range",
]


def compute_features(
    bar: Bar,
    prev_close: Optional[float],
    returns_buf: deque,
    volumes_buf: deque,
) -> Optional[list[float]]:
    if prev_close is None or prev_close <= 0 or bar.close <= 0:
        return None
    log_ret = math.log(bar.close / prev_close)
    if len(returns_buf) < 10 or len(volumes_buf) < 10:
        return None
    ret_std = statistics.pstdev(returns_buf) or 1e-9
    vol_median = statistics.median(volumes_buf) or 1e-9
    agg_total = bar.buy_volume + bar.sell_volume
    agg_ratio = bar.buy_volume / agg_total if agg_total > 0 else 0.5
    vwap_dev = (bar.close - bar.vwap) / bar.vwap if bar.vwap > 0 else 0.0
    price_range = (bar.high - bar.low) / bar.close if bar.close > 0 else 0.0
    return [
        log_ret,
        log_ret / ret_std,
        math.log1p(bar.volume),
        bar.volume / vol_median,
        agg_ratio,
        math.log1p(bar.trade_count),
        ret_std,
        vwap_dev,
        price_range,
    ]


def train_model(cur) -> tuple[Optional[IsolationForest], deque, deque, Optional[float], int]:
    """Returns (model, returns_buf, volumes_buf, prev_close, training_size)."""
    cur.execute(SELECT_TRAIN_SQL, (SYMBOL, TRAIN_HOURS))
    rows = cur.fetchall()
    if len(rows) < MIN_TRAINING_BARS:
        log.info("training skipped: only %d bars (<%d)", len(rows), MIN_TRAINING_BARS)
        return None, deque(maxlen=WINDOW_RETURNS), deque(maxlen=WINDOW_VOLUME), None, 0

    returns_buf: deque[float] = deque(maxlen=WINDOW_RETURNS)
    volumes_buf: deque[float] = deque(maxlen=WINDOW_VOLUME)
    prev_close: Optional[float] = None
    feats: list[list[float]] = []

    for row in rows:
        bar = Bar(row)
        f = compute_features(bar, prev_close, returns_buf, volumes_buf)
        if f is not None:
            feats.append(f)
        if prev_close is not None and prev_close > 0 and bar.close > 0:
            returns_buf.append(math.log(bar.close / prev_close))
        volumes_buf.append(bar.volume)
        prev_close = bar.close

    if len(feats) < MIN_TRAINING_BARS:
        log.info("training skipped: %d feature rows (<%d)", len(feats), MIN_TRAINING_BARS)
        return None, returns_buf, volumes_buf, prev_close, 0

    X = np.array(feats, dtype=np.float64)
    model = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,
        random_state=RANDOM_STATE,
        n_jobs=1,
    )
    model.fit(X)
    log.info("trained IsolationForest on %d feature rows (contamination=%.3f)",
             len(feats), CONTAMINATION)
    return model, returns_buf, volumes_buf, prev_close, len(feats)


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

    # Wait until enough data exists to train
    while True:
        cur.execute("SELECT COUNT(*) FROM ohlcv_1s WHERE symbol = %s", (SYMBOL,))
        n = cur.fetchone()[0]
        if n >= MIN_TRAINING_BARS:
            log.info("found %d bars, proceeding to train", n)
            break
        log.info("waiting for data: have %d bars, need %d", n, MIN_TRAINING_BARS)
        time.sleep(30)

    model, returns_buf, volumes_buf, prev_close, _ = train_model(cur)
    if model is None:
        log.error("initial training failed; exiting")
        return
    last_train = time.monotonic()

    cur.execute(LAST_IF_BUCKET_SQL, (SYMBOL,))
    last_seen = cur.fetchone()[0]
    log.info("starting scoring loop from last_seen=%s", last_seen)

    context: deque[Bar] = deque(maxlen=CONTEXT_BARS)
    scored = 0
    flagged = 0
    last_stats = time.monotonic()

    stopping = False
    def stop(_s, _f):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    while not stopping:
        cur.execute(SELECT_NEW_BARS_SQL, (SYMBOL, last_seen))
        rows = cur.fetchall()

        for row in rows:
            bar = Bar(row)
            last_seen = bar.bucket
            context.append(bar)

            feat = compute_features(bar, prev_close, returns_buf, volumes_buf)

            if prev_close is not None and prev_close > 0 and bar.close > 0:
                returns_buf.append(math.log(bar.close / prev_close))
            volumes_buf.append(bar.volume)
            prev_close = bar.close

            if feat is None:
                continue

            X = np.array([feat], dtype=np.float64)
            score = float(model.decision_function(X)[0])
            pred = int(model.predict(X)[0])
            scored += 1

            if pred == -1:
                features_dict = dict(zip(FEATURE_NAMES, feat))
                features_dict["close"] = bar.close
                features_dict["if_score"] = score
                log_ret = feat[0]
                if log_ret > 0:
                    direction = "pump"
                elif log_ret < 0:
                    direction = "dump"
                else:
                    direction = "unknown"

                window_end = bar.bucket
                window_start = window_end - timedelta(seconds=CONTEXT_BARS)
                cur.execute(
                    INSERT_SQL,
                    (
                        SYMBOL, window_start, window_end,
                        "isolation_forest", direction, abs(score),
                        json.dumps(features_dict),
                        json.dumps([b.to_dict() for b in context]),
                    ),
                )
                flagged += 1
                log.info("ANOMALY bucket=%s direction=%s score=%.4f close=%.2f",
                         bar.bucket.isoformat(), direction, score, bar.close)

        now = time.monotonic()
        if now - last_stats >= 10:
            log.info("scored=%d flagged=%d", scored, flagged)
            scored = 0
            flagged = 0
            last_stats = now

        if now - last_train >= RETRAIN_INTERVAL_SEC:
            log.info("retraining...")
            new_model, new_returns, new_volumes, new_prev_close, n_train = train_model(cur)
            if new_model is not None:
                model = new_model
                returns_buf = new_returns
                volumes_buf = new_volumes
                prev_close = new_prev_close
                log.info("model swapped (trained on %d rows)", n_train)
            last_train = now

        time.sleep(POLL_INTERVAL_SEC)

    cur.close()
    conn.close()
    log.info("done")


if __name__ == "__main__":
    main()
