CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS ohlcv_1s (
    symbol       TEXT        NOT NULL,
    bucket       TIMESTAMPTZ NOT NULL,
    open         DOUBLE PRECISION NOT NULL,
    high         DOUBLE PRECISION NOT NULL,
    low          DOUBLE PRECISION NOT NULL,
    close        DOUBLE PRECISION NOT NULL,
    volume       DOUBLE PRECISION NOT NULL,
    quote_volume DOUBLE PRECISION NOT NULL,
    buy_volume   DOUBLE PRECISION NOT NULL,
    sell_volume  DOUBLE PRECISION NOT NULL,
    vwap         DOUBLE PRECISION NOT NULL,
    trade_count  INTEGER          NOT NULL,
    PRIMARY KEY (symbol, bucket)
);

SELECT create_hypertable('ohlcv_1s', 'bucket', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS ohlcv_1s_symbol_bucket_idx
    ON ohlcv_1s (symbol, bucket DESC);

CREATE TABLE IF NOT EXISTS anomalies (
    id           BIGSERIAL PRIMARY KEY,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol       TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    detector     TEXT        NOT NULL,
    direction    TEXT        NOT NULL DEFAULT 'unknown',
    score        DOUBLE PRECISION,
    features     JSONB       NOT NULL,
    raw_context  JSONB
);

CREATE INDEX IF NOT EXISTS anomalies_symbol_time_idx
    ON anomalies (symbol, detected_at DESC);
