-- Raw order ticks as they arrive from the consumer
CREATE TABLE IF NOT EXISTS ticks (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    side        TEXT NOT NULL,          -- BUY / SELL
    price       NUMERIC(12, 4) NOT NULL,
    size        INTEGER NOT NULL,
    order_type  TEXT NOT NULL,          -- LIMIT / MARKET / CANCEL
    order_id    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ticks_ts_symbol ON ticks (symbol, ts);

-- 1-second OHLC candles produced by the PySpark job
CREATE TABLE IF NOT EXISTS candles_1s (
    ts_bucket   TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    open        NUMERIC(12, 4) NOT NULL,
    high        NUMERIC(12, 4) NOT NULL,
    low         NUMERIC(12, 4) NOT NULL,
    close       NUMERIC(12, 4) NOT NULL,
    volume      BIGINT NOT NULL,
    tick_count  INTEGER NOT NULL,
    PRIMARY KEY (ts_bucket, symbol)
);

CREATE INDEX IF NOT EXISTS idx_candles_ts ON candles_1s (ts_bucket DESC);
