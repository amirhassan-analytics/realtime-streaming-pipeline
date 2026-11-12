-- Streaming warehouse schema (PostgreSQL).
CREATE TABLE IF NOT EXISTS fact_ticks (
    symbol TEXT,
    price NUMERIC,
    volume INT,
    ts TIMESTAMPTZ,
    received_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mart_minute_bars (
    symbol TEXT,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,
    total_volume BIGINT,
    PRIMARY KEY (symbol, window_start)
);
