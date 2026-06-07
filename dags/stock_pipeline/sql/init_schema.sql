-- Create Schema for Raw Data
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.stock_prices (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    open NUMERIC(12, 4),
    high NUMERIC(12, 4),
    low NUMERIC(12, 4),
    close NUMERIC(12, 4),
    adj_close NUMERIC(12, 4),
    volume BIGINT,
    ingested_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_date_ticker_raw UNIQUE (date, ticker)
);

-- Create Schema for Mart Data
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.stock_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    close NUMERIC(12, 4),
    ma7 NUMERIC(12, 4),
    ma30 NUMERIC(12, 4),
    rsi14 NUMERIC(8, 4),
    daily_return NUMERIC(8, 4),
    volatility NUMERIC(8, 4),
    processed_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_date_ticker_mart UNIQUE (date, ticker)
);
