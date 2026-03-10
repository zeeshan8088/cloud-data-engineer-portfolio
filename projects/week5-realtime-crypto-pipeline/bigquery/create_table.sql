-- ============================================================
-- Week 5 - BigQuery Streaming Table
-- 
-- This is the RAW layer of our warehouse.
-- Every message from Pub/Sub lands here exactly as-is.
-- Partitioned by date so queries are fast and cheap.
-- ============================================================

CREATE TABLE IF NOT EXISTS `intricate-ward-459513-e1.crypto_streaming.raw_crypto_prices`
(
    coin                STRING    NOT NULL,   -- bitcoin / ethereum / solana
    price_usd           FLOAT64   NOT NULL,   -- current price in USD
    volume_24h          FLOAT64,              -- 24 hour trading volume
    change_24h_pct      FLOAT64,              -- % price change last 24h
    ingested_at         TIMESTAMP NOT NULL,   -- when our pipeline captured it
    source              STRING,               -- always "coingecko_api"
    pipeline_version    STRING                -- track which version ran
)
PARTITION BY DATE(ingested_at)   -- one partition per day = cheaper queries
OPTIONS (
    description = "Raw real-time crypto prices from CoinGecko via Pub/Sub streaming",
    partition_expiration_days = 90   -- auto-delete data older than 90 days
);