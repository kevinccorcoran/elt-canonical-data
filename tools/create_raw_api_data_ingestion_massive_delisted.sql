-- Dedicated bars table for historically-delisted tickers.
-- Schema mirrors raw.api_data_ingestion_massive exactly (uses LIKE so
-- future schema changes to the live table can be replayed onto this
-- one) so downstream dbt models can UNION ALL between the two.
-- Populated by the Phase 2 backfill script
-- (src/datapipeline/ingestion/massive_backfill_delisted_aggregates.py).
-- Live active-ticker ingestion continues to write to
-- raw.api_data_ingestion_massive and is unaffected.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.api_data_ingestion_massive_delisted
    (LIKE raw.api_data_ingestion_massive INCLUDING DEFAULTS);

-- Idempotency: match the unique constraint the ETL relies on via
-- ON CONFLICT (ticker_date_id) DO NOTHING in
-- src/datapipeline/config/helpers.py. Use a distinct constraint name
-- so it can coexist with the live table's `unique_ticker_date_id`.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'unique_ticker_date_id_delisted'
    ) THEN
        ALTER TABLE raw.api_data_ingestion_massive_delisted
            ADD CONSTRAINT unique_ticker_date_id_delisted
            UNIQUE (ticker_date_id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_massive_delisted_ticker
    ON raw.api_data_ingestion_massive_delisted (ticker);

CREATE INDEX IF NOT EXISTS idx_massive_delisted_date
    ON raw.api_data_ingestion_massive_delisted (date);
