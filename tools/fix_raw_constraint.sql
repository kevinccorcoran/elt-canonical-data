-- Fix for Airflow Task Failure (InvalidColumnReference)
-- The Python ETL script uses "ON CONFLICT (ticker_date_id) DO NOTHING"
-- This requires a matching UNIQUE constraint on the target table.

ALTER TABLE raw.api_data_ingestion_massive
ADD CONSTRAINT unique_ticker_date_id UNIQUE (ticker_date_id);
