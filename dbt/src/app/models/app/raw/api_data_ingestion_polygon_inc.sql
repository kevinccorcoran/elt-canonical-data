{{ config(
    materialized='incremental',
    schema='raw',
    unique_key='ticker_date_id',
    on_schema_change='sync_all_columns',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS uq_api_backup_ticker_date_id ON {{ this }} (ticker_date_id)",
      "CREATE INDEX IF NOT EXISTS idx_api_backup_ticker_date ON {{ this }} (ticker, \"date\")",
      "ANALYZE {{ this }}"
    ]
) }}

with src as (
  select
      "date",
      ticker,
      "open",
      high,
      low,
      "close",
      volume,
      dividends,
      stock_splits,
      processed_at,
      adj_close,
      capital_gains,
      ticker_date_id
  from {{ source('raw', 'api_data_ingestion_massive') }}
)

select
    s."date",
    s.ticker,
    s."open",
    s.high,
    s.low,
    s."close",
    s.volume,
    s.dividends,
    s.stock_splits,
    (now() at time zone 'utc')::timestamp as processed_at,
    s.adj_close,
    s.capital_gains,
    s.ticker_date_id
from src s

{% if is_incremental() %}
where not exists (
  select 1
  from {{ this }} b
  where b.ticker_date_id = s.ticker_date_id
)
{% endif %}
