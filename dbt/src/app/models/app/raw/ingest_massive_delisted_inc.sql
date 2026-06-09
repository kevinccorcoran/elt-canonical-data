{{
  config(
    materialized = 'incremental',
    unique_key = 'ticker_date_id',
    on_schema_change = 'sync_all_columns',
    post_hook = [
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_date_desc_ticker
        ON {{ this }} (date DESC, ticker);
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker
        ON {{ this }} (ticker);
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_category
        ON {{ this }} (delisting_category);
      """
    ]
  )
}}

-- Delisted-ticker bars enriched with metadata + a backtest-inclusion
-- flag. include_for_backtest = TRUE only when the delisting reason is
-- one of the survivorship-bias-relevant categories (real failures or
-- real M&A exits). SPACs and shells are excluded — not investable as
-- normal equity in a backtest.
--
-- Incremental on processed_at: new monthly delisted backfill batches
-- get appended without rebuilding the whole table. When the Polygon
-- plan window expands and historical bars get added, those new rows
-- arrive with a fresh processed_at and get picked up automatically.
--
-- After running the classifier (tools/classify_delisting_categories.sql)
-- to refresh delisting_category on existing tickers, do a full refresh
-- to propagate the new categories:
--     dbt run --full-refresh --select ingest_massive_delisted_inc

SELECT
    b.*,
    m.name,
    m.delisting_category,
    m.delisted_utc,
    m.list_date,
    CASE
        WHEN m.delisting_category IN ('BANKRUPTCY', 'M&A_OR_PRIVATE', 'DISTRESSED')
            THEN TRUE
        ELSE FALSE
    END AS include_for_backtest
FROM {{ source('raw', 'api_data_ingestion_massive_delisted') }} b
JOIN {{ source('raw', 'ticker_metadata') }} m USING (ticker)

{% if is_incremental() %}
    WHERE b.processed_at > (SELECT COALESCE(MAX(processed_at), '1970-01-01'::timestamp) FROM {{ this }})
{% endif %}
