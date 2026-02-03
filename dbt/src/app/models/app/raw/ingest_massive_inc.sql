{{ 
  config(
    materialized = 'incremental',
    unique_key = 'ticker_date_id'
  ) 
}}

with source_data as (

    select
        ticker,
        date,
        processed_at,
        ticker || '_' || date as ticker_date_id,
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        dividends,
        stock_splits
    from {{ source('raw', 'api_data_ingestion_massive') }}

)

select *
from source_data

{% if is_incremental() %}
where processed_at >
      (
        select coalesce(
            max(processed_at),
            timestamp '1900-01-01'
        )
        from {{ this }}
      )
{% endif %}
