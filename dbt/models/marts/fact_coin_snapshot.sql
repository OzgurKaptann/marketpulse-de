{{ config(
    materialized='incremental',
    unique_key="coin_id || '-' || as_of_ts::text || '-' || vs_currency || '-' || page::text || '-' || per_page::text",
    on_schema_change='sync_all_columns'
) }}

with base as (
    select
        as_of_ts,
        coin_id,
        current_price,
        market_cap,
        total_volume,
        market_cap_rank,
        last_updated,
        source,
        vs_currency,
        page,
        per_page
    from {{ ref('stg_coin_markets') }}
)

select *
from base

{% if is_incremental() %}
where as_of_ts > (select coalesce(max(as_of_ts), '1970-01-01'::timestamptz) from {{ this }})
{% endif %}
