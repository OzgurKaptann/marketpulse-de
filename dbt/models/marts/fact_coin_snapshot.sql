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
from base