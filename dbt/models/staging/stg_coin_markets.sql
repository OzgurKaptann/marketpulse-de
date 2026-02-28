select
    ingested_at as as_of_ts,

    coin_id,
    lower(symbol) as symbol,
    name,

    current_price::numeric as current_price,
    market_cap::numeric as market_cap,
    total_volume::numeric as total_volume,
    market_cap_rank::int as market_cap_rank,

    last_updated::timestamptz as last_updated,
    source,
    vs_currency,
    page::int as page,
    per_page::int as per_page

from raw.coin_markets