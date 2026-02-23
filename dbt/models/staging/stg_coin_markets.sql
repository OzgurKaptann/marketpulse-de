select
    as_of_ts,
    coin_id,
    lower(symbol) as symbol,
    name,
    current_price::numeric as current_price,
    market_cap::numeric as market_cap,
    total_volume::numeric as total_volume,
    market_cap_rank::int as market_cap_rank,
    price_change_percentage_24h::numeric as price_change_percentage_24h,
    source
from raw.coin_markets
