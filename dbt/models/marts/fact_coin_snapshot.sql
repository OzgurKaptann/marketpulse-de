select
  as_of_ts,
  coin_id,
  current_price,
  market_cap,
  total_volume,
  market_cap_rank,
  price_change_percentage_24h
from {{ ref('stg_coin_markets') }}
