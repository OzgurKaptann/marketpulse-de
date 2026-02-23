
    
    

select
    coin_id as unique_field,
    count(*) as n_records

from "marketpulse"."analytics_marts"."dim_coin"
where coin_id is not null
group by coin_id
having count(*) > 1


