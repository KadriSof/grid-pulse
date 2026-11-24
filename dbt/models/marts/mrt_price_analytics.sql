{{
    config(
        materialized='table',
        description='Daily day-ahead price statistics per bidding zone. Note: negative prices are valid in ENTSO-E markets and are intentionally retained.'
    )
}}


select
    country_code,
    bidding_zone,
    bidding_zone_name,
    date_trunc('day', datetime_utc)::date       as date_utc,
    round(avg(price_eur_per_mwh)::numeric, 2)   as avg_price_eur_mwh,
    round(min(price_eur_per_mwh)::numeric, 2)   as min_price_eur_mwh,
    round(max(price_eur_per_mwh)::numeric, 2)   as max_price_eur_mwh,
    round((max(price_eur_per_mwh) - min(price_eur_per_mwh))::numeric, 2) as price_spread_eur_mwh,
    round(stddev(price_eur_per_mwh)::numeric, 2) as price_volatility_eur_mwh,
    count(*)                                    as price_intervals
from {{ ref('stg_entsoe_day_ahead_prices') }}
group by country_code, bidding_zone, bidding_zone_name, date_trunc('day', datetime_utc)::date
order by country_code, date_utc
