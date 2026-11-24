-- ENTSO-E day-ahead prices historically range from ~-500 to ~4000 EUR/MWh.
-- Values outside this range indicate a data quality issue.
select
    country_code,
    datetime_utc,
    price_eur_per_mwh
from {{ ref('stg_entsoe_day_ahead_prices') }}
where price_eur_per_mwh < -500
   or price_eur_per_mwh > 4000
