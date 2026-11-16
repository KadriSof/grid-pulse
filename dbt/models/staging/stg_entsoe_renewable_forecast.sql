with source as (
    select * from {{ source('bronze', 'renewable_forecast') }}
)

select
    s.country_code,
    z.area_short_code as bidding_zone,
    z.meaning as bidding_zone_name,
    z.timezone,
    s.datetime_utc,
    s.generation_mw as forecasted_generation_mw,
    s.ingested_at
from source s
left join {{ ref('entsoe_bidding_zones') }} z
    on s.country_code = z.country_code
