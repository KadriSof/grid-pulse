with source as (
    select * from {{ source('bronze', 'actual_generation') }}
),

split_psr as (
    select
        country_code,
        datetime_utc,
        generation_mw,
        ingested_at,
        -- Split "Fossil Hard coal_Actual Consumption" into two columns
        split_part(psr_type, '_', 1) as fuel_type_name,
        split_part(psr_type, '_', 2) as business_type
    from source
)

select
    s.country_code,
    z.area_short_code as bidding_zone,
    z.meaning as bidding_zone_name,
    z.timezone,
    s.datetime_utc,
    s.generation_mw,
    s.fuel_type_name,
    s.business_type,
    p.psr_code,       -- The official ENTSO-E code (e.g., 'B05' for Hard Coal)
    s.ingested_at
from split_psr s
left join {{ ref('entsoe_psr_types') }} p
    on s.fuel_type_name = p.fuel_type_name
left join {{ ref('entsoe_bidding_zones') }} z
    on s.country_code = z.country_code
