with source as (
    select * from {{ source('bronze', 'actual_generation') }}
),

split_psr as (
    select
        country_code,
        datetime_utc,
        generation_mw,
        ingested_at,
        -- Split "Fossil Hard coal_Actual Aggregated" into two columns
        split_part(psr_type, '_', 1) as fuel_type_name,
        split_part(psr_type, '_', 2) as business_type
    from source
)

select
    s.country_code,
    s.datetime_utc,
    s.generation_mw,
    s.fuel_type_name,
    s.business_type,
    p.psr_code,       -- The official ENTSO-E code (e.g., 'B18')
    s.ingested_at
from split_psr s
left join {{ ref('entsoe_psr_types') }} p
    on s.fuel_type_name = p.fuel_type_name
