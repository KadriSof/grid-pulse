{{
    config(
        materialized='table',
        description='Daily generation mix per bidding zone and fuel type, with share of total.'
    )
}}

with daily_fuel as (
    select
        country_code,
        bidding_zone,
        bidding_zone_name,
        date_trunc('day', datetime_utc)::date  as date_utc,
        fuel_type_name,
        psr_code,
        avg(generation_mw)                     as avg_generation_mw,
        sum(generation_mw) * 0.25              as estimated_daily_mwh
    from {{ ref('stg_entsoe_actual_generation') }}
    where business_type = 'Actual Aggregated'
    group by
        country_code, bidding_zone, bidding_zone_name,
        date_trunc('day', datetime_utc)::date,
        fuel_type_name, psr_code
),

daily_total as (
    select
        country_code,
        date_utc,
        sum(avg_generation_mw) as total_avg_generation_mw
    from daily_fuel
    group by country_code, date_utc
)

select
    f.country_code,
    f.bidding_zone,
    f.bidding_zone_name,
    f.date_utc,
    f.fuel_type_name,
    f.psr_code,
    f.avg_generation_mw,
    f.estimated_daily_mwh,
    case
        when t.total_avg_generation_mw > 0
        then round((f.avg_generation_mw / t.total_avg_generation_mw * 100)::numeric, 2)
        else 0
    end as pct_of_total_generation
from daily_fuel f
left join daily_total t
    on  f.country_code = t.country_code
    and f.date_utc     = t.date_utc
order by f.country_code, f.date_utc, f.avg_generation_mw desc
