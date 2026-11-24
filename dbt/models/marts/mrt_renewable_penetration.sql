{{
    config(
        materialized='table',
        description='Hourly renewable generation as a percentage of total load.'
    )
}}

-- Renewable PSR codes:
--   Wind + Solar: B16 (Solar), B18 (Wind Offshore), B19 (Wind Onshore)
--   All Renewables: + B09 (Geothermal), B11 (Hydro RoR), B12 (Hydro Reservoir), B15 (Other renewable)

with hourly_generation as (
    select
        country_code,
        bidding_zone,
        bidding_zone_name,
        date_trunc('hour', datetime_utc) as hour_utc,
        sum(case when psr_code in ('B16', 'B18', 'B19')
                 then generation_mw else 0 end)                  as wind_solar_mw,
        sum(case when psr_code in ('B09', 'B11', 'B12', 'B15', 'B16', 'B18', 'B19')
                 then generation_mw else 0 end)                  as all_renewables_mw,
        sum(generation_mw)                                       as total_generation_mw
    from {{ ref('stg_entsoe_actual_generation') }}
    where business_type = 'Actual Aggregated'
    group by country_code, bidding_zone, bidding_zone_name, date_trunc('hour', datetime_utc)
),

hourly_load as (
    select
        country_code,
        date_trunc('hour', datetime_utc) as hour_utc,
        avg(load_mw) as load_mw
    from {{ ref('stg_entsoe_actual_load') }}
    group by country_code, date_trunc('hour', datetime_utc)
)

select
    g.country_code,
    g.bidding_zone,
    g.bidding_zone_name,
    g.hour_utc,
    g.wind_solar_mw,
    g.all_renewables_mw,
    g.total_generation_mw,
    l.load_mw,
    case when l.load_mw > 0
         then round((g.wind_solar_mw     / l.load_mw * 100)::numeric, 2)
         else null end as wind_solar_penetration_pct,
    case when l.load_mw > 0
         then round((g.all_renewables_mw / l.load_mw * 100)::numeric, 2)
         else null end as all_renewables_penetration_pct
from hourly_generation g
left join hourly_load l
    on  g.country_code = l.country_code
    and g.hour_utc     = l.hour_utc
order by g.country_code, g.hour_utc
