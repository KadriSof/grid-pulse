{{
    config(
        materialized='table',
        description='Hourly generation vs load balance per bidding zone. Positive = surplus, negative = deficit.'
    )
}}

-- Step 1: Total generation per 15-min timestamp (sum across all fuel types)
with generation_15min as (
    select
        country_code,
        bidding_zone,
        bidding_zone_name,
        datetime_utc,
        sum(generation_mw) as total_generation_mw
    from {{ ref('stg_entsoe_actual_generation') }}
    where business_type = 'Actual Aggregated'
    group by country_code, bidding_zone, bidding_zone_name, datetime_utc
),

-- Step 2: Aggregate generation to hourly (average of the 4x 15-min readings)
generation_hourly as (
    select
        country_code,
        bidding_zone,
        bidding_zone_name,
        date_trunc('hour', datetime_utc) as hour_utc,
        avg(total_generation_mw) as generation_mw
    from generation_15min
    group by country_code, bidding_zone, bidding_zone_name, date_trunc('hour', datetime_utc)
),

-- Step 3: Aggregate load to hourly
load_hourly as (
    select
        country_code,
        bidding_zone,
        bidding_zone_name,
        date_trunc('hour', datetime_utc) as hour_utc,
        avg(load_mw) as load_mw
    from {{ ref('stg_entsoe_actual_load') }}
    group by country_code, bidding_zone, bidding_zone_name, date_trunc('hour', datetime_utc)
)

-- Step 4: Join and calculate the balance
select
    coalesce(g.country_code, l.country_code)           as country_code,
    coalesce(g.bidding_zone, l.bidding_zone)           as bidding_zone,
    coalesce(g.bidding_zone_name, l.bidding_zone_name) as bidding_zone_name,
    coalesce(g.hour_utc, l.hour_utc)                   as hour_utc,
    g.generation_mw,
    l.load_mw,
    g.generation_mw - l.load_mw                        as net_balance_mw,
    case
        when l.load_mw > 0
        then round(((g.generation_mw - l.load_mw) / l.load_mw * 100)::numeric, 2)
        else null
    end                                                as balance_pct_of_load,
    case
        when g.generation_mw > l.load_mw then 'surplus'
        when g.generation_mw < l.load_mw then 'deficit'
        else 'balanced'
    end                                                as balance_status
from generation_hourly g
full outer join load_hourly l
    on  g.country_code = l.country_code
    and g.hour_utc     = l.hour_utc
order by country_code, hour_utc
