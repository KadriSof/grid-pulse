{{
    config(
        materialized='table',
        description='Daily net cross-border flow position per zone. Positive = net exporter.'
    )
}}

-- Unpivot flows so each zone appears once per interconnection record.
-- flow_mw direction: out_domain → in_domain (ENTSO-E convention).
with zone_flows as (
    -- Exporter side
    select
        out_domain       as country_code,
        out_zone_name    as bidding_zone_name,
        datetime_utc,
        flow_mw          as export_mw,
        0                as import_mw
    from {{ ref('stg_entsoe_physical_cross_border_flows') }}

    union all

    -- Importer side
    select
        in_domain        as country_code,
        in_zone_name     as bidding_zone_name,
        datetime_utc,
        0                as export_mw,
        flow_mw          as import_mw
    from {{ ref('stg_entsoe_physical_cross_border_flows') }}
),

daily_net as (
    select
        country_code,
        bidding_zone_name,
        date_trunc('day', datetime_utc)::date as date_utc,
        avg(export_mw)                        as avg_export_mw,
        avg(import_mw)                        as avg_import_mw,
        avg(export_mw) - avg(import_mw)       as net_flow_mw
    from zone_flows
    group by country_code, bidding_zone_name, date_trunc('day', datetime_utc)::date
)

select
    country_code,
    bidding_zone_name,
    date_utc,
    round(avg_export_mw::numeric, 2) as avg_export_mw,
    round(avg_import_mw::numeric, 2) as avg_import_mw,
    round(net_flow_mw::numeric, 2)   as net_flow_mw,
    case
        when net_flow_mw > 0 then 'net_exporter'
        when net_flow_mw < 0 then 'net_importer'
        else 'balanced'
    end as net_position
from daily_net
order by country_code, date_utc
