with source as (
    select * from {{ source('bronze', 'physical_cross_border_flows') }}
)

select
    s.in_domain,
    z_in.meaning as in_zone_name,
    s.out_domain,
    z_out.meaning as out_zone_name,
    s.datetime_utc,
    s.flow_mw,
    s.ingested_at
from source s
left join {{ ref('entsoe_bidding_zones') }} z_in
    on s.in_domain = z_in.country_code
left join {{ ref('entsoe_bidding_zones') }} z_out
    on s.out_domain = z_out.country_code
