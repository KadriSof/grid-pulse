-- Electricity load can never be negative.
-- Fails if any negative load values are found.
select
    country_code,
    datetime_utc,
    load_mw
from {{ ref('stg_entsoe_actual_load') }}
where load_mw < 0
