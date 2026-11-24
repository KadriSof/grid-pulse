-- Generation output can never be negative.
-- Fails if any negative generation values are found.
select
    country_code,
    datetime_utc,
    generation_mw
from {{ ref('stg_entsoe_actual_generation') }}
where generation_mw < 0
