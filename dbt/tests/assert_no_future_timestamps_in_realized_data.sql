-- Realized (actual) data should never have future timestamps.
-- Note: day_ahead_prices is intentionally EXEMPT because it is
-- forward-looking by design (published today for tomorrow's delivery).
select datetime_utc, 'actual_load' as source_table
from {{ ref('stg_entsoe_actual_load') }}
where datetime_utc > current_timestamp

union all

select datetime_utc, 'actual_generation' as source_table
from {{ ref('stg_entsoe_actual_generation') }}
where datetime_utc > current_timestamp
