with dates as (
  select cast(order_timestamp as date) as full_date from {{ ref('stg_postgres_orders') }}
  union distinct
  select cast(scan_timestamp as date) from {{ ref('stg_carrier_events') }}
  union distinct
  select cast(dispatched_at as date) from {{ ref('stg_wms_picks') }}
)
select cast(extract(year from full_date) * 10000 + extract(month from full_date) * 100
    + extract(day from full_date) as {{ dbt.type_int() }}) as date_key,
  full_date, extract(year from full_date) as year,
  extract(quarter from full_date) as quarter, extract(month from full_date) as month,
  extract(day from full_date) as day_of_month
from dates where full_date is not null
