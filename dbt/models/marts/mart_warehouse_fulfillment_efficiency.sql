select warehouse_id, cast(dispatched_at as date) as dispatch_date,
  count(*) as dispatched_orders,
  avg({{ dbt.datediff('picked_at', 'packed_at', 'second') }} / 60.0) as avg_pick_to_pack_minutes,
  avg({{ dbt.datediff('packed_at', 'dispatched_at', 'second') }} / 60.0) as avg_pack_to_dispatch_minutes
from {{ ref('int_latest_wms') }}
group by warehouse_id, cast(dispatched_at as date)
