select order_date, carrier_key, route_key,
  count(*) as total_shipments,
  sum(case when delivered_at is not null then 1 else 0 end) as delivered_shipments,
  sum(case when is_sla_breached is not null then 1 else 0 end) as sla_eligible_shipments,
  sum(case when is_sla_breached then 1 else 0 end) as sla_breached_shipments,
  100.0 * sum(case when is_sla_breached = false then 1 else 0 end)
    / nullif(sum(case when is_sla_breached is not null then 1 else 0 end), 0) as on_time_delivery_rate,
  avg(delivery_duration_hours) as avg_delivery_hours,
  sum(case when missing_order or missing_wms or missing_merchant or missing_carrier then 1 else 0 end) as incomplete_shipments
from {{ ref('fct_shipments') }}
group by order_date, carrier_key, route_key
