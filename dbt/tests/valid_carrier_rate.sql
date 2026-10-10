select * from {{ ref('mart_carrier_performance_daily') }}
where on_time_delivery_rate < 0 or on_time_delivery_rate > 100
   or sla_breached_shipments > sla_eligible_shipments
