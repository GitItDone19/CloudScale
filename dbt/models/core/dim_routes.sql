with routes as (
  select distinct w.warehouse_id as origin_warehouse_id,
    o.customer_postal_code as destination_postal_zone,
    o.customer_country as destination_country
  from {{ ref('stg_postgres_orders') }} o
  left join {{ ref('int_latest_wms') }} w on o.order_id = w.order_id
  union distinct
  select w.warehouse_id, o.customer_postal_code, o.customer_country
  from {{ ref('int_shipment_events') }} e
  left join {{ ref('stg_postgres_orders') }} o on e.order_id = o.order_id
  left join {{ ref('int_latest_wms') }} w on e.order_id = w.order_id
  union distinct
  select null, null, null
)
select {{ stable_key(['origin_warehouse_id', 'destination_postal_zone', 'destination_country']) }} as route_key,
  origin_warehouse_id, destination_postal_zone, destination_country
from routes
