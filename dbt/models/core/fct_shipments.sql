{{ incremental_config('shipment_key', 'order_date', ['carrier_key', 'merchant_key']) }}
with joined as (
  select e.*, o.order_timestamp, cast(o.order_timestamp as date) as order_date,
    coalesce(m.merchant_key, {{ stable_key(['cast(null as ' ~ dbt.type_string() ~ ')']) }}) as merchant_key,
    coalesce(c.carrier_key, {{ stable_key(['cast(null as ' ~ dbt.type_string() ~ ')']) }}) as carrier_key,
    {{ stable_key(['w.warehouse_id', 'o.customer_postal_code', 'o.customer_country']) }} as route_key,
    w.dispatched_at, w.parcel_weight_kg, o.declared_value, o.currency,
    c.agreed_sla_hours,
    o.order_id is null as missing_order,
    w.order_id is null as missing_wms,
    m.merchant_id is null as missing_merchant,
    c.carrier_id is null as missing_carrier,
    case when e.delivered_at >= w.dispatched_at then
      {{ dbt.datediff('w.dispatched_at', 'e.delivered_at', 'second') }} / 3600.0
    end as delivery_duration_hours
  from {{ ref('int_shipment_events') }} e
  left join {{ ref('stg_postgres_orders') }} o on e.order_id = o.order_id
  left join {{ ref('int_latest_wms') }} w on e.order_id = w.order_id
  left join {{ ref('dim_merchants') }} m on o.merchant_id = m.merchant_id
  left join {{ ref('dim_carriers') }} c on e.carrier_id = c.carrier_id
)
select *, cast(delivered_at as date) as delivery_date,
  {{ date_key('order_timestamp') }} as order_date_key,
  {{ date_key('delivered_at') }} as delivery_date_key,
  case when delivered_at is not null and delivery_duration_hours is not null and agreed_sla_hours is not null
    then delivery_duration_hours > agreed_sla_hours end as is_sla_breached,
  delivered_at < dispatched_at or dispatched_at < order_timestamp as invalid_time_sequence,
  cast('{{ run_date() }}' as date) as modeled_through_date
from joined
