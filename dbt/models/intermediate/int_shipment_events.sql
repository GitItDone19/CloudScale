select
  {{ stable_key(['carrier_id', 'tracking_number']) }} as shipment_key,
  carrier_id, tracking_number, order_id,
  min(case when event_type = 'DELIVERED' then scan_timestamp end) over (
    partition by carrier_id, tracking_number
  ) as delivered_at,
  event_type as current_status,
  scan_timestamp as latest_scan_at,
  received_at as latest_received_at
from {{ ref('stg_carrier_events') }}
qualify row_number() over (
  partition by carrier_id, tracking_number
  order by scan_timestamp desc, received_at desc, event_id desc
) = 1
