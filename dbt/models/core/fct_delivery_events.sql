{{ incremental_config('event_key', 'event_date', ['shipment_key', 'event_type']) }}
select {{ stable_key(['event_id']) }} as event_key, event_id,
  {{ stable_key(['carrier_id', 'tracking_number']) }} as shipment_key,
  cast(scan_timestamp as date) as event_date,
  {{ date_key('scan_timestamp') }} as event_date_key,
  scan_timestamp as event_timestamp, received_at,
  event_type, location_code, exception_reason, dt as ingestion_date
from {{ ref('stg_carrier_events') }}
