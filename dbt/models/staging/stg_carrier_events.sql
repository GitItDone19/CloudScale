{{ incremental_config('event_id', 'dt', ['event_id']) }}
with candidates as (
  select event_id, tracking_number, order_id, carrier_id, event_type, location_code, exception_reason, currency, scan_timestamp, received_at, customs_fee, _source_file, _batch_date, _processed_at, dt from {{ source('silver', 'carrier_events') }}
  where {{ source_window() }}
  {% if is_incremental() %}
  union all
  select event_id, tracking_number, order_id, carrier_id, event_type, location_code, exception_reason, currency, scan_timestamp, received_at, customs_fee, _source_file, _batch_date, _processed_at, dt from {{ this }}
  {% endif %}
)
select * from candidates
qualify row_number() over (partition by event_id order by received_at desc, dt desc, _processed_at desc, _source_file desc) = 1
