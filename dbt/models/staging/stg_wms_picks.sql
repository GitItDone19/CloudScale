{{ incremental_config('wms_record_id', 'dt', ['wms_record_id']) }}
with candidates as (
  select wms_record_id, order_id, warehouse_id, box_type, parcel_weight_kg, picked_at, packed_at, dispatched_at, _source_file, _batch_date, _processed_at, dt from {{ source('silver', 'wms_picks') }}
  where {{ source_window() }}
  {% if is_incremental() %}
  union all
  select wms_record_id, order_id, warehouse_id, box_type, parcel_weight_kg, picked_at, packed_at, dispatched_at, _source_file, _batch_date, _processed_at, dt from {{ this }}
  {% endif %}
)
select * from candidates
qualify row_number() over (partition by wms_record_id order by dt desc, _processed_at desc, _source_file desc) = 1
