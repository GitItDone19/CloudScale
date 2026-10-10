select * from {{ ref('stg_wms_picks') }}
qualify row_number() over (
  partition by order_id
  order by dispatched_at desc, dt desc, _processed_at desc, wms_record_id desc
) = 1
