{{ incremental_config('order_id', 'dt', ['order_id']) }}
with candidates as (
  select order_id, merchant_id, customer_postal_code, customer_country, currency, declared_value, order_timestamp, _source_file, _batch_date, _processed_at, dt from {{ source('silver', 'postgres_orders') }}
  where {{ source_window() }}
  {% if is_incremental() %}
  union all
  select order_id, merchant_id, customer_postal_code, customer_country, currency, declared_value, order_timestamp, _source_file, _batch_date, _processed_at, dt from {{ this }}
  {% endif %}
)
select * from candidates
qualify row_number() over (partition by order_id order by dt desc, _processed_at desc, _source_file desc) = 1
