{{ incremental_config('merchant_id', 'dt', ['merchant_id']) }}
with candidates as (
  select merchant_id, merchant_name, merchant_tier, country_code, created_at, _source_file, _batch_date, _processed_at, dt from {{ source('silver', 'postgres_merchants') }}
  where {{ source_window() }}
  {% if is_incremental() %}
  union all
  select merchant_id, merchant_name, merchant_tier, country_code, created_at, _source_file, _batch_date, _processed_at, dt from {{ this }}
  {% endif %}
)
select * from candidates
qualify row_number() over (partition by merchant_id order by dt desc, _processed_at desc, _source_file desc) = 1
