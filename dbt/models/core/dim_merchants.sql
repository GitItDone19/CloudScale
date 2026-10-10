select
  {{ stable_key(['merchant_id']) }} as merchant_key,
  merchant_id, merchant_name, merchant_tier, country_code
from {{ ref('stg_postgres_merchants') }}
union all
select {{ stable_key(['cast(null as ' ~ dbt.type_string() ~ ')']) }},
  null, 'Unknown merchant', 'UNKNOWN', null
