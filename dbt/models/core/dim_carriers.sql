select
  {{ stable_key(['carrier_id']) }} as carrier_key,
  carrier_id, carrier_name, service_level, agreed_sla_hours
from {{ ref('carrier_contracts') }}
union all
select {{ stable_key(['cast(null as ' ~ dbt.type_string() ~ ')']) }},
  null, 'Unknown carrier', 'UNKNOWN', null
