select * from {{ ref('fct_shipments') }}
where delivery_duration_hours < 0
   or (is_sla_breached is not null and (delivery_duration_hours is null or agreed_sla_hours is null))
