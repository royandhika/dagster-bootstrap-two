{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_dm',
        unique_key=['ticket_no'],
        group='inbound_awo',
        tags=['inbound']
    )
}}

with queue as (
    select * 
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_awo_cc_queue_intelix') }}
    where is_ticket = 2
        {% if is_incremental() %}
        and modified_time >= '{{ var('min_date') }}'
        and modified_time <= '{{ var('max_date') }}'
        {% endif %}
)
,queue_detail as (
    select * 
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_awo_cc_queue_detail_intelix') }} a
    where exists (
        select ticket_id 
        from queue b
        where a.ticket_id = b.ticket_id
    )
)
,master_category as (
    select * 
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_awo_cc_master_category_intelix') }}
)
,master_reference as (
    select * 
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_awo_cc_master_reference_intelix') }}
)
,master_customer as (
    select * 
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_awo_cc_master_customer_intelix') }} a
    where exists (
        select customer_id 
        from queue b
        where a.customer_id = b.customer_id
    )
)
select
    source = b.data_content 
    ,ticket_No = a.ticket_id 
    ,e1.unit
    ,d.customer_name
    ,contact = a.name 
    ,lov1 = e1.name 
    ,lov2 = e2.name 
    ,lov3 = e3.name
    ,lov4 = e4.name
    ,general_notes = c1.data_content 
    ,init_agent = c2.data_content 
    ,status = c3.data_content 
    ,last_status = a.queue_status 
    ,receive_Time = a.created_time 
    ,a.pickup_time
    ,a.pickup_by
    ,last_update_time = a.info_lastupdate_time 
    ,uploaddate = getdate()
from queue a
left join queue_detail b
	on a.ticket_id = b.ticket_id
	and b.field_name_id = 'source'
left join queue_detail c1
	on a.ticket_id = c1.ticket_id
	and c1.field_name_id = 'general-notes'
left join queue_detail c2
	on a.ticket_id = c2.ticket_id
	and c2.field_name_id = 'init-agent'
left join queue_detail c3
	on a.ticket_id = c3.ticket_id
	and c3.field_name_id = 'status'
left join queue_detail c4
	on a.ticket_id = c4.ticket_id
	and c4.field_name_id = 'category_ticket_1'	
left join queue_detail c5
	on a.ticket_id = c5.ticket_id
	and c5.field_name_id = 'category_ticket_2'		
left join queue_detail c6
	on a.ticket_id = c6.ticket_id
	and c6.field_name_id = 'category_ticket_3'		
left join master_customer d
	on a.customer_id = d.customer_id
left join master_category e1
	on a.first_category = e1.id
left join master_category e2
	on e2.id = c4.data_content
left join master_category e3
	on e3.id = c5.data_content
left join master_category e4
	on e4.id = c6.data_content
left join master_reference f
	on a.source_id = f.ref
	and f.id = 'CHANNEL_MODULE'
where b.ticket_id is not null