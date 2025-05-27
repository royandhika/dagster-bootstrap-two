{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_dm',
        inbound_trac_unique_key_intelix=['ticket_id'],
        group='inbound_trac',
        tags=['daily']
    )
}}

with cc_queue as (
    select 
        a.ticket_id 
        ,a.sap_code 
        ,sap_groupcode = b.group_code 
        -- ,first_category 
        ,lov1 = c.name 
        ,a.source_id
        ,f.sla_release
        ,f.sla_complete
        ,f.sla_close
        ,agent_create = g.name
        ,agent_complete = h.name
        ,task_title_notes = replace(replace(i.title_note, '\r', ''), '\n', '')
        ,status = d.code
        ,notif_created = a.created_time
        ,created_time = a.categorization_time 
        -- ,created_date = a.categorization_time 
        ,target_release = dateadd(hour, a.sla_group, i.target_start)
        ,target_complete = dateadd(hour, a.sla_backdesk, dateadd(hour, a.sla_group, i.target_start))
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_trac_cc_queue_intelix') }} a
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_trac_cc_master_sap_intelix') }} b 
        on a.sap_code = b.id
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_trac_cc_master_category_intelix') }} c 
        on a.first_category = c.id 
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_trac_cc_master_reference_intelix') }} d 
        on a.queue_status = d.ref 
        and d.id = 'TICKET_STATUS'
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_trac_cc_master_reference_intelix') }} e 
        on a.source_id = e.ref 
        and e.id = 'CHANNEL_MODULE'
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_trac_cc_master_category_intelix') }} f 
        on a.task_category = f.id
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_trac_cc_user_intelix') }} g 
        on a.pickup_by = g.id 
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_trac_cc_user_intelix') }} h 
        on a.last_response_by = h.id 
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_trac_cc_ticket_task_intelix') }} i 
        on a.ticket_id = i.ticket_id 
)
,cc_queue_detail as (
    select
        a.ticket_id
        ,lov2 = max(case when a.field_name_id = 'category_ticket_1' then c.name end) 
        ,lov3 = max(case when a.field_name_id = 'category_ticket_2' then c.name end) 
        ,lov4 = max(case when a.field_name_id = 'category_ticket_3' then c.name end) 
        ,lov5 = max(case when a.field_name_id = 'category_ticket_4' then c.name end) 
        ,lov6 = max(case when a.field_name_id = 'category_ticket_5' then c.name end) 
        ,lov7 = max(case when a.field_name_id = 'category_ticket_6' then c.name end) 
        ,email = max(case when a.field_name_id = 'txtReporterEmail' then a.data_content end) 
        ,cp1 = max(case when a.field_name_id = 'txtReporterName' then a.data_content end) 
        ,phone_cp1 = max(case when a.field_name_id = 'txtReporterPhone' then a.data_content end) 
        ,cp2 = max(case when a.field_name_id = 'txtContactName1' then a.data_content end) 
        ,phone_cp2 = max(case when a.field_name_id = 'txtContactPhone1' then a.data_content end) 
        ,incident_title = max(case when a.field_name_id = 'txtIncidentTitle' then a.data_content end) 
        ,cabang_name = max(case when a.field_name_id = 'optCabang' then c.name end) 
        ,cabang_code = max(case when a.field_name_id = 'optCabang' then c.code end) 
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_trac_cc_queue_detail_intelix') }} a
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'inbound_trac_cc_master_category_intelix') }} c 
        on a.data_content = convert(varchar, c.id) 
    where exists (
        select 1
        from cc_queue b
        where a.ticket_id = b.ticket_id
    )
    group by a.ticket_id
)
,final as (
    select 
        a.ticket_id 
        ,a.sap_code 
        ,a.sap_groupcode 
        ,a.lov1
        ,b.lov2
        ,b.lov3
        ,b.lov4
        ,b.lov5
        ,b.lov6
        ,b.lov7
        ,a.source_id
        ,a.sla_release
        ,a.sla_complete
        ,a.sla_close
        ,a.agent_create
        ,b.email
        ,b.cp1
        ,b.phone_cp1
        ,b.cp2
        ,b.phone_cp2
        ,b.incident_title
        ,a.agent_complete
        ,b.cabang_name
        ,b.cabang_code
        ,a.task_title_notes 
        ,a.status 
        ,a.notif_created
        ,a.created_time
        ,a.target_release
        ,a.target_complete 
        ,uploaddate = getdate()
    from cc_queue a 
    left join cc_queue_detail b 
        on a.ticket_id = b.ticket_id 
)
select * from final