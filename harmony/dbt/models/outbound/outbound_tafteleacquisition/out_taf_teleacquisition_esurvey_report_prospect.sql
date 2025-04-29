{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_tafteleacquisition',
		tags=['3hourly']
	)
}}

with prospect as (
    select 
        tp.id
        ,tp.file_id
        ,tp.main_campaign
        ,tp.group_tele
        ,tp.start_date
        ,tp.end_date
        ,customername = tp.cust_name
		,prospectno = tp.customer_id
        ,tp.cust_nik
        ,tp.home_phone
        ,tp.mobile_phone
        ,tp.office_phone
        ,tp.other_phone1
        ,tp.other_phone2
        ,tp.other_phone3
        ,tp.other_phone4
        ,tp.other_phone5
        ,tp.other_phone6
        ,tp.other_phone7
        ,tp.note
        ,tp.admin_assigned_by
        ,tp.admin_assign_time
        ,tp.admin_assign_note
        ,tp.head_unit_id
        ,tp.head_unit_assign_time
        ,tp.head_unit_note
        ,tp.spv_id
        ,tp.spv_assign_time
        ,tp.spv_notes
        ,tp.agent_id
        ,tp.agent_pickup_time
        ,tp.last_phone_call
        ,tp.last_id_history_contact
        ,tp.first_category
        ,tp.last_category
        ,tp.last_campaign_agree
        ,tp.first_response_time
        ,tp.last_response_time
        ,tp.last_agent_notes
        ,tp.qa_id
        ,tp.qa_id_time_frame
        ,tp.qa_response_time
        ,tp.qa_notes
        ,tp.is_pickup
        ,tp.is_recycle_headunit
        ,tp.is_recycle_spv
        ,tp.is_predial
        ,tp.status
        ,tp.is_customer
        ,tp.created_by
        ,tp.created_time
        ,tp.last_info_time
        ,tp.appointmentDate
        ,tp.appointmentTime
        ,tp.call_attempt
        ,tp.duration
        ,tp.filter1
        ,tp.filter2
        ,tp.filter3
        ,tp.filter4
        ,tp.filter5
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_tms_prospect_intelix') }} tp
    where main_campaign in ('ESURVEY')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,campaign_result as (
    select 
        cr.prospect_id
        ,cr.campaign_id 
        ,cr.file_id 
        ,max(case when field_name_id in ('aplikasi') then data_content end) as aplikasi
        ,max(case when field_name_id in ('application_number') then data_content end) as application_number
        ,max(case when field_name_id in ('cabang') then data_content end) as cabang
        ,max(case when field_name_id in ('customer_name') then data_content end) as customer_name
        ,max(case when field_name_id in ('notes') then data_content end) as notes
        ,max(case when field_name_id in ('phone_number') then data_content end) as phone_number
        ,max(case when field_name_id in ('titik_survey') then data_content end) as titik_survey
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_tms_prospect_campaign_result_intelix') }} cr
    where exists (
    	select file_id 
    	from prospect p
    	where cr.file_id = p.file_id
	)
    group by prospect_id, campaign_id, file_id
)
,history_contact as (
    select 
        a.prospect_id
		,a.first_category as LOV1
		,a.last_category as LOV2
		,a.notes as Description
		,a.appointmentDate as [Appointment Date]
		,a.appointmentTime as [Appointment Time]
		,a.created_time
		,row_number()over(partition by a.prospect_id order by a.created_time desc) as Sort
		,[Attempt Call] = count(a.created_time) over(partition by a.prospect_id)
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_tms_prospect_history_contact_intelix') }} a
    where exists (
    	select file_id 
    	from prospect p
    	where a.file_id = p.file_id
	)
)
,map_lov as (
    select 
        a.prospect_id
		,b.[name] as [Connect Response]
		,c.[name] as [Contact Response]
		,a.[Description]
		,a.[Appointment Date]
		,a.[Appointment Time]
		,a.[created_time] as [Last Response Time]
		,a.[Sort]
		,a.[Attempt Call]
    from history_contact a
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_cc_master_category_intelix') }} b
        on a.LOV1 = b.id
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_cc_master_category_intelix') }} c
        on a.LOV2 = c.id
)
,time_frame as (
    select 
        a.id
        ,a.prospect_id
		,a.campaign_id
        ,a.response_status as [Campaign Result]
        ,a.response_sub_status as [Campaign Status]
        ,a.description as [Campaign Description]
        ,a.created_time as [Last Response Time]
        ,a.created_by as [Agent Id]
        ,row_number() over(partition by a.prospect_id, a.campaign_id order by a.created_time desc) as Sort
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_tms_prospect_time_frame_intelix') }} a
    where exists (
    	select file_id 
    	from prospect p
    	where a.file_id = p.file_id
	)
)
,final as (
    select
        a.id
		,a.file_id
		,a.main_campaign
		,a.customername
        ,a.prospectno
        ,a.home_phone
        ,a.mobile_phone
        ,a.office_phone
        ,a.other_phone1
        ,a.other_phone2
		,a.start_date
		,a.end_date
		,a.admin_assigned_by
		,a.admin_assign_time
		,a.head_unit_id
		,a.head_unit_assign_time
		,a.spv_id
		,a.spv_assign_time
		,a.agent_id
		,a.agent_pickup_time
		,a.created_time
		,a.last_phone_call
		,a.last_response_time
		,a.last_agent_notes
		,c.campaign_id
        ,c.aplikasi
        ,c.application_number
        ,c.cabang
        ,c.customer_name
        ,c.notes
        ,c.phone_number
        ,c.titik_survey
		,d.[Connect Response]
		,d.[Contact Response]
		,d.[Description]
		,e.[Campaign Result]
		,e.[Campaign Status]
		-- ,d.[Last Response Time]
		,e.[Agent Id]
		,d.[Appointment Date]
		,d.[Appointment Time]
		,d.[Attempt Call]
		,uploaddate = getdate()
	from prospect a
	left join campaign_result c 
        on a.id = c.prospect_id 
		and a.main_campaign = c.campaign_id
	left join map_lov d 
        on a.id = d.prospect_id 
		and d.Sort = 1
	left join time_frame e 
        on a.id = e.prospect_id 
		and c.campaign_id = e.campaign_id
		and e.Sort = 1
)
select * from final