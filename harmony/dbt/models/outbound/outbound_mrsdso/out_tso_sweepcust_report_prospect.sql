{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_mrsdso',
		tags=['outbound']
	)
}}

with prospect as (
    select 
        tp.id
		,tp.file_id
		,tp.main_campaign
		,tp.start_date
		,tp.end_date
		,tp.admin_assigned_by
		,tp.admin_assign_time
		,tp.head_unit_id
		,tp.head_unit_assign_time
		,tp.spv_id
		,tp.spv_assign_time
		,tp.agent_id
		,tp.agent_pickup_time
		,tp.created_time
		,tp.last_phone_call
		,tp.last_response_time
		,tp.last_agent_notes
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_intelix') }} tp
    where main_campaign = 'TSO'
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
        ,max(case when field_name = 'CustomerNo' then data_content end) as [CustomerNo]
        ,max(case when field_name = 'CustID' then data_content end) as [CustID]
        ,max(case when field_name = 'CustomerName' then data_content end) as [CustomerName]
        ,max(case when field_name = 'MSISDN' then data_content end) as [MSISDN]
        ,max(case when field_name = 'TelephoneNo' then data_content end) as [TelephoneNo]
        ,max(case when field_name = 'Year' then data_content end) as [Year]
        ,max(case when field_name = 'model' then data_content end) as [model]
        ,max(case when field_name = 'LastUnitServiceTransaction' then data_content end) as [LastUnitServiceTransaction]
        ,max(case when field_name = 'LastUnitServicePoliceRegNo' then data_content end) as [LastUnitServicePoliceRegNo]
        ,max(case when field_name = 'TrxTime' then data_content end) as [TrxTime]
        ,max(case when field_name = 'SweepingCategory' then data_content end) as [SweepingCategory]
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_detail_intelix') }} pd
    where exists (
    	select file_id 
    	from prospect p
    	where pd.file_id = p.file_id
	)
    group by prospect_id
)
,campaign_result as (
    select 
        prospect_id 
        ,campaign_id 
        ,file_id
        ,max(case when field_name = 'Q1 Agent' then data_content end) as [Q1 Agent]
        ,max(case when field_name = 'Q2 Agent' then data_content end) as [Q2 Agent]
        ,max(case when field_name = 'Q3 Agent' then data_content end) as [Q3 Agent]
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_campaign_result_intelix') }} cr
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
		-- ,a.notes as Description
		,a.appointmentDate as [Appointment Date]
		,a.appointmentTime as [Appointment Time]
		,a.created_time
		,row_number() over(partition by a.prospect_id order by a.created_time desc) as Sort
		,[Attempt Call] = count(a.created_time) over(partition by a.prospect_id)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_history_contact_intelix') }} a
    where a.file_id in (select file_id from prospect)
)
,map_lov as (
    select 
        a.prospect_id
		,b.[name] as [Connect Response]
		,c.[name] as [Contact Response]
		-- ,a.[Description]
		,a.[Appointment Date]
		,a.[Appointment Time]
		,a.[created_time] as [Last Response Time]
		,a.[Sort]
		,a.[Attempt Call]
    from history_contact a
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_cc_master_category_intelix') }} b
        ON a.LOV1 = b.id
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_cc_master_category_intelix') }} c
        ON a.LOV2 = c.id
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_time_frame_intelix') }} a
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
        ,b.[CustomerNo] 
        ,b.[CustID] 
        ,b.[CustomerName] 
        ,b.[MSISDN] 
        ,b.[TelephoneNo] 
        ,b.[Year] 
        ,b.[model] 
        ,b.[LastUnitServiceTransaction] 
        ,b.[LastUnitServicePoliceRegNo] 
        ,b.[TrxTime] 
        ,b.[SweepingCategory] 
		,c.campaign_id
        ,c.[Q1 Agent]
        ,c.[Q2 Agent]
        ,c.[Q3 Agent]
		,d.[Connect Response]
		,d.[Contact Response]
		-- ,d.[Description]
		,e.[Campaign Result]
		,e.[Campaign Status]
        ,e.[Campaign Description]
		,d.[Last Response Time]
		,e.[Agent Id]
		,d.[Attempt Call]
		,d.[Appointment Date]
		,d.[Appointment Time]
		,uploaddate = getdate()
	from prospect a
	left join prospect_detail b 
        on a.id = b.prospect_id
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