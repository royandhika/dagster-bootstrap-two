{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_mrsdso',
		tags=['12hourly']
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
    where main_campaign = 'DS03'
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
        ,max(case when field_name = 'colordesc' then data_content end) [colordesc]
        ,max(case when field_name = 'customername' then data_content end) [customername]
        ,max(case when field_name = 'customerno' then data_content end) [customerno]
        ,max(case when field_name = 'customertitle' then data_content end) [customertitle]
        ,max(case when field_name = 'dealercode' then data_content end) [dealercode]
        ,max(case when field_name = 'dealerdesc' then data_content end) [dealerdesc]
        ,max(case when field_name = 'equipmentno' then data_content end) [equipmentno]
        ,max(case when field_name = 'flagresponse' then data_content end) [flagresponse]
        ,max(case when field_name = 'flagstatus' then data_content end) [flagstatus]
        ,max(case when field_name = 'gidate' then data_content end) [gidate]
        ,max(case when field_name = 'model' then data_content end) [model]
        ,max(case when field_name = 'modelyear' then data_content end) [modelyear]
        ,max(case when field_name = 'msisdn' then data_content end) [msisdn]
        ,max(case when field_name = 'Parameter1' then data_content end) [Parameter1]
        ,max(case when field_name = 'priority' then data_content end) [priority]
        ,max(case when field_name = 'proddesc' then data_content end) [proddesc]
        ,max(case when field_name = 'salesmanname' then data_content end) [salesmanname]
        ,max(case when field_name = 'senddate' then data_content end) [senddate]
        ,max(case when field_name = 'supplydate' then data_content end) [supplydate]
        ,max(case when field_name = 'supplydatewa' then data_content end) [supplydatewa]
        ,max(case when field_name = 'responsedate' then data_content end) [responsedate]
        ,max(case when field_name = 'customerresponse' then data_content end) [customerresponse]
        ,max(case when field_name = 'transmission' then data_content end) [transmission]
        ,max(case when field_name = 'uploaddate' then data_content end) [uploaddate]
        ,max(case when field_name = 'flagdrop' then data_content end) [flagdrop]
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
        ,max(case when field_name = 'g1' then data_content end) [g1]
        ,max(case when field_name = 'g2' then data_content end) [g2]
        ,max(case when field_name = 'Q1 Validasi Nama Nomor Telp' then data_content end) [q1_validasi_nama_nomor_telp]
        ,max(case when field_name = 'Q2 Validasi Jenis Kendaraan' then data_content end) [q2_validasi_jenis_kendaraan]
        ,max(case when field_name = 'Q3 Validasi Outlet Pembelian' then data_content end) [q3_validasi_outlet_pembelian]
        ,max(case when field_name = 'q3a' then data_content end) [q3a]
        ,max(case when field_name = 'Q4 Validasi Tanggal DO' then data_content end) [q4_validasi_tanggal_do]
        ,max(case when field_name = 'q4a' then data_content end) [q4a]
        ,max(case when field_name = 'Q5 Validasi Nama Wiraniaga' then data_content end) [q5_validasi_nama_wiraniaga]
        ,max(case when field_name = 'q5a' then data_content end) [q5a]
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
        ,b.[colordesc]
        ,b.[customername]
        ,b.[customerno]
        ,b.[customertitle]
        ,b.[dealercode]
        ,b.[dealerdesc]
        ,b.[equipmentno]
        ,b.[flagresponse]
        ,b.[flagstatus]
        ,b.[gidate]
        ,b.[model]
        ,b.[modelyear]
        ,b.[msisdn]
        ,b.[Parameter1]
        ,b.[priority]
        ,b.[proddesc]
        ,b.[salesmanname]
        ,b.[senddate]
        ,b.[supplydate]
        ,b.[supplydatewa]
        ,b.[responsedate]
        ,b.[customerresponse]
        ,b.[transmission]
        ,b.[flagdrop]
		,c.campaign_id
        ,c.[g1]
        ,c.[g2]
        ,c.[q1_validasi_nama_nomor_telp]
        ,c.[q2_validasi_jenis_kendaraan]
        ,c.[q3_validasi_outlet_pembelian]
        ,c.[q3a]
        ,c.[q4_validasi_tanggal_do]
        ,c.[q4a]
        ,c.[q5_validasi_nama_wiraniaga]
        ,c.[q5a]
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