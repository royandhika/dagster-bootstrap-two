{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_ahm',
		tags=['12hourly']
	)
}}

with prospect as (
    select 
        tp.id
		,tp.customer_id
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_ahm_tms_prospect_intelix') }} tp
    where main_campaign in ('007')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
        ,max(case when field_name_id = 'Nama' then data_content end) [Nama]
        ,max(case when field_name_id = 'Pekerjaan' then data_content end) [Pekerjaan]
        ,max(case when field_name_id = 'Alamat' then data_content end) [Alamat]
        ,max(case when field_name_id = 'Kode MD' then data_content end) [Kode MD]
        ,max(case when field_name_id = 'No HP clean' then data_content end) [No HP clean]
        ,max(case when field_name_id = 'Kota/Kabupaten' then data_content end) [Kota/Kabupaten]
        ,max(case when field_name_id = 'ID Customer' then data_content end) [ID Customer]
        ,max(case when field_name_id = 'Status Area' then data_content end) [Status Area]
        ,max(case when field_name_id = 'Kode Dealer' then data_content end) [Kode Dealer]
        ,max(case when field_name_id = 'Varian Motor' then data_content end) [Varian Motor]
        ,max(case when field_name_id = 'CRMID' then data_content end) [CRMID]
        ,max(case when field_name_id = 'No HP' then data_content end) [No HP]
        ,max(case when field_name_id = 'Sales Date' then data_content end) [Sales Date]
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_ahm_tms_prospect_detail_intelix') }} pd
    where exists (
    	select file_id 
    	from prospect p
    	where pd.file_id = p.file_id
	)
    group by prospect_id
)
,campaign_result as (
    select 
        cr.prospect_id
		,cr.campaign_id
		,cr.file_id
        ,max(case when field_name_id = 'konfrimasi_nama_customer' then data_content end) [konfrimasi_nama_customer]
        ,max(case when field_name_id = 'pertanyaan_cust' then data_content end) [pertanyaan_cust]
        ,max(case when field_name_id = 'q1' then data_content end) [q1]
        ,max(case when field_name_id = 'q2' then data_content end) [q2]
        ,max(case when field_name_id = 'q3' then data_content end) [q3]
        ,max(case when field_name_id = 'q4' then data_content end) [q4]
        ,max(case when field_name_id = 'q5' then data_content end) [q5]
        ,max(case when field_name_id = 'q6' then data_content end) [q6]
        ,max(case when field_name_id = 's1_ketersediaan_waktu' then data_content end) [s1_ketersediaan_waktu]
        ,max(case when field_name_id = 's2_hubungan_dengan_customer' then data_content end) [s2_hubungan_dengan_customer]
        ,max(case when field_name_id = 's2a_nama_keluarga' then data_content end) [s2a_nama_keluarga]
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_ahm_tms_prospect_campaign_result_intelix') }} cr
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
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_ahm_tms_prospect_history_contact_intelix') }} a
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
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_ahm_cc_master_category_intelix') }} b
        on a.LOV1 = b.id
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_ahm_cc_master_category_intelix') }} c
        on a.LOV2 = c.id
)
,time_frame as (
    select 
        a.id
        ,a.prospect_id
		,a.campaign_id
        ,a.response_status as [Campaign Result]
        ,a.response_sub_status as [Campaign Status]
        ,a.created_time as [Last Response Time]
        ,a.created_by as [Agent Id]
        ,row_number() over(partition by a.prospect_id, a.campaign_id order by a.created_time desc) as Sort
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_ahm_tms_prospect_time_frame_intelix') }} a
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
        ,a.customer_id
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
        ,b.[Nama]
        ,b.[Pekerjaan]
        ,b.[Alamat]
        ,b.[Kode MD]
        ,b.[No HP clean]
        ,b.[Kota/Kabupaten]
        ,b.[ID Customer]
        ,b.[Status Area]
        ,b.[Kode Dealer]
        ,b.[Varian Motor]
        ,b.[CRMID]
        ,b.[No HP]
        ,b.[Sales Date]
		,c.campaign_id
        ,c.konfrimasi_nama_customer
        ,c.pertanyaan_cust
        ,c.q1
        ,c.q2
        ,c.q3
        ,c.q4
        ,c.q5
        ,c.q6
        ,c.s1_ketersediaan_waktu
        ,c.s2_hubungan_dengan_customer
        ,c.s2a_nama_keluarga
		,d.[Connect Response]
		,d.[Contact Response]
		,d.[Description]
		,e.[Campaign Result]
		,e.[Campaign Status]
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