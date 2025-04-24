{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_ahm',
		tags=['outbound']
	)
}}

with prospect as (
    select 
        tp.id
		,tp.customer_id as CRM_ID
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
    where main_campaign in ('004')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
		,max(case when field_name_id in ('No.') then data_content end) as [No.]
		,max(case when field_name_id in ('Customer/Reporter Name') then data_content end) as [Customer/Reporter Name]
		,max(case when field_name_id in ('Address') then data_content end) as [Address]
		,max(case when field_name_id in ('City') then data_content end) as [City]
		,max(case when field_name_id in ('Bulan Open') then data_content end) as [Bulan Open]
		,max(case when field_name_id in ('Phone') then data_content end) as [Phone]
		,max(case when field_name_id in ('Phone clean') then data_content end) as [Phone clean]
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
        ,max(case when field_name_id = 'q1_konfirmasi_nama' then data_content end) as [q1 Konfirmasi Nama]
        ,max(case when field_name_id = 'q2_memiliki_motor_honda' then data_content end) as [q2 Memiliki Motor Honda]
        ,max(case when field_name_id = 'q3_kondisi_motor' then data_content end) as [q3 Kondisi Motor]
        ,max(case when field_name_id = 'q4_minat_nambah_sepeda_motor' then data_content end) as [q4 Minat Nambah Sepeda Motor]
        ,max(case when field_name_id = 'q4a_jenis_sepeda_motor' then data_content end) as [q4a Jenis Sepeda Motor]
        ,max(case when field_name_id = 'q4b_minat_pembelian' then data_content end) as [q4b Minat Pembelian]
        ,max(case when field_name_id = 'q4c_info_channel' then data_content end) as [q4c Info Channel]
        ,max(case when field_name_id = 'q4d_penawaran_promo' then data_content end) as [q4d Penawaran Promo]
        ,max(case when field_name_id = 'q5_konfirmasi_memberikan_no' then data_content end) as [q5 Konfirmasi Memberikan No]
        ,max(case when field_name_id = 'q6_konfirmasi_alamat' then data_content end) as [q6 Konfirmasi Alamat]
        ,max(case when field_name_id = 'q6a_konfirmasi_kota' then data_content end) as [q6a Konfirmasi Kota]
        ,max(case when field_name_id = 'q6b_md' then data_content end) as [q6b MD]
        ,max(case when field_name_id = 'q6c_bulan_pembelian' then data_content end) as [q6c Bulan Pembelian]
        ,max(case when field_name_id = 'q6d_assign_dealer' then data_content end) as [q6d Assign Dealer]
        ,max(case when field_name_id = 'q7_pertanyaan_customer' then data_content end) as [q7 Pertanyaan Customer]
        ,max(case when field_name_id = 'tipe_motor' then data_content end) as [Type Motor]
        ,max(case when field_name_id = 'update_alamat' then data_content end) as [Update Alamat]
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
		,b.[No.]
		,b.[Customer/Reporter Name]
		,b.[Address]
		,b.[City]
		,b.[Bulan Open]
		,b.[Phone]
		,b.[Phone clean]
		,c.campaign_id
        ,c.[q1 Konfirmasi Nama]
        ,c.[q2 Memiliki Motor Honda]
        ,c.[q3 Kondisi Motor]
        ,c.[q4 Minat Nambah Sepeda Motor]
        ,c.[q4a Jenis Sepeda Motor]
        ,c.[q4b Minat Pembelian]
        ,c.[q4c Info Channel]
        ,c.[q4d Penawaran Promo]
        ,c.[q5 Konfirmasi Memberikan No]
        ,c.[q6 Konfirmasi Alamat]
        ,c.[q6a Konfirmasi Kota]
        ,c.[q6b MD]
        ,c.[q6c Bulan Pembelian]
        ,c.[q6d Assign Dealer]
        ,c.[q7 Pertanyaan Customer]
        ,c.[Type Motor]
        ,c.[Update Alamat]
		,d.[Connect Response]
		,d.[Contact Response]
		,d.[Description]
		,e.[Campaign Result]
		,e.[Campaign Status]
		,d.[Last Response Time]
		,e.[Agent Id]
		,d.[Attempt Call]
		,d.[Appointment Date]
		,d.[Appointment Time]
		,a.CRM_ID
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