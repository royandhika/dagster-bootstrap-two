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
    where main_campaign in ('001')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
		,max(case when field_name_id in ('vmain_dealer') then data_content end) as [vmain_dealer]
		,max(case when field_name_id in ('vdealer_code') then data_content end) as [vdealer_code]
		,max(case when field_name_id in ('vframeno','vframe_no') then data_content end) as [vframeno]
		,max(case when field_name_id in ('dsales_date', 'sales_date') then data_content end) as [dsales_date]
		,max(case when field_name_id in ('vtype_motor', 'vtype_motor_terakhir') then data_content end) as [vtype_motor]
		,max(case when field_name_id in ('vnama_pemilik') then data_content end) as [vnama_pemilik]
		,max(case when field_name_id in ('vjenis_kelamin') then data_content end) as [vjenis_kelamin]
		,max(case when field_name_id in ('valamat_surat') then data_content end) as [valamat_surat]
		,max(case when field_name_id in ('vkelurahan_surat') then data_content end) as [vkelurahan_surat]
		,max(case when field_name_id in ('vkecamatan_surat') then data_content end) as [vkecamatan_surat]
		,max(case when field_name_id in ('vpropinsi') then data_content end) as [vpropinsi]
		,max(case when field_name_id in ('vno_hp_cleansing','vno_hp clean') then data_content end) as [vno_hp_cleansing]
		,max(case when field_name_id in ('vno_telp_cleansing','vno_telp clean') then data_content end) as [vno_telp_cleansing]
		,max(case when field_name_id in ('nama_kotakabupaten') then data_content end) as [nama_kotakabupaten]
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
		,max(case when field_name_id = 'q6_konfrimasi_pembelian' then data_content end) as [Q6 Konfrimasi Pembelian]
		,max(case when field_name_id = 'q7_kondisi_motor' then data_content end) as [Q7 Kondisi Motor]
		,max(case when field_name_id = 'q8_minat_promo' then data_content end) as [Q8 Minat Promo]
		,max(case when field_name_id = 'segment' then data_content end) as [Segment]
		,max(case when field_name_id = 'type_motor' then data_content end) as [Type Motor]
		,max(case when field_name_id = 'q8a_minat_pembelian' then data_content end) as [Q8a Minat Pembelian]
		,max(case when field_name_id = 'q8b_info_channel' then data_content end) as [Q8b Info Channel]
		,max(case when field_name_id = 'q8c_ketersediaan_tawaran_promo' then data_content end) as [Q8c ketersediaan tawaran promo]
		,max(case when field_name_id = 'q10_konfrimasi_alamat' then data_content end) as [Q10 Konfrimasi Alamat]
		,max(case when field_name_id = 'q11_minat_booking_service' then data_content end) as [Q11 Minat Booking Service]
		,max(case when field_name_id = 'q12_rencana_service' then data_content end) as [Q12 Rencana service]
		,max(case when field_name_id = 'q12a_minat_service' then data_content end) as [Q12a Minat service]
		,max(case when field_name_id = 'q12b_tanggal_service' then data_content end) as [Q12b Tanggal Service]
		,max(case when field_name_id = 'q12c_lokasi_service' then data_content end) as [Q12c Lokasi Service]
		,max(case when field_name_id = 'q12d_area_ahass' then data_content end) as [Q12d Area AHASS]
		,max(case when field_name_id = 'q13_notes' then data_content end) as [Q13 Notes]
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
		,b.[vmain_dealer]
		,b.[vdealer_code]
		,b.[vframeno]
		,b.[dsales_date]
		,b.[vtype_motor]
		,b.[vnama_pemilik]
		,b.[vjenis_kelamin]
		,b.[valamat_surat]
		,b.[vkelurahan_surat]
		,b.[vkecamatan_surat]
		,b.[vpropinsi]
		,b.[vno_hp_cleansing]
		,b.[vno_telp_cleansing]
		,b.[nama_kotakabupaten]
		,c.campaign_id
		,c.[Q6 Konfrimasi Pembelian]
		,c.[Q7 Kondisi Motor]
		,c.[Q8 Minat Promo]
		,c.[Segment]
		,c.[Type Motor]
		,c.[Q8a Minat Pembelian]
		,c.[Q8b Info Channel]
		,c.[Q8c ketersediaan tawaran promo]
		,c.[Q10 Konfrimasi Alamat]
		,c.[Q11 Minat Booking Service]
		,c.[Q12 Rencana service]
		,c.[Q12a Minat service]
		,c.[Q12b Tanggal Service]
		,c.[Q12c Lokasi Service]
		,c.[Q12d Area AHASS]
		,c.[Q13 Notes]
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