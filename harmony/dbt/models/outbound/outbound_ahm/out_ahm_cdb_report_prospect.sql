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
    where main_campaign in ('002')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        pd.prospect_id
		,max(case when field_name_id = 'NAMA_PEMILIK' then data_content end) as [NAMA_PEMILIK]
		,max(case when field_name_id = 'TYPE_MOTOR' then data_content end) as [TYPE_MOTOR]
		,max(case when field_name_id = 'COLOR' then data_content end) as [COLOR]
		,max(case when field_name_id = 'SALES_DATE' then data_content end) as [SALES_DATE]
		,max(case when field_name_id = 'TANGGAL_LAHIR' then data_content end) as [TANGGAL_LAHIR]
		,max(case when field_name_id = 'ALAMAT_SURAT' then data_content end) as [ALAMAT_SURAT]
		,max(case when field_name_id = 'KELURAHAN_SURAT' then data_content end) as [KELURAHAN_SURAT]
		,max(case when field_name_id = 'KECAMATAN_SURAT' then data_content end) as [KECAMATAN_SURAT]
		,max(case when field_name_id = 'KOTA_SURAT' then data_content end) as [KOTA_SURAT]
		,max(case when field_name_id = 'KODE_POS' then data_content end) as [KODE_POS]
		,max(case when field_name_id = 'PROPINSI' then data_content end) as [PROPINSI]
		,max(case when field_name_id = 'EMAIL' then data_content end) as [EMAIL]
		,max(case when field_name_id = 'AGAMA' then data_content end) as [AGAMA]
		,max(case when field_name_id = 'PEKERJAAN' then data_content end) as [PEKERJAAN]
		,max(case when field_name_id = 'FRAME_NO' then data_content end) as [FRAME_NO]
		,max(case when field_name_id = 'HPCleansing' then data_content end) as [HPCleansing]
		,max(case when field_name_id = 'TelpCleansing' then data_content end) as [TelpCleansing]
		,max(case when field_name_id = 'MD' then data_content end) as [MD]
		,max(case when field_name_id = 'DEALER_CODE' then data_content end) as [DEALER_CODE]
		,max(case when field_name_id = 'NAMA DEALER' then data_content end) as [NAMA DEALER]
		,max(case when field_name_id = 'KELURAHAN DEALER' then data_content end) as [KELURAHAN DEALER]
		,max(case when field_name_id = 'MERK_MOTOR_SBLMNYA' then data_content end) as [MERK_MOTOR_SBLMNYA]
		,max(case when field_name_id = 'TYPE_MOTOR_SBLMNYA' then data_content end) as [TYPE_MOTOR_SBLMNYA]
		,max(case when field_name_id = 'FACEBOOK' then data_content end) as [FACEBOOK]
		,max(case when field_name_id = 'TWITTER' then data_content end) as [TWITTER]
		,max(case when field_name_id = 'INSTAGRAM' then data_content end) as [INSTAGRAM]
		,max(case when field_name_id = 'YOUTUBE' then data_content end) as [YOUTUBE]
		,max(case when field_name_id = 'STATUS_VALIDASI' then data_content end) as [STATUS_VALIDASI]
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
		,max(case when field_name_id = 'q1_konfrimasi_pembelian' then data_content end) as [Q1 Konfrimasi pembelian]
		,max(case when field_name_id = 'q2_memiliki_motor_lain' then data_content end) as [Q2 Memiliki motor lain]
		,max(case when field_name_id = 'q2a_merk_motor_lainnya' then data_content end) as [Q2a Merk motor lainnya]
		,max(case when field_name_id = 'merk_other' then data_content end) as [Merk other]
		,max(case when field_name_id = 'q3_konfrimasi_tanggal_lahir' then data_content end) as [Q3 Konfrimasi tanggal lahir]
		,max(case when field_name_id = 'q3a_update_tanggal_lahir' then data_content end) as [Q3a Update tanggal lahir]
		,max(case when field_name_id = 'q4_konfrimasi_alamat_bapak' then data_content end) as [Q4 Konfrimasi alamat Bapak]
		,max(case when field_name_id = 'q4a_alamat_update' then data_content end) as [Q4a Alamat Update]
		,max(case when field_name_id = 'kelurahan_update' then data_content end) as [Kelurahan Update]
		,max(case when field_name_id = 'kecamatan_update' then data_content end) as [Kecamatan Update]
		,max(case when field_name_id = 'kodya_update' then data_content end) as [Kodya Update]
		,max(case when field_name_id = 'provinsi_update' then data_content end) as [Provinsi Update]
		,max(case when field_name_id = 'q5_konfrimasi_punya_email' then data_content end) as [Q5 Konfrimasi punya email]
		,max(case when field_name_id = 'q6_konfrimasi_agama' then data_content end) as [Q6 Konfrimasi Agama]
		,max(case when field_name_id = 'q6a_new_agama' then data_content end) as [Q6 New Agama]
		,max(case when field_name_id = 'q5a_alamat_email' then data_content end) as [Q5a Alamat Email]
		,max(case when field_name_id = 'q7_konfrimasi_pekerjaan' then data_content end) as [Q7 Konfrimasi Pekerjaan]
		,max(case when field_name_id = 'q7a_detail_pekerjaan' then data_content end) as [Q7a Detail Pekerjaan]
		,max(case when field_name_id = 'q7a_detailkan_pekerjaan' then data_content end) as [Q7a Detailkan Pekerjaan]
		,max(case when field_name_id = 'q7b_new_pekerjaan' then data_content end) as [Q7b New Pekerjaan]
		,max(case when field_name_id = 'q7b_detail_pekerjaan_update' then data_content end) as [Q7b Detail Pekerjaan Update]
		,max(case when field_name_id = 'q7b_detailkan_pekerjaan_other' then data_content end) as [Q7b Detailkan Pekerjaan Other]
		,max(case when field_name_id = 'q8_pernah_merekomendasikan' then data_content end) as [Q8 pernah merekomendasikan]
		,max(case when field_name_id = 'q8a_akan_merekomendasikan' then data_content end) as [Q8a Akan merekomendasikan]
		,max(case when field_name_id = 'q8b_rekomen_ke_kerabat_lainnya' then data_content end) as [Q8b rekomen ke kerabat lainnya]
		,max(case when field_name_id = 'q9_konfrimasi_no_hp_lainnya' then data_content end) as [Q9 Konfrimasi No HP lainnya]
		,max(case when field_name_id = 'q9a_no_hp_lainnya' then data_content end) as [Q9a No Hp Lainnya]
		,max(case when field_name_id = 'q10_no_terdaftar_di_wa' then data_content end) as [Q10 No terdaftar di WA]
		,max(case when field_name_id = 'q11_pembelian_di_dealer_sama' then data_content end) as [Q11 pembelian di dealer sama]
		,max(case when field_name_id = '_q12_pernah_servis_dealer_sama' then data_content end) as [Q12 pernah servis dealer sama]
		,max(case when field_name_id = 'q13_pembelian_smh_tim_sales' then data_content end) as [Q13 pembelian SMH tim sales]
		,max(case when field_name_id = 'q14_diingatkan_service_dealer' then data_content end) as [Q14 diingatkan Service dealer]
		,max(case when field_name_id = 'q14a_diingatkan_service_via' then data_content end) as [Q14a Diingatkan Service via]
		,max(case when field_name_id = 'q15_memiliki_sosial_media' then data_content end) as [Q15 Memiliki Sosial Media]
		,max(case when field_name_id = 'account_facebook' then data_content end) as [Account Facebook]
		,max(case when field_name_id = 'account_twitter' then data_content end) as [Account Twitter]
		,max(case when field_name_id = 'account_instagram' then data_content end) as [Account Instagram]
		,max(case when field_name_id = 'account_youtube' then data_content end) as [Account Youtube]
		,max(case when field_name_id = 'q15a_media_sosial_masih_aktif' then data_content end) as [Q15a media sosial masih aktif]
		,max(case when field_name_id = 'q16_mengetahui_nomor_cc_honda' then data_content end) as [Q16 mengetahui nomor cc Honda]
		,max(case when field_name_id = 'q16a_bersedia_menghub_cc_honda' then data_content end) as [Q16a bersedia menghub cc honda]
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
		,a.response_status AS [Campaign Result]
		,a.response_sub_status AS [Campaign Status]
		,a.description AS [Campaign Description]
		,a.created_time AS [Last Response Time]
		,a.created_by AS [Agent Id]
        ,row_number() over(partition by a.prospect_id, a.campaign_id order by a.created_time desc) as Sort
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_ahm_tms_prospect_time_frame_intelix') }} a
    where a.file_id in (select file_id from prospect)
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
		,b.[NAMA_PEMILIK]
		,b.[TYPE_MOTOR]
		,b.[COLOR]
		,b.[SALES_DATE]
		,b.[TANGGAL_LAHIR]
		,b.[ALAMAT_SURAT]
		,b.[KELURAHAN_SURAT]
		,b.[KECAMATAN_SURAT]
		,b.[KOTA_SURAT]
		,b.[KODE_POS]
		,b.[PROPINSI]
		,b.[EMAIL]
		,b.[AGAMA]
		,b.[PEKERJAAN]
		,b.[FRAME_NO]
		,b.[HPCleansing]
		,b.[TelpCleansing]
		,b.[MD]
		,b.[DEALER_CODE]
		,b.[NAMA DEALER]
		,b.[KELURAHAN DEALER]
		,b.[MERK_MOTOR_SBLMNYA]
		,b.[TYPE_MOTOR_SBLMNYA]
		,b.[FACEBOOK]
		,b.[TWITTER]
		,b.[INSTAGRAM]
		,b.[YOUTUBE]
		,b.[STATUS_VALIDASI]
		,c.campaign_id
		,c.[Q1 Konfrimasi pembelian]
		,c.[Q2 Memiliki motor lain]
		,c.[Q2a Merk motor lainnya]
		,c.[Merk other]
		,c.[Q3 Konfrimasi tanggal lahir]
		,c.[Q3a Update tanggal lahir]
		,c.[Q4 Konfrimasi alamat Bapak]
		,c.[Q4a Alamat Update]
		,c.[Kelurahan Update]
		,c.[Kecamatan Update]
		,c.[Kodya Update]
		,c.[Provinsi Update]
		,c.[Q5 Konfrimasi punya email]
		,c.[Q6 Konfrimasi Agama]
		,c.[Q6 New Agama]
		,c.[Q5a Alamat Email]
		,c.[Q7 Konfrimasi Pekerjaan]
		,c.[Q7a Detail Pekerjaan]
		,c.[Q7a Detailkan Pekerjaan]
		,c.[Q7b New Pekerjaan]
		,c.[Q7b Detail Pekerjaan Update]
		,c.[Q7b Detailkan Pekerjaan Other]
		,c.[Q8 pernah merekomendasikan]
		,c.[Q8a Akan merekomendasikan]
		,c.[Q8b rekomen ke kerabat lainnya]
		,c.[Q9 Konfrimasi No HP lainnya]
		,c.[Q9a No Hp Lainnya]
		,c.[Q10 No terdaftar di WA]
		,c.[Q11 pembelian di dealer sama]
		,c.[Q12 pernah servis dealer sama]
		,c.[Q13 pembelian SMH tim sales]
		,c.[Q14 diingatkan Service dealer]
		,c.[Q14a Diingatkan Service via]
		,c.[Q15 Memiliki Sosial Media]
		,c.[Account Facebook]
		,c.[Account Twitter]
		,c.[Account Instagram]
		,c.[Account Youtube]
		,c.[Q15a media sosial masih aktif]
		,c.[Q16 mengetahui nomor cc Honda]
		,c.[Q16a bersedia menghub cc honda]
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