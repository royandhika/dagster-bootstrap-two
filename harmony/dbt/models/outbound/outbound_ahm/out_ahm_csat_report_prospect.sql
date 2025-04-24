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
    where main_campaign in ('005')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        pd.prospect_id
		,max(case when field_name_id = 'Jumlah' then data_content end) as [Jumlah]
		,max(case when field_name_id = 'No' then data_content end) as [No]
		,max(case when field_name_id = 'Kode MD' then data_content end) as [Kode MD]
		,max(case when field_name_id = 'Nama Konsumen' then data_content end) as [Nama Konsumen]
		,max(case when field_name_id = 'Tiket ICH' then data_content end) as [Tiket ICH]
		,max(case when field_name_id = 'No Telepon' then data_content end) as [No Telepon]
		,max(case when field_name_id = 'No Telepon 2' then data_content end) as [No Telepon 2]
		,max(case when field_name_id = 'No Telepon Clean' then data_content end) as [No Telepon Clean]
		,max(case when field_name_id = 'No Telepon 2 Clean' then data_content end) as [No Telepon 2 Clean]
		,max(case when field_name_id = 'Email' then data_content end) as [Email]
		,max(case when field_name_id = 'Akun Sosmed' then data_content end) as [Akun Sosmed]
		,max(case when field_name_id = 'Sepeda Motor' then data_content end) as [Sepeda Motor]
		,max(case when field_name_id = 'Nomor Polisi' then data_content end) as [Nomor Polisi]
		,max(case when field_name_id = 'Nomor Rangka' then data_content end) as [Nomor Rangka]
		,max(case when field_name_id = 'Tahun Motor' then data_content end) as [Tahun Motor]
		,max(case when field_name_id = 'Masalah' then data_content end) as [Masalah]
		,max(case when field_name_id = 'Nama Lokasi AHASS' then data_content end) as [Nama Lokasi AHASS]
		,max(case when field_name_id = 'Permasalahan Lain' then data_content end) as [Permasalahan Lain]
		,max(case when field_name_id = 'Alamat' then data_content end) as [Alamat]
		,max(case when field_name_id = 'Kota' then data_content end) as [Kota]
		,max(case when field_name_id = 'Provinsi' then data_content end) as [Provinsi]
		,max(case when field_name_id = 'Latitude' then data_content end) as [Latitude]
		,max(case when field_name_id = 'Longitude' then data_content end) as [Longitude]
		,max(case when field_name_id = 'Tipe Servis' then data_content end) as [Tipe Servis]
		,max(case when field_name_id = 'Jarak Tempuh' then data_content end) as [Jarak Tempuh]
		,max(case when field_name_id = 'Kategori Jarak Tempuh' then data_content end) as [Kategori Jarak Tempuh]
		,max(case when field_name_id = 'Waktu Input' then data_content end) as [Waktu Input]
		,max(case when field_name_id = 'Waktu Menghubungi' then data_content end) as [Waktu Menghubungi]
		,max(case when field_name_id = 'Waktu Diinginkan Konsumen' then data_content end) as [Waktu Diinginkan Konsumen]
		,max(case when field_name_id = 'Waktu Menangani' then data_content end) as [Waktu Menangani]
		,max(case when field_name_id = 'Status Penyelesaian' then data_content end) as [Status Penyelesaian]
		,max(case when field_name_id = 'TKP 1' then data_content end) as [TKP 1]
		,max(case when field_name_id = 'TKP 2' then data_content end) as [TKP 2]
		,max(case when field_name_id = 'TKP 3' then data_content end) as [TKP 3]
		,max(case when field_name_id = 'AHASS 1' then data_content end) as [AHASS 1]
		,max(case when field_name_id = 'AHASS 2' then data_content end) as [AHASS 2]
		,max(case when field_name_id = 'AHASS 3' then data_content end) as [AHASS 3]
		,max(case when field_name_id = 'Part 1' then data_content end) as [Part 1]
		,max(case when field_name_id = 'Part 2' then data_content end) as [Part 2]
		,max(case when field_name_id = 'Part 3' then data_content end) as [Part 3]
		,max(case when field_name_id = 'Part 4' then data_content end) as [Part 4]
		,max(case when field_name_id = 'Part 5' then data_content end) as [Part 5]
		,max(case when field_name_id = 'Harga Part' then data_content end) as [Harga Part]
		,max(case when field_name_id = 'Keterangan' then data_content end) as [Keterangan]
		,max(case when field_name_id = 'Approval' then data_content end) as [Approval]
		,max(case when field_name_id = 'Backdate' then data_content end) as [Backdate]
		,max(case when field_name_id = 'tipe tahun motor' then data_content end) as [tipe tahun motor]
		,max(case when field_name_id = 'Kategori Waktu' then data_content end) as [Kategori Waktu]
		,max(case when field_name_id = 'Nama MD' then data_content end) as [Nama MD]
		,max(case when field_name_id = 'Bulan PICA' then data_content end) as [Bulan PICA]
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
		,max(case when field_name_id = 'q1_menggunakan_fasilitas_honda' then data_content end) as [q1]
		,max(case when field_name_id = 'q2_darimana_mengetahui_fasilit' then data_content end) as [q2]
		,max(case when field_name_id = 'q3__petugas_dealer_pernah_meng' then data_content end) as [q3]
		,max(case when field_name_id = 'q4_berapa_kali_menghubungi_lay' then data_content end) as [q4]
		,max(case when field_name_id = 'q5_berapa_lama_armada_hc_tiba_' then data_content end) as [q5]
		,max(case when field_name_id = 'q6_kepuasan_waktu_tunggu_mekan' then data_content end) as [q6]
		,max(case when field_name_id = 'q6_a_ideal_berapa_lama_waktu_t' then data_content end) as [q6_a]
		,max(case when field_name_id = 'q7_tindakan_mekanik_honda_care' then data_content end) as [q7]
		,max(case when field_name_id = 'q8_mekani_honda_care_menawarka' then data_content end) as [q8]
		,max(case when field_name_id = 'q9_mekani_honda_care_menawarka' then data_content end) as [q9]
		,max(case when field_name_id = 'q10_kepuasan_pelayanan_dari_ho' then data_content end) as [q10]
		,max(case when field_name_id = 'q10_a_mengapa_tidak_puas_denga' then data_content end) as [q10_a]
		,max(case when field_name_id = 'q10_a_lainnya' then data_content end) as [q10_a_lainnya]
		,max(case when field_name_id = 'notes_pertanyaan_customer' then data_content end) as [notes]
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
		,b.[Jumlah]
		,b.[No]
		,b.[Kode MD]
		,b.[Nama Konsumen]
		,b.[Tiket ICH]
		,b.[No Telepon]
		,b.[No Telepon 2]
		,b.[No Telepon Clean]
		,b.[No Telepon 2 Clean]
		,b.[Email]
		,b.[Akun Sosmed]
		,b.[Sepeda Motor]
		,b.[Nomor Polisi]
		,b.[Nomor Rangka]
		,b.[Tahun Motor]
		,b.[Masalah]
		,b.[Nama Lokasi AHASS]
		,b.[Permasalahan Lain]
		,b.[Alamat]
		,b.[Kota]
		,b.[Provinsi]
		,b.[Latitude]
		,b.[Longitude]
		,b.[Tipe Servis]
		,b.[Jarak Tempuh]
		,b.[Kategori Jarak Tempuh]
		,b.[Waktu Input]
		,b.[Waktu Menghubungi]
		,b.[Waktu Diinginkan Konsumen]
		,b.[Waktu Menangani]
		,b.[Status Penyelesaian]
		,b.[TKP 1]
		,b.[TKP 2]
		,b.[TKP 3]
		,b.[AHASS 1]
		,b.[AHASS 2]
		,b.[AHASS 3]
		,b.[Part 1]
		,b.[Part 2]
		,b.[Part 3]
		,b.[Part 4]
		,b.[Part 5]
		,b.[Harga Part]
		,b.[Keterangan]
		,b.[Approval]
		,b.[Backdate]
		,b.[tipe tahun motor]
		,b.[Kategori Waktu]
		,b.[Nama MD]
		,b.[Bulan PICA]
		,c.campaign_id
		,c.[q1]
		,c.[q2]
		,c.[q3]
		,c.[q4]
		,c.[q5]
		,c.[q6]
		,c.[q7]
		,c.[q8]
		,c.[q9]
		,c.[q10]
		,c.[q6_a]
		,c.[q10_a]
		,c.[q10_a_lainnya]
		,c.[notes]
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