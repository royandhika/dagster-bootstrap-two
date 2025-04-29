{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
        unique_key=['id', '"Attempt Call"'],
        group='outbound_adm',
		tags=['12hourly']
	)
}}

with prospect as (
    select 
        tp.id
		,tp.file_id
		,tp.main_campaign
        ,tp.customer_id
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_adm_tms_prospect_intelix') }} tp
    where main_campaign  in ('ADM007')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
        ,max(case when field_name_id in ('01 Nama Pengguna / Pemilik Kendaraan') then data_content end) as [01 Nama Pengguna / Pemilik Kendaraan]
		,max(case when field_name_id in ('02 Model') then data_content end) as [02 Model]
		,max(case when field_name_id in ('03 PlatNumber') then data_content end) as [03 PlatNumber]
		,max(case when field_name_id in ('04 SA Name') then data_content end) as [04 SA Name]
		,max(case when field_name_id in ('05 LastService') then data_content end) as [05 LastService]
		,max(case when field_name_id in ('06 Address') then data_content end) as [06 Address]
		,max(case when field_name_id in ('08 MSISDN') then data_content end) as [08 MSISDN]
		,max(case when field_name_id in ('09 ProductYear') then data_content end) as [09 ProductYear]
		,max(case when field_name_id in ('10 MileAge') then data_content end) as [10 MileAge]
		,max(case when field_name_id in ('11 NomorRangka') then data_content end) as [11 NomorRangka]
		,max(case when field_name_id in ('14 SupplyType') then data_content end) as [14 SupplyType]
		,max(case when field_name_id in ('16 Gender') then data_content end) as [16 Gender]
		,max(case when field_name_id in ('17 BranchCode') then data_content end) as [17 BranchCode]
		,max(case when field_name_id in ('18 BranchName') then data_content end) as [18 BranchName]
		,max(case when field_name_id in ('19 Jenis Pekerjaan') then data_content end) as [19 Jenis Pekerjaan]
		,max(case when field_name_id in ('23 AlternativePhoneNumber') then data_content end) as [23 AlternativePhoneNumber]
		,max(case when field_name_id in ('Agent ID') then data_content end) as [Agent ID]
		,max(case when field_name_id in ('parameter1') then data_content end) as [parameter1]
		,max(case when field_name_id in ('supply period') then data_content end) as [supply period]
		,max(case when field_name_id in ('supplyweek') then data_content end) as [supplyweek]
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_adm_tms_prospect_detail_intelix') }} pd
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
		,max(case when field_name_id in ('q1_reminder_service') then data_content end) as q1_reminder_service
		,max(case when field_name_id in ('q2_booking_service') then data_content end) as q2_booking_service
		,max(case when field_name_id in ('q3_attitude_sa') then data_content end) as q3_attitude_sa
		,max(case when field_name_id in ('q3a') then data_content end) as q3a
		,max(case when field_name_id in ('q4_waktu_menunggu_sebelum_dila') then data_content end) as q4_waktu_menunggu_sebelum_dila
		,max(case when field_name_id in ('q4a') then data_content end) as q4a
		,max(case when field_name_id in ('q5_knowledge_sa') then data_content end) as q5_knowledge_sa
		,max(case when field_name_id in ('q5a') then data_content end) as q5a
		,max(case when field_name_id in ('q6_konfirmasi_pekerjaan_tambah') then data_content end) as q6_konfirmasi_pekerjaan_tambah
		,max(case when field_name_id in ('q7_kesesuaian_waktu_service') then data_content end) as q7_kesesuaian_waktu_service
		,max(case when field_name_id in ('q7a') then data_content end) as q7a
		,max(case when field_name_id in ('q8_layanan_vehicle_pick-up') then data_content end) as [q8_layanan_vehicle_pick-up]
		,max(case when field_name_id in ('q9_layanan_vehicle_pick-up') then data_content end) as [q9_layanan_vehicle_pick-up]
		,max(case when field_name_id in ('q9a') then data_content end) as q9a
		,max(case when field_name_id in ('q10_kualitas_hasil_service') then data_content end) as q10_kualitas_hasil_service
		,max(case when field_name_id in ('q10a') then data_content end) as q10a
		,max(case when field_name_id in ('q11_kebersihan_kendaraan') then data_content end) as q11_kebersihan_kendaraan
		,max(case when field_name_id in ('q12_booking_after_service') then data_content end) as q12_booking_after_service
		,max(case when field_name_id in ('q13_follow_up_after_service') then data_content end) as q13_follow_up_after_service
		,max(case when field_name_id in ('q14_fasilitas_ruang_tunggu_ben') then data_content end) as q14_fasilitas_ruang_tunggu_ben
		,max(case when field_name_id in ('q15_feedback') then data_content end) as q15_feedback
		,max(case when field_name_id in ('s1_konfirmasi_kendaraan') then data_content end) as s1_konfirmasi_kendaraan
		,max(case when field_name_id in ('s2_konfirmasi_cabang') then data_content end) as s2_konfirmasi_cabang
		,max(case when field_name_id in ('s3_konfirmasi_keterlibatan_cus') then data_content end) as s3_konfirmasi_keterlibatan_cus
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_adm_tms_prospect_campaign_result_intelix') }} cr
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_adm_tms_prospect_history_contact_intelix') }} a
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
    from history_contact a
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_adm_cc_master_category_intelix') }} b 
        ON a.LOV1 = b.id
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_adm_cc_master_category_intelix') }} c 
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_adm_tms_prospect_time_frame_intelix') }} a
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
		,b.[01 Nama Pengguna / Pemilik Kendaraan]
		,b.[02 Model]
		,b.[03 PlatNumber]
		,b.[04 SA Name]
		,b.[05 LastService]
		,b.[06 Address]
		,b.[08 MSISDN]
		,b.[09 ProductYear]
		,b.[10 MileAge]
		,b.[11 NomorRangka]
		,b.[14 SupplyType]
		,b.[16 Gender]
		,b.[17 BranchCode]
		,b.[18 BranchName]
		,b.[19 Jenis Pekerjaan]
		,b.[23 AlternativePhoneNumber]
		,b.[Agent ID]
		,b.[parameter1]
		,b.[supply period]
		,b.[supplyweek]
		,c.campaign_id
		,c.q10_kualitas_hasil_service
		,c.q10a
		,c.q11_kebersihan_kendaraan
		,c.q12_booking_after_service
		,c.q13_follow_up_after_service
		,c.q14_fasilitas_ruang_tunggu_ben
		,c.q15_feedback
		,c.q1_reminder_service
		,c.q2_booking_service
		,c.q3_attitude_sa
		,c.q3a
		,c.q4_waktu_menunggu_sebelum_dila
		,c.q4a
		,c.q5_knowledge_sa
		,c.q5a
		,c.q6_konfirmasi_pekerjaan_tambah
		,c.q7_kesesuaian_waktu_service
		,c.q7a
		,c.[q8_layanan_vehicle_pick-up]
		,c.[q9_layanan_vehicle_pick-up]
		,c.q9a
		,c.s1_konfirmasi_kendaraan
		,c.s2_konfirmasi_cabang
		,c.s3_konfirmasi_keterlibatan_cus
		,d.[Connect Response]
		,d.[Contact Response]
		,d.[Description]
		,e.[Campaign Result]
		,e.[Campaign Status]
		,e.[Campaign Description]
		,d.[Last Response Time]
		-- ,e.[Agent Id]
		,[Attempt Call] = case 
			when d.[Last Response Time] is null then null 
			else row_number() over(partition by a.id order by d.[last Response Time])
		end
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
	left join time_frame e 
        on a.id = e.prospect_id 
		and c.campaign_id = e.campaign_id
		and d.[Last Response Time] between dateadd(second, -20, e.[Last Response Time]) and dateadd(second, 20, e.[Last Response Time])
)
select * from final