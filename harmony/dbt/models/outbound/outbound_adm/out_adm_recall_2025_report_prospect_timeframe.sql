{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
        unique_key=['id', 'attempt_call'],
        group='outbound_adm',
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_adm_tms_prospect_intelix') }} tp
    where main_campaign  in ('ADM008', 'ADM009')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
        ,max(case when field_name_id in ('Address') then  data_content end) as [address]
        ,max(case when field_name_id in ('Assign Agent') then  data_content end) as [agent]
        ,max(case when field_name_id in ('Supply Date') then  data_content end) as [supplydate]
        ,max(case when field_name_id in ('Customer Name') then  data_content end) as [customername]
        ,max(case when field_name_id in ('Kategori Recall') then  data_content end) as [recall_category]
        ,max(case when field_name_id in ('Mobile Phone') then  data_content end) as [phone]
        ,max(case when field_name_id in ('Model') then  data_content end) as [model]
        ,max(case when field_name_id in ('Parameter1') then  data_content end) as [documentid]
        ,max(case when field_name_id in ('Plat Number') then  data_content end) as [platnum]
        ,max(case when field_name_id in ('Predict Service') then  data_content end) as [predict_pkbdate]
        ,max(case when field_name_id in ('SourceCode') then  data_content end) as [dealercode]
        ,max(case when field_name_id in ('SourceDesc') then  data_content end) as [dealerdesc]
        ,max(case when field_name_id in ('VIN') then  data_content end) as [equipmentno]
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
        ,max(case when field_name_id in ('konfirm_program_ssc_daihatsu') then  data_content end) as [konfirm_program_ssc_daihatsu]
        ,max(case when field_name_id in ('konfrim_pemilik_unit_daihatsu') then  data_content end) as [konfrim_pemilik_unit_daihatsu]
        ,max(case when field_name_id in ('q0') then  data_content end) as [q0]
        ,max(case when field_name_id in ('q1_penjelasan_program_ssc') then  data_content end) as [q1_penjelasan_program_ssc]
        ,max(case when field_name_id in ('q2_penjelasan_program_ssc') then  data_content end) as [q2_penjelasan_program_ssc]
        ,max(case when field_name_id in ('q3_konfrim_cus_ttg_program') then  data_content end) as [q3_konfrim_cus_ttg_program]
        ,max(case when field_name_id in ('q4_konfirm_program_ecu') then  data_content end) as [q4_konfirm_program_ecu]
        ,max(case when field_name_id in ('q4a') then  data_content end) as [q4a]
        ,max(case when field_name_id in ('q5_konf_buat_jadwal_di_bengkel') then  data_content end) as [q5_konf_buat_jadwal_di_bengkel]
        ,max(case when field_name_id in ('q6_konf_cust_tgl_service') then  data_content end) as [q6_konf_cust_tgl_service]
        ,max(case when field_name_id in ('q6a_konf_cust_jam_service') then  data_content end) as [q6a_konf_cust_jam_service]
        ,max(case when field_name_id in ('q6b_lokasi_bengkel') then  data_content end) as [q6b_lokasi_bengkel]
        ,max(case when field_name_id in ('q7_alasan_tdk_pemeriksaan') then  data_content end) as [q7_alasan_tdk_pemeriksaan]
        ,max(case when field_name_id in ('q7a_mapping_bengkel_terdekat') then  data_content end) as [q7a_mapping_bengkel_terdekat]
        ,max(case when field_name_id in ('q7b_resiko_blm_pengecekan') then  data_content end) as [q7b_resiko_blm_pengecekan]
        ,max(case when field_name_id in ('q7c_no_pelanggan_rental') then  data_content end) as [q7c_no_pelanggan_rental]
        ,max(case when field_name_id in ('q7d_alasan_lain') then  data_content end) as [q7d_alasan_lain]
        ,max(case when field_name_id in ('q8_confrim_ketersediaan_waktu') then  data_content end) as [q8_confrim_ketersediaan_waktu]
        ,max(case when field_name_id in ('q9_closing') then  data_content end) as [q9_closing]
        ,max(case when field_name_id in ('q9a_closing') then  data_content end) as [q9a_closing]
        ,max(case when field_name_id in ('q9b_closing') then  data_content end) as [q9b_closing]
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
		,a.notes as description
		,a.appointmentDate as [appointment_date]
		,a.appointmentTime as [appointment_time]
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
		,b.[name] as [connect_response]
		,c.[name] as [contact_response]
		,a.[description]
		,a.[appointment_date]
		,a.[appointment_time]
		,a.[created_time] as [last_response_time]
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
        ,a.response_status as [campaign_result]
        ,a.response_sub_status as [campaign_status]
        ,a.description as [campaign_description]
        ,a.created_time as [last_response_time]
        ,a.created_by as [agent_id]
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
		,a.start_date
		,a.end_date
		,a.admin_assigned_by
		,a.admin_assign_time
		,a.head_unit_id
		,a.head_unit_assign_time
		,a.spv_id
		,a.spv_assign_time
		,e.agent_id
		,a.agent_pickup_time
		,a.created_time
		,a.last_phone_call
		,d.last_response_time
		,a.last_agent_notes
        ,b.[address]
        ,b.[agent]
        ,b.[supplydate]
        ,b.[customername]
        ,b.[recall_category]
        ,b.[phone]
        ,b.[model]
        ,b.[documentid]
        ,b.[platnum]
        ,b.[predict_pkbdate]
        ,b.[dealercode]
        ,b.[dealerdesc]
        ,b.[equipmentno]
		,c.campaign_id
		,c.[konfirm_program_ssc_daihatsu]
        ,c.[konfrim_pemilik_unit_daihatsu]
        ,c.[q0]
        ,c.[q1_penjelasan_program_ssc]
        ,c.[q2_penjelasan_program_ssc]
        ,c.[q3_konfrim_cus_ttg_program]
        ,c.[q4_konfirm_program_ecu]
        ,c.[q4a]
        ,c.[q5_konf_buat_jadwal_di_bengkel]
        ,c.[q6_konf_cust_tgl_service]
        ,c.[q6a_konf_cust_jam_service]
        ,c.[q6b_lokasi_bengkel]
        ,c.[q7_alasan_tdk_pemeriksaan]
        ,c.[q7a_mapping_bengkel_terdekat]
        ,c.[q7b_resiko_blm_pengecekan]
        ,c.[q7c_no_pelanggan_rental]
        ,c.[q7d_alasan_lain]
        ,c.[q8_confrim_ketersediaan_waktu]
        ,c.[q9_closing]
        ,c.[q9a_closing]
        ,c.[q9b_closing]
		,d.[connect_response]
		,d.[contact_response]
		,d.[description]
		,e.[campaign_result]
		,e.[campaign_status]
		,[attempt_call] = case 
			when d.[last_response_time] is null then null 
			else row_number() over(partition by a.id order by d.[last_response_time])
		end
		,d.[appointment_date]
		,d.[appointment_time]
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
		and d.[last_response_time] between dateadd(second, -20, e.[last_response_time]) and dateadd(second, 20, e.[last_response_time])
)
select * from final