{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_adm',
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_adm_tms_prospect_intelix') }} tp
    where main_campaign  in ('ADM013')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
        ,max(case when field_name_id in ('Address') then data_content end) as [address]
        ,max(case when field_name_id in ('Agent') then data_content end) as [agent]
        ,max(case when field_name_id in ('Booking Date') then data_content end) as [booking_date]
        ,max(case when field_name_id in ('Booking Hour') then data_content end) as [booking_hour]
        ,max(case when field_name_id in ('Cabang Booking') then data_content end) as [dealer_booking]
        ,max(case when field_name_id in ('Customer Name') then data_content end) as [customername]
        ,max(case when field_name_id in ('Home Phone') then data_content end) as [homephone]
        ,max(case when field_name_id in ('Kategori Recall') then data_content end) as [recall_category]
        ,max(case when field_name_id in ('Model') then data_content end) as [model]
        ,max(case when field_name_id in ('Parameter1') then data_content end) as [documentid]
        ,max(case when field_name_id in ('Phone') then data_content end) as [phone]
        ,max(case when field_name_id in ('Plat Number') then data_content end) as [platnum]
        ,max(case when field_name_id in ('VIN') then data_content end) as [equipmentno]
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
        ,max(case when field_name_id in ('q1_intro_to_cust') then data_content end) as [q1_intro_to_cust]
        ,max(case when field_name_id in ('q2_pemeriksaan_ecu_mesin') then data_content end) as [q2_pemeriksaan_ecu_mesin]
        ,max(case when field_name_id in ('q2a_konf_sdh_repair') then data_content end) as [q2a_konf_sdh_repair]
        ,max(case when field_name_id in ('q2b_konf_tdk_ada_waktu_bengkel') then data_content end) as [q2b_konf_tdk_ada_waktu_bengkel]
        ,max(case when field_name_id in ('q2c_konf_lain-lain') then data_content end) as [q2c_konf_lain-lain]
        ,max(case when field_name_id in ('q3_konf_cust_tgl_service') then data_content end) as [q3_konf_cust_tgl_service]
        ,max(case when field_name_id in ('q3a_reschedule') then data_content end) as [q3a_reschedule]
        ,max(case when field_name_id in ('q3b_lokasi_bengkel') then data_content end) as [q3b_lokasi_bengkel]
        ,max(case when field_name_id in ('q3c_reschedule') then data_content end) as [q3c_reschedule]
        ,max(case when field_name_id in ('q4_penutup') then data_content end) as [q4_penutup]
        ,max(case when field_name_id in ('q4a_penutup') then data_content end) as [q4a_penutup]
        ,max(case when field_name_id in ('q4b_penutup') then data_content end) as [q4b_penutup]
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
		,row_number()over(partition by a.prospect_id order by a.created_time desc) as sort
		,[attempt_call] = count(a.created_time) over(partition by a.prospect_id)
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
		,a.[sort]
		,a.[attempt_call]
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
		,row_number() over(partition by a.prospect_id, a.campaign_id order by a.created_time desc) as sort
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
		,a.agent_id
		,a.agent_pickup_time
		,a.created_time
		,a.last_phone_call
		,a.last_response_time
		,a.last_agent_notes
        ,b.[address]
        ,b.[agent]
        ,b.[booking_date]
        ,b.[booking_hour]
        ,b.[dealer_booking]
        ,b.[customername]
        ,b.[homephone]
        ,b.[recall_category]
        ,b.[model]
        ,b.[documentid]
        ,b.[phone]
        ,b.[platnum]
        ,b.[equipmentno]
		,c.campaign_id
        ,c.[q1_intro_to_cust]
        ,c.[q2_pemeriksaan_ecu_mesin]
        ,c.[q2a_konf_sdh_repair]
        ,c.[q2b_konf_tdk_ada_waktu_bengkel]
        ,c.[q2c_konf_lain-lain]
        ,c.[q3_konf_cust_tgl_service]
        ,c.[q3a_reschedule]
        ,c.[q3b_lokasi_bengkel]
        ,c.[q3c_reschedule]
        ,c.[q4_penutup]
        ,c.[q4a_penutup]
        ,c.[q4b_penutup]
		,d.[connect_response]
		,d.[contact_response]
		,d.[description]
		,e.[campaign_result]
		,e.[campaign_status]
		,d.[attempt_call]
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
		and d.sort = 1
	left join time_frame e 
        on a.id = e.prospect_id 
		and c.campaign_id = e.campaign_id
		and e.sort = 1
)
select * from final