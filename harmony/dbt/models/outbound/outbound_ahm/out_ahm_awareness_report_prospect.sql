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
    where main_campaign in ('003', '009')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
		,max(case when field_name_id = 'No' then data_content end) as [No]
		,max(case when field_name_id = 'vframe_no' then data_content end) as [vframe_no]
		,max(case when field_name_id = 'dsales_date' then data_content end) as [dsales_date]
		,max(case when field_name_id = 'vkode_mesin' then data_content end) as [vkode_mesin]
		,max(case when field_name_id = 'vsequence_mesin' then data_content end) as [vsequence_mesin]
		,max(case when field_name_id = 'vtype_motor' then data_content end) as [vtype_motor]
		,max(case when field_name_id in ('vcustomer_type','vkode_customer') then data_content end) as [vcustomer_type]
		,max(case when field_name_id = 'vnama_pemilik' then data_content end) as [vnama_pemilik]
		,max(case when field_name_id = 'valamat_surat' then data_content end) as [valamat_surat]
		,max(case when field_name_id = 'vkelurahan_surat' then data_content end) as [vkelurahan_surat]
		,max(case when field_name_id = 'vkecamatan_surat' then data_content end) as [vkecamatan_surat]
		,max(case when field_name_id = 'vkota_surat' then data_content end) as [vkota_surat]
		,max(case when field_name_id = 'vkode_pos' then data_content end) as [vkode_pos]
		,max(case when field_name_id = 'vpropinsi' then data_content end) as [vpropinsi]
		,max(case when field_name_id = 'vno_hp' then data_content end) as [vno_hp]
		,max(case when field_name_id = 'MSISDN' then data_content end) as [MSISDN]
		,max(case when field_name_id in ('vmain_dealer','vkode_md') then data_content end) as [vmain_dealer]
		,max(case when field_name_id = 'vdealer_code' then data_content end) as [vdealer_code]
		,max(case when field_name_id = 'vkode_sales_person' then data_content end) as [vkode_sales_person]
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
		,max(case when field_name_id in ('q1') then data_content end) as [q1]
		,max(case when field_name_id in ('q2', 'q1_mengetahui_honda_care') then data_content end) as [q2]
		,max(case when field_name_id in ('q3') then data_content end) as [q3]
		,max(case when field_name_id in ('q4') then data_content end) as [q4]
		,max(case when field_name_id in ('q5', 'q1a_drmn_mengetahui_honda_care') then data_content end) as [q5]
		,max(case when field_name_id in ('q5a') then data_content end) as [q5a]
		,max(case when field_name_id in ('q6', 'q3_pernah_mengalami_mogok') then data_content end) as [q6]
		,max(case when field_name_id in ('q7', 'q4_dilakukan_saat_mogok_di_jln') then data_content end) as [q7]
		,max(case when field_name_id in ('q8', 'q5_mengetahui_cr_hub_hondacare') then data_content end) as [q8]
		,max(case when field_name_id in ('q8a', 'q5a_biasanya_menghub_kemana') then data_content end) as [q8a]
		,max(case when field_name_id in ('q9', 'q6_jk_mogok_gunakan_hondacare') then data_content end) as [q9]
		,max(case when field_name_id = 'q1b_infoi_layanan_honda_care' then data_content end) as [old_q1b]
		,max(case when field_name_id = 'q2_apakah_penting_honda_care' then data_content end) as [old_q2]
		-- ,case when field_name_id = 'q1_mengetahui_honda_care' then data_content end as [q1]
		-- ,case when field_name_id = 'q1a_drmn_mengetahui_honda_care' then data_content end as [q1a]
		-- ,case when field_name_id = 'q1b_infoi_layanan_honda_care' then data_content end as [q1b]
		-- ,case when field_name_id = 'q2_apakah_penting_honda_care' then data_content end as [q2]
		-- ,case when field_name_id = 'q3_pernah_mengalami_mogok' then data_content end as [q3]
		-- ,case when field_name_id = 'q4_dilakukan_saat_mogok_di_jln' then data_content end as [q4]
		-- ,case when field_name_id = 'q5_mengetahui_cr_hub_hondacare' then data_content end as [q5]
		-- ,case when field_name_id = 'q5a_biasanya_menghub_kemana' then data_content end as [q5a]
		-- ,case when field_name_id = 'q6_jk_mogok_gunakan_hondacare' then data_content end as [q6]
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
		,b.[No]
		,b.vframe_no
		,b.dsales_date
		,b.vkode_mesin
		,b.vsequence_mesin
		,b.vtype_motor
		,b.vcustomer_type
		,b.vnama_pemilik
		,b.valamat_surat
		,b.vkelurahan_surat
		,b.vkecamatan_surat
		,b.vkota_surat
		,b.vkode_pos
		,b.vpropinsi
		,b.vno_hp
		,b.MSISDN
		,b.vmain_dealer
		,b.vdealer_code
		,b.vkode_sales_person
		,c.campaign_id
		,c.[q1]
		,c.[q2]
		,c.[q3]
		,c.[q4]
		,c.[q5]
		,c.[q5a]
		,c.[q6]
		,c.[q7]
		,c.[q8]
		,c.[q8a]
		,c.[q9]
		,c.[old_q1b]
		,c.[old_q2]
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