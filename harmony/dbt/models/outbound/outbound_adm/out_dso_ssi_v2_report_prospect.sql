{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_adm',
		tags=['outbound']
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
    where main_campaign  in ('ADM006')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
        ,max(case when field_name_id in ('01 Gender') then data_content end) as [01 Gender]
        ,max(case when field_name_id in ('02 CustomerName') then data_content end) as [02 CustomerName]
        ,max(case when field_name_id in ('03 Address') then data_content end) as [03 Address]
        ,max(case when field_name_id in ('04 City') then data_content end) as [04 City]
        ,max(case when field_name_id in ('05 UnitType') then data_content end) as [05 UnitType]
        ,max(case when field_name_id in ('06 ChasisNumber') then data_content end) as [06 ChasisNumber]
        ,max(case when field_name_id in ('08 Purchase/DeliveryDate') then data_content end) as [08 Purchase/DeliveryDate]
        ,max(case when field_name_id in ('10 SalesmanName') then data_content end) as [10 SalesmanName]
        ,max(case when field_name_id in ('11 TypeCustomer') then data_content end) as [11 TypeCustomer]
        ,max(case when field_name_id in ('13 PaymentType') then data_content end) as [13 PaymentType]
        ,max(case when field_name_id in ('14 AddressAlternative') then data_content end) as [14 AddressAlternative]
        ,max(case when field_name_id in ('15 CodeArea') then data_content end) as [15 CodeArea]
        ,max(case when field_name_id in ('16 DealerCode') then data_content end) as [16 DealerCode]
        ,max(case when field_name_id in ('17 DealerName') then data_content end) as [17 DealerName]
        ,max(case when field_name_id in ('18 DealerCity') then data_content end) as [18 DealerCity]
        ,max(case when field_name_id in ('19 MSISDN') then data_content end) as [19 MSISDN]
        ,max(case when field_name_id in ('21 Handphone') then data_content end) as [21 Handphone]
        ,max(case when field_name_id in ('22 Email') then data_content end) as [22 Email]
        ,max(case when field_name_id in ('23 FaxNo') then data_content end) as [23 FaxNo]
        ,max(case when field_name_id in ('Agent ID') then data_content end) as [Agent ID]
        ,max(case when field_name_id in ('parameter1') then data_content end) as [parameter1]
        ,max(case when field_name_id in ('SupplyPeriod') then data_content end) as [SupplyPeriod]
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
		,max(case when field_name_id in ('q10_pengisian_form') then data_content end) as [q10_pengisian_form]
        ,max(case when field_name_id in ('q11_kepuasan_delivery') then data_content end) as [q11_kepuasan_delivery]
        ,max(case when field_name_id in ('q11a') then data_content end) as [q11a]
        ,max(case when field_name_id in ('q12_follow_up') then data_content end) as [q12_follow_up]
        ,max(case when field_name_id in ('q13_fasilitas_showroom') then data_content end) as [q13_fasilitas_showroom]
        ,max(case when field_name_id in ('q14_feeback') then data_content end) as [q14_feeback]
        ,max(case when field_name_id in ('q1_attitude_salesman') then data_content end) as [q1_attitude_salesman]
        ,max(case when field_name_id in ('q1a') then data_content end) as [q1a]
        ,max(case when field_name_id in ('q2_knowledge_g_skill_salesman') then data_content end) as [q2_knowledge_g_skill_salesman]
        ,max(case when field_name_id in ('q2a') then data_content end) as [q2a]
        ,max(case when field_name_id in ('q3_grooming_salesman') then data_content end) as [q3_grooming_salesman]
        ,max(case when field_name_id in ('q3a') then data_content end) as [q3a]
        ,max(case when field_name_id in ('q4_test_drive') then data_content end) as [q4_test_drive]
        ,max(case when field_name_id in ('q5_proses_administrasi') then data_content end) as [q5_proses_administrasi]
        ,max(case when field_name_id in ('q5a') then data_content end) as [q5a]
        ,max(case when field_name_id in ('q6_delivery_timing') then data_content end) as [q6_delivery_timing]
        ,max(case when field_name_id in ('q6a') then data_content end) as [q6a]
        ,max(case when field_name_id in ('q7_delivery_by_salesman') then data_content end) as [q7_delivery_by_salesman]
        ,max(case when field_name_id in ('q8_kondisi_kendaraan') then data_content end) as [q8_kondisi_kendaraan]
        ,max(case when field_name_id in ('q8a') then data_content end) as [q8a]
        ,max(case when field_name_id in ('q9_proses_dec') then data_content end) as [q9_proses_dec]
        ,max(case when field_name_id in ('s1_konfirmasi_kendaraan') then data_content end) as [s1_konfirmasi_kendaraan]
        ,max(case when field_name_id in ('s2_konfirmasi_nama_sales') then data_content end) as [s2_konfirmasi_nama_sales]
        ,max(case when field_name_id in ('s3_konfirmasi_penerimaan_kenda') then data_content end) as [s3_konfirmasi_penerimaan_kenda]
        ,max(case when field_name_id in ('s4_konfirmasi_keterlibatan_cus') then data_content end) as [s4_konfirmasi_keterlibatan_cus]
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
		,row_number()over(partition by a.prospect_id order by a.created_time desc) as Sort
		,[Attempt Call] = count(a.created_time) over(partition by a.prospect_id)
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
		,a.[Sort]
		,a.[Attempt Call]
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
		,row_number() over(partition by a.prospect_id, a.campaign_id order by a.created_time desc) as Sort
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
		,b.[01 Gender]
        ,b.[02 CustomerName]
        ,b.[03 Address]
        ,b.[04 City]
        ,b.[05 UnitType]
        ,b.[06 ChasisNumber]
        ,b.[08 Purchase/DeliveryDate]
        ,b.[10 SalesmanName]
        ,b.[11 TypeCustomer]
        ,b.[13 PaymentType]
        ,b.[14 AddressAlternative]
        ,b.[15 CodeArea]
        ,b.[16 DealerCode]
        ,b.[17 DealerName]
        ,b.[18 DealerCity]
        ,b.[19 MSISDN]
        ,b.[21 Handphone]
        ,b.[22 Email]
        ,b.[23 FaxNo]
        ,b.[Agent ID]
        ,b.[parameter1]
        ,b.[SupplyPeriod]
        ,b.[supplyweek]
		,c.campaign_id
        ,c.[q1_attitude_salesman]
        ,c.[q1a]
        ,c.[q2_knowledge_g_skill_salesman]
        ,c.[q2a]
        ,c.[q3_grooming_salesman]
        ,c.[q3a]
        ,c.[q4_test_drive]
        ,c.[q5_proses_administrasi]
        ,c.[q5a]
        ,c.[q6_delivery_timing]
        ,c.[q6a]
        ,c.[q7_delivery_by_salesman]
        ,c.[q8_kondisi_kendaraan]
        ,c.[q8a]
        ,c.[q9_proses_dec]
		,c.[q10_pengisian_form]
        ,c.[q11_kepuasan_delivery]
        ,c.[q11a]
        ,c.[q12_follow_up]
        ,c.[q13_fasilitas_showroom]
        ,c.[q14_feeback]
        ,c.[s1_konfirmasi_kendaraan]
        ,c.[s2_konfirmasi_nama_sales]
        ,c.[s3_konfirmasi_penerimaan_kenda]
        ,c.[s4_konfirmasi_keterlibatan_cus]
		,d.[Connect Response]
		,d.[Contact Response]
		,d.[Description]
		,e.[Campaign Result]
		,e.[Campaign Status]
		,e.[Campaign Description]
		,d.[Last Response Time]
		-- ,e.[Agent Id]
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