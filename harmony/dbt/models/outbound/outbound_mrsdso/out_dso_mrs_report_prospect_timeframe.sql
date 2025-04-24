{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id', '"Attempt Call"'],
        group='outbound_mrsdso',
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_intelix') }} tp
    where main_campaign = 'MRSDSO'
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
        ,max(case when field_name_id = '01 PARTNER ID' then data_content end) as [Partner ID]
        ,max(case when field_name_id = '02 NAMA CP' then data_content end) as [Nama CP]
        ,max(case when field_name_id = '03 PHONE CP' then data_content end) as [Phone CP]
        ,max(case when field_name_id = '04 NAMA DM' then data_content end) as [Nama DM]
        ,max(case when field_name_id = '05 PHONE DM' then data_content end) as [Phone DM]
        ,max(case when field_name_id = '06 NAMA STP' then data_content end) as [Nama STP]
        ,max(case when field_name_id = '07 PHONE STP' then data_content end) as [Phone STP]
        ,max(case when field_name_id = '08 ADDRESS' then data_content end) as [Address]
        ,max(case when field_name_id = '09 CITY' then data_content end) as [City]
        ,max(case when field_name_id = '10 PIC_1' then data_content end) as [PIC_1]
        ,max(case when field_name_id = '11 TLP_PIC_1' then data_content end) as [TLP_PIC_1]
        ,max(case when field_name_id = '12 PIC_2' then data_content end) as [PIC_2]
        ,max(case when field_name_id = '13 TLP_PIC_2' then data_content end) as [TLP_PIC_2]
        ,max(case when field_name_id = '14 REGION' then data_content end) as [Region]
        ,max(case when field_name_id = '16 PLAT NUMBER' then data_content end) as [Plat Number]
        ,max(case when field_name_id = '17 MODEL' then data_content end) as [Model]
        ,max(case when field_name_id = '18 INVOICE DATE' then data_content end) as [Invoice Date]
        ,max(case when field_name_id = '19 LastServiceDate' then data_content end) as [LastServiceDate]
        ,max(case when field_name_id = '20 LastPlant' then data_content end) as [LastPlant]
        ,max(case when field_name_id = '21 BRANCH DESCRIPTION' then data_content end) as [Branch Description]
        ,max(case when field_name_id = '22 PERIODE' then data_content end) as [Periode]
        ,max(case when field_name_id = '23 Next PM DATE' then data_content end) as [Next PM Date]
        ,max(case when field_name_id = '24 NEXT PM KM' then data_content end) as [Next PM KM]
        ,max(case when field_name_id = '26 SA NAME' then data_content end) as [SA Name]
        ,max(case when field_name_id = '27 SA CODE' then data_content end) as [SA Code]
        ,max(case when field_name_id = '28 STNK DATE' then data_content end) as [STNK Date]
        ,max(case when field_name_id = '29 NAME ON STNK' then data_content end) as [Name on STNK]
        ,max(case when field_name_id = '30 ProductID' then data_content end) as [ProductID]
        ,max(case when field_name_id = '31 PLANT' then data_content end) as [Plant]
        ,max(case when field_name_id = '32 SOURCE DESCRIPTION' then data_content end) as [Source Description]
        ,max(case when field_name_id = '33 TITLE' then data_content end) as [Title]
        ,max(case when field_name_id = 'agent' then data_content end) as [Agent]
        ,max(case when field_name_id = 'flag_supply' then data_content end) as [Flag Supply]
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_detail_intelix') }} pd
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
        ,max(case when field_name = '10a Service dimana' then data_content end) as [10a Service dimana]
        ,max(case when field_name = '10b Service kapan' then data_content end) as [10b Service kapan]
        ,max(case when field_name = '10c KM saat service' then data_content end) as [10c KM saat service]
        ,max(case when field_name = 'Bersedia di reminder next PM' then data_content end) as [Bersedia di reminder next PM]
        ,max(case when field_name = 'Booking Date' then data_content end) as [Booking Date]
        ,max(case when field_name = 'Branch Name' then data_content end) as [Branch Name]
        ,max(case when field_name = 'Catatan SA u Service PM' then data_content end) as [Catatan SA u Service PM]
        ,max(case when field_name = 'Catatan Tambahan' then data_content end) as [Catatan Tambahan]
        ,max(case when field_name = 'Jika Sudah Service' then data_content end) as [Jika Sudah Service]
        ,max(case when field_name = 'Jika tdk mau kebengkel yg sama' then data_content end) as [Jika tdk mau kebengkel yg sama]
        ,max(case when field_name = 'Jika Tidak Perlu Booking' then data_content end) as [Jika Tidak Perlu Booking]
        ,max(case when field_name = 'Keluhan Pada Kendaraan' then data_content end) as [Keluhan Pada Kendaraan]
        ,max(case when field_name = 'Periode Service' then data_content end) as [Periode Service]
        ,max(case when field_name = 'Perkiraan Waktu Service' then data_content end) as [Perkiraan Waktu Service]
        ,max(case when field_name = 'Reason Not Booked' then data_content end) as [Reason Not Booked]
        ,max(case when field_name = 'Reason Not Booked Lainnya' then data_content end) as [Reason Not Booked Lainnya]
        ,max(case when field_name = 'Service Type' then data_content end) as [Service Type]
        ,max(case when field_name = 'Type Of Booking' then data_content end) as [Type Of Booking]
        ,max(case when field_name = 'V1 Note Reason Not Booked' then data_content end) as [V1 Note Reason Not Booked]
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_campaign_result_intelix') }} cr
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
		-- ,a.notes as Description
		,a.appointmentDate as [Appointment Date]
		,a.appointmentTime as [Appointment Time]
		,a.created_time
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_history_contact_intelix') }} a
    where a.file_id in (select file_id from prospect)
)
,map_lov as (
    select 
        a.prospect_id
		,b.[name] as [Connect Response]
		,c.[name] as [Contact Response]
		-- ,a.[Description]
		,a.[Appointment Date]
		,a.[Appointment Time]
		,a.[created_time] as [Last Response Time]
    from history_contact a
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_cc_master_category_intelix') }} b
        ON a.LOV1 = b.id
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_cc_master_category_intelix') }} c
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_time_frame_intelix') }} a
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
		,b.[Partner ID]
        ,b.[Nama CP]
        ,b.[Phone CP]
        ,b.[Nama DM]
        ,b.[Phone DM]
        ,b.[Nama STP]
        ,b.[Phone STP]
        ,b.[Address]
        ,b.[City]
        ,b.[PIC_1]
        ,b.[TLP_PIC_1]
        ,b.[PIC_2]
        ,b.[TLP_PIC_2]
        ,b.[Region]
        ,b.[Plat Number]
        ,b.[Model]
        ,b.[Invoice Date]
        ,b.[LastServiceDate]
        ,b.[LastPlant]
        ,b.[Branch Description]
        ,b.[Periode]
        ,b.[Next PM Date]
        ,b.[Next PM KM]
        ,b.[SA Name]
        ,b.[SA Code]
        ,b.[STNK Date]
        ,b.[Name on STNK]
        ,b.[ProductID]
        ,b.[Plant]
        ,b.[Source Description]
        ,b.[Title]
        ,b.[Agent]
        ,b.[Flag Supply]
		,c.campaign_id
        ,c.[10a Service dimana]
        ,c.[10b Service kapan]
        ,c.[10c KM saat service]
        ,c.[Bersedia di reminder next PM]
        ,[Booking Date] = try_convert(date, replace(c.[Booking Date], 'T', ' '))
        ,[Booking Hour] = format(try_convert(datetime, replace(c.[Booking Date], 'T', ' ')), 'HH:mm')
        ,c.[Branch Name]
        ,c.[Catatan SA u Service PM]
        ,c.[Catatan Tambahan]
        ,c.[Jika Sudah Service]
        ,c.[Jika tdk mau kebengkel yg sama]
        ,c.[Jika Tidak Perlu Booking]
        ,c.[Keluhan Pada Kendaraan]
        ,c.[Periode Service]
        ,c.[Perkiraan Waktu Service]
        ,c.[Reason Not Booked]
        ,c.[Reason Not Booked Lainnya]
        ,c.[Service Type]
        ,c.[Type Of Booking]
        ,c.[V1 Note Reason Not Booked]
		,d.[Connect Response]
		,d.[Contact Response]
		-- ,d.[Description]
		,e.[Campaign Result]
		,e.[Campaign Status]
        ,e.[Campaign Description]
		,d.[Last Response Time]
		,e.[Agent Id]
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