{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
        unique_key='id',
        group='outbound_mrsdso'
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
    	and modified_time >= convert(date, getdate()-1)
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
        ,case when field_name_id = '01 PARTNER ID' then data_content end as [Partner ID]
        ,case when field_name_id = '02 NAMA CP' then data_content end as [Nama CP]
        ,case when field_name_id = '03 PHONE CP' then data_content end as [Phone CP]
        ,case when field_name_id = '04 NAMA DM' then data_content end as [Nama DM]
        ,case when field_name_id = '05 PHONE DM' then data_content end as [Phone DM]
        ,case when field_name_id = '06 NAMA STP' then data_content end as [Nama STP]
        ,case when field_name_id = '07 PHONE STP' then data_content end as [Phone STP]
        ,case when field_name_id = '08 ADDRESS' then data_content end as [Address]
        ,case when field_name_id = '09 CITY' then data_content end as [City]
        ,case when field_name_id = '10 PIC_1' then data_content end as [PIC_1]
        ,case when field_name_id = '11 TLP_PIC_1' then data_content end as [TLP_PIC_1]
        ,case when field_name_id = '12 PIC_2' then data_content end as [PIC_2]
        ,case when field_name_id = '13 TLP_PIC_2' then data_content end as [TLP_PIC_2]
        ,case when field_name_id = '14 REGION' then data_content end as [Region]
        ,case when field_name_id = '16 PLAT NUMBER' then data_content end as [Plat Number]
        ,case when field_name_id = '17 MODEL' then data_content end as [Model]
        ,case when field_name_id = '18 INVOICE DATE' then data_content end as [Invoice Date]
        ,case when field_name_id = '19 LastServiceDate' then data_content end as [LastServiceDate]
        ,case when field_name_id = '20 LastPlant' then data_content end as [LastPlant]
        ,case when field_name_id = '21 BRANCH DESCRIPTION' then data_content end as [Branch Description]
        ,case when field_name_id = '22 PERIODE' then data_content end as [Periode]
        ,case when field_name_id = '23 Next PM DATE' then data_content end as [Next PM Date]
        ,case when field_name_id = '24 NEXT PM KM' then data_content end as [Next PM KM]
        ,case when field_name_id = '26 SA NAME' then data_content end as [SA Name]
        ,case when field_name_id = '27 SA CODE' then data_content end as [SA Code]
        ,case when field_name_id = '28 STNK DATE' then data_content end as [STNK Date]
        ,case when field_name_id = '29 NAME ON STNK' then data_content end as [Name on STNK]
        ,case when field_name_id = '30 ProductID' then data_content end as [ProductID]
        ,case when field_name_id = '31 PLANT' then data_content end as [Plant]
        ,case when field_name_id = '32 SOURCE DESCRIPTION' then data_content end as [Source Description]
        ,case when field_name_id = '33 TITLE' then data_content end as [Title]
        ,case when field_name_id = 'agent' then data_content end as [Agent]
        ,case when field_name_id = 'flag_supply' then data_content end as [Flag Supply]
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_detail_intelix') }} pd
    where exists (
    	select file_id 
    	from prospect p
    	where pd.file_id = p.file_id
	)
)
,prospect_detail_transpose as (
    select 
        prospect_id
        ,max([Partner ID]) as [Partner ID]
        ,max([Nama CP]) as [Nama CP]
        ,max([Phone CP]) as [Phone CP]
        ,max([Nama DM]) as [Nama DM]
        ,max([Phone DM]) as [Phone DM]
        ,max([Nama STP]) as [Nama STP]
        ,max([Phone STP]) as [Phone STP]
        ,max([Address]) as [Address]
        ,max([City]) as [City]
        ,max([PIC_1]) as [PIC_1]
        ,max([TLP_PIC_1]) as [TLP_PIC_1]
        ,max([PIC_2]) as [PIC_2]
        ,max([TLP_PIC_2]) as [TLP_PIC_2]
        ,max([Region]) as [Region]
        ,max([Plat Number]) as [Plat Number]
        ,max([Model]) as [Model]
        ,max([Invoice Date]) as [Invoice Date]
        ,max([LastServiceDate]) as [LastServiceDate]
        ,max([LastPlant]) as [LastPlant]
        ,max([Branch Description]) as [Branch Description]
        ,max([Periode]) as [Periode]
        ,max([Next PM Date]) as [Next PM Date]
        ,max([Next PM KM]) as [Next PM KM]
        ,max([SA Name]) as [SA Name]
        ,max([SA Code]) as [SA Code]
        ,max([STNK Date]) as [STNK Date]
        ,max([Name on STNK]) as [Name on STNK]
        ,max([ProductID]) as [ProductID]
        ,max([Plant]) as [Plant]
        ,max([Source Description]) as [Source Description]
        ,max([Title]) as [Title]
        ,max([Agent]) as [Agent]
        ,max([Flag Supply]) as [Flag Supply]
    from prospect_detail 
    group by prospect_id
)
,campaign_result as (
    select 
        prospect_id 
        ,campaign_id 
        ,file_id
        ,case when field_name = '10a Service dimana' then data_content end as [10a Service dimana]
        ,case when field_name = '10b Service kapan' then data_content end as [10b Service kapan]
        ,case when field_name = '10c KM saat service' then data_content end as [10c KM saat service]
        ,case when field_name = 'Bersedia di reminder next PM' then data_content end as [Bersedia di reminder next PM]
        ,case when field_name = 'Booking Date' then data_content end as [Booking Date]
        ,case when field_name = 'Branch Name' then data_content end as [Branch Name]
        ,case when field_name = 'Catatan SA u Service PM' then data_content end as [Catatan SA u Service PM]
        ,case when field_name = 'Catatan Tambahan' then data_content end as [Catatan Tambahan]
        ,case when field_name = 'Jika Sudah Service' then data_content end as [Jika Sudah Service]
        ,case when field_name = 'Jika tdk mau kebengkel yg sama' then data_content end as [Jika tdk mau kebengkel yg sama]
        ,case when field_name = 'Jika Tidak Perlu Booking' then data_content end as [Jika Tidak Perlu Booking]
        ,case when field_name = 'Keluhan Pada Kendaraan' then data_content end as [Keluhan Pada Kendaraan]
        ,case when field_name = 'Periode Service' then data_content end as [Periode Service]
        ,case when field_name = 'Perkiraan Waktu Service' then data_content end as [Perkiraan Waktu Service]
        ,case when field_name = 'Reason Not Booked' then data_content end as [Reason Not Booked]
        ,case when field_name = 'Reason Not Booked Lainnya' then data_content end as [Reason Not Booked Lainnya]
        ,case when field_name = 'Service Type' then data_content end as [Service Type]
        ,case when field_name = 'Type Of Booking' then data_content end as [Type Of Booking]
        ,case when field_name = 'V1 Note Reason Not Booked' then data_content end as [V1 Note Reason Not Booked]
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrsdso_tms_prospect_campaign_result_intelix') }} cr
	where exists (
    	select file_id 
    	from prospect p
    	where cr.file_id = p.file_id
	)  
)
,campaign_result_transpose as (
    select 
        prospect_id
        ,campaign_id
        ,file_id
        ,max([10a Service dimana]) as [10a Service dimana]
        ,max([10b Service kapan]) as [10b Service kapan]
        ,max([10c KM saat service]) as [10c KM saat service]
        ,max([Bersedia di reminder next PM]) as [Bersedia di reminder next PM]
        ,max([Booking Date]) as [Booking Date]
        ,max([Branch Name]) as [Branch Name]
        ,max([Catatan SA u Service PM]) as [Catatan SA u Service PM]
        ,max([Catatan Tambahan]) as [Catatan Tambahan]
        ,max([Jika Sudah Service]) as [Jika Sudah Service]
        ,max([Jika tdk mau kebengkel yg sama]) as [Jika tdk mau kebengkel yg sama]
        ,max([Jika Tidak Perlu Booking]) as [Jika Tidak Perlu Booking]
        ,max([Keluhan Pada Kendaraan]) as [Keluhan Pada Kendaraan]
        ,max([Periode Service]) as [Periode Service]
        ,max([Perkiraan Waktu Service]) as [Perkiraan Waktu Service]
        ,max([Reason Not Booked]) as [Reason Not Booked]
        ,max([Reason Not Booked Lainnya]) as [Reason Not Booked Lainnya]
        ,max([Service Type]) as [Service Type]
        ,max([Type Of Booking]) as [Type Of Booking]
        ,max([V1 Note Reason Not Booked]) as [V1 Note Reason Not Booked]
    from campaign_result
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
		,row_number() over(partition by a.prospect_id order by a.created_time desc) as Sort
		,[Attempt Call] = count(a.created_time) over(partition by a.prospect_id)
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
		,a.[Sort]
		,a.[Attempt Call]
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
        ,row_number() over(partition by a.prospect_id, a.campaign_id order by a.created_time desc) as Sort
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
		,d.[Attempt Call]
		,d.[Appointment Date]
		,d.[Appointment Time]
		,uploaddate = getdate()
	from prospect a
	left join prospect_detail_transpose b 
        on a.id = b.prospect_id
	left join campaign_result_transpose c 
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