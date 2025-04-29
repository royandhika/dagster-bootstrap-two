{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_esvi',
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_esvi_tms_prospect_intelix') }} tp
    where main_campaign in ('ESVI01', 'ESVI02', 'ESVI03', 'ESVI04', 'ESVI05', 'ESVI06')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
		,max(case when field_name_id IN ('01. PIN','PIN') then data_content end) as [PIN]
		,max(case when field_name_id IN ('02. Sumber Data', 'Sumber Data') then data_content end) as [Sumber Data]
		,max(case when field_name_id IN ('03. Titel', 'Titel') then data_content end) as [Titel]
		,max(case when field_name_id IN ('04. AccountID', 'AccountID') then data_content end) as [AccountID]
		,max(case when field_name_id IN ('05. Account', 'Account') then data_content end) as [Account]
		,max(case when field_name_id IN ('06. IsAWO', 'Account') then data_content end) as [IsAWO]
		,max(case when field_name_id IN ('07. MobilePhone2', 'MobilePhone2') then data_content end) as [MobilePhone2]
		,max(case when field_name_id IN ('08. HomePhone', 'HomePhone') then data_content end) as [HomePhone]
		,max(case when field_name_id IN ('09. OfficePhone', 'OfficePhone') then data_content end) as [OfficePhone]
		,max(case when field_name_id IN ('10. name_stp', 'name_stp') then data_content end) as [name_stp]
		,max(case when field_name_id IN ('11. hp_stp', 'hp_stp') then data_content end) as [hp_stp]
		,max(case when field_name_id IN ('12. name_cp', 'name_cp') then data_content end) as [name_cp]
		,max(case when field_name_id IN ('13. hp_cp', 'hp_cp') then data_content end) as [hp_cp]
		,max(case when field_name_id IN ('14. name_dm', 'name_dm') then data_content end) as [name_dm]
		,max(case when field_name_id IN ('15. hp_dm', 'hp_dm') then data_content end) as [hp_dm]
		,max(case when field_name_id IN ('16. Street', 'Street') then data_content end) as [Street]
		,max(case when field_name_id IN ('17. City', 'City') then data_content end) as [City]
		,max(case when field_name_id IN ('18. PostalCode', 'PostalCode') then data_content end) as [PostalCode]
		,max(case when field_name_id IN ('19. Region', 'Region') then data_content end) as [Region]
		,max(case when field_name_id IN ('20. MSISDN', 'MSISDN') then data_content end) as [MSISDN]
		,max(case when field_name_id IN ('21. VIN', 'VIN') then data_content end) as [VIN]
		,max(case when field_name_id IN ('22. PlatNum', 'PlatNum') then data_content end) as [PlatNum]
		,max(case when field_name_id IN ('23. InvoiceDate', 'InvoiceDate') then data_content end) as [InvoiceDate]
		,max(case when field_name_id IN ('24. Model', 'Model') then data_content end) as [Model]
		,max(case when field_name_id IN ('25. ModelYear', 'ModelYear') then data_content end) as [ModelYear]
		,max(case when field_name_id IN ('26. Brand', 'Brand') then data_content end) as [Brand]
		,max(case when field_name_id IN ('27. RegValidFrom', 'RegValidFrom') then data_content end) as [RegValidFrom]
		,max(case when field_name_id IN ('28. RegValidTo', 'RegValidTo') then data_content end) as [RegValidTo]
		,max(case when field_name_id IN ('29. RegStatus', 'RegStatus') then data_content end) as [RegStatus]
		,max(case when field_name_id IN ('30. LastServiceDate', 'LastServiceDate') then data_content end) as [LastServiceDate]
		,max(case when field_name_id IN ('31. SourceCode', 'SourceCode') then data_content end) as [SourceCode]
		,max(case when field_name_id IN ('32. SourceDesc', 'SourceDesc') then data_content end) as [SourceDesc]
		,max(case when field_name_id IN ('33. AWO', 'AWO') then data_content end) as [AWO]
		,max(case when field_name_id IN ('34. LastTransERA', 'LastTransERA') then data_content end) as [LastTransERA]
		,max(case when field_name_id IN ('35. LastTransERADate', 'LastTransERADate') then data_content end) as [LastTransERADate]
		,max(case when field_name_id IN ('36. LastTransERAByPerson', 'LastTransERAByPerson') then data_content end) as [LastTransERAByPerson]
		,max(case when field_name_id IN ('37. LastTransERADateByPerson', 'LastTransERADateByPerson') then data_content end) as [LastTransERADateByPerson]
		,max(case when field_name_id IN ('38. LastInteraction', 'LastInteraction') then data_content end) as [LastInteraction]
		,max(case when field_name_id IN ('39. LastInteractionDate', 'LastInteractionDate') then data_content end) as [LastInteractionDate]
		,max(case when field_name_id IN ('40. Prioritas', 'Prioritas') then data_content end) as [Prioritas]
		,max(case when field_name_id IN ('41. Reply', 'Reply') then data_content end) as [Reply]
		,max(case when field_name_id IN ('42. PlatNum_Cantik', 'PlatNum_Cantik') then data_content end) as [PlatNum_Cantik]
		,max(case when field_name_id IN ('43. Flag', 'Flag') then data_content end) as [Flag]
		,max(case when field_name_id IN ('44. CoverageArea_ERA', 'CoverageArea_ERA') then data_content end) as [CoverageArea_ERA]
		,max(case when field_name_id IN ('45. Reply SMS by WA', 'Reply SMS by WA') then data_content end) as [Reply SMS by WA]
		,max(case when field_name_id IN ('46. report SMS by MSISDN', 'report SMS by MSISDN') then data_content end) as [report SMS by MSISDN]
		,max(case when field_name_id IN ('47. report SMS by PlatNum', 'report SMS by PlatNum') then data_content end) as [report SMS by PlatNum]
		,max(case when field_name_id IN ('48. Month Supply', 'Month Supply') then data_content end) as [Month Supply]
		,max(case when field_name_id IN ('49. RO', 'RO') then data_content end) as [RO]
		,max(case when field_name_id IN ('50. FlagMSISDN', 'FlagMSISDN') then data_content end) as [FlagMSISDN]
		,max(case when field_name_id IN ('51. Week', 'Week') then data_content end) as [Week]
		,max(case when field_name_id IN ('52. FLAG DATA', 'FLAG DATA') then data_content end) as [FLAG DATA]
		,max(case when field_name_id IN ('53. Note Rutin Service', 'Note Rutin Service') then data_content end) as [Note Rutin Service]
		,max(case when field_name_id IN ('54. Note Pernah Bayar EES', 'Note Pernah Bayar EES') then data_content end) as [Note Pernah Bayar EES]
		,max(case when field_name_id IN ('55. Opt_In', 'Opt_In') then data_content end) as [Opt_In]
		,max(case when field_name_id IN ('56. CCA Before', 'CCA Before') then data_content end) as [CCA Before]
		,max(case when field_name_id IN ('57. CCA', 'CCA') then data_content end) as [CCA]
		,max(case when field_name_id IN ('58. Periode_Call', 'Periode_Call') then data_content end) as [Periode_Call]
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_esvi_tms_prospect_detail_intelix') }} pd
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
        ,max(case when field_name_id = 'preferensi_harga' then data_content end) as [PREFERENSI HARGA]
		,max(case when field_name_id = 'rencana_tanggal_bayar' then data_content end) as [Rencana Tanggal Bayar]
		,max(case when field_name_id = 'connect_response_wa' then data_content end) as [CONNECT RESPONSE CALL]
		,max(case when field_name_id = 'connect_response_wa' then data_content end) as [CONNECT RESPONSE WA]
		,max(case when field_name_id = 'contacted_response_wa' then data_content end) as [CONTACT RESPONSE WA]
		,max(case when field_name_id = 'reason_interest_not_paid' then data_content end) as [Reason Interest Not Paid]
		,max(case when field_name_id = 'reason_interest_not_paid_lainn' then data_content end) as [Reason Interest Not Paid Lainnya]
		,max(case when field_name_id = 'nama_baru' then data_content end) as [Nama Baru]
		,max(case when field_name_id = 'alamat_baru' then data_content end) as [Alamat Baru]
		,max(case when field_name_id = 'plat_number_baru' then data_content end) as [Plat Number Baru]
		,max(case when field_name_id = 'mobile_phone_baru' then data_content end) as [Mobile Phone Baru]
		,max(case when field_name_id = 'nama_contact' then data_content end) as [Nama Contact]
		,max(case when field_name_id = 'alamat_email' then data_content end) as [Alamat Email]
		,max(case when field_name_id = 'notes' then data_content end) as [Notes]
		,max(case when field_name_id = 'other_request' then data_content end) as [Other Request]
		,max(case when field_name_id = 'tanggal_kfrm_mutasi_finance' then data_content end) as [Tanggal konfrimasi mutasi finance]
		,max(case when field_name_id = 'tanggal_validasi' then data_content end) as [Tanggal Validasi]
		,max(case when field_name_id = 'kfrm_jangka_wkt_pembayaran' then data_content end) as [Konfrimasi Jangka Waktu Pembayaran]
		,max(case when field_name_id = 'nominal_pembayaran' then data_content end) as [Nominal Pembayaran]
		,max(case when field_name_id = 'keterangan_konfrimasi' then data_content end) as [Keterangan Konfrimasi]
		,max(case when field_name_id = 'tanggal_update_fu' then data_content end) as [Tanggal Update FU]
		,max(case when field_name_id = 'tanggal_kirim_surat' then data_content end) as [Tanggal Kirim Surat]
		,max(case when field_name_id = 'update_di_sap' then data_content end) as [Update di SAP]
		,max(case when field_name_id = 'request_info_uees' then data_content end) as [Request Info UEES]
		,max(case when field_name_id = 'info_uees_dikirim_ke' then data_content end) as [Info UEES dikirim ke]
		,max(case when field_name_id = 'tanggal_kirim_info_uees' then data_content end) as [Tanggal Kirim Info UEES]
		,max(case when field_name_id = 'promo' then data_content end) as [Promo]
		,max(case when field_name_id = 'kode_voucher_promo' then data_content end) as [Kode Voucher Promo]
		,max(case when field_name_id = 'konfrimasi_wa' then data_content end) as [Konfrimasi WA]
		,max(case when field_name_id = 'no_telp_dengan_wa' then data_content end) as [No Telp dengan WA]
		,max(case when field_name_id = 'rekonsel_fa' then data_content end) as [Rekonsel FA]
		,max(case when field_name_id = 'rekonsel_date_fa' then data_content end) as [Rekonsel Date FA]
		,max(case when field_name_id = 'q1' then data_content end) as [Q1]
		,max(case when field_name_id = 'no_telp_lain' then data_content end) as [No Telp Lain]
		,max(case when field_name_id = 'konfirmasi' then data_content end) as [Konfirmasi]
		,max(case when field_name_id = 'sign-off_status' then data_content end) as [Sign-off status]
		,max(case when field_name_id = 'result_fu' then data_content end) as [Result FU]
		,max(case when field_name_id = '1st_fu' then data_content end) as [1st FU]
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_esvi_tms_prospect_campaign_result_intelix') }} cr
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
		,a.phone_called
		,a.appointmentDate as [Appointment Date]
		,a.appointmentTime as [Appointment Time]
		,a.created_time
		,row_number() over(partition by a.prospect_id order by a.created_time desc) as Sort
		,[Attempt Call] = count(a.created_time) over(partition by a.prospect_id)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_esvi_tms_prospect_history_contact_intelix') }} a
    where a.file_id in (select file_id from prospect)
)
,map_lov as (
    select 
        a.prospect_id
		,b.[name] as [Connect Response]
		,c.[name] as [Contact Response]
		,a.[Description]
		,a.[Appointment Date]
		,a.[Appointment Time]
		,a.[phone_called]
		,a.[created_time] as [Last Response Time]
		,a.[Sort]
		,a.[Attempt Call]
    from history_contact a
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_esvi_cc_master_category_intelix') }} b
        ON a.LOV1 = b.id
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_esvi_cc_master_category_intelix') }} c
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_esvi_tms_prospect_time_frame_intelix') }} a
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
		,b.[PIN]
		,b.[Sumber Data]
		,b.[Titel]
		,b.[AccountID]
		,b.[Account]
		,b.[IsAWO]
		,b.[MobilePhone2]
		,b.[HomePhone]
		,b.[OfficePhone]
		,b.[name_stp]
		,b.[hp_stp]
		,b.[name_cp]
		,b.[hp_cp]
		,b.[name_dm]
		,b.[hp_dm]
		,b.[Street]
		,b.[City]
		,b.[PostalCode]
		,b.[Region]
		,b.[MSISDN]
		,b.[VIN]
		,b.[PlatNum]
		,b.[InvoiceDate]
		,b.[Model]
		,b.[ModelYear]
		,b.[Brand]
		,b.[RegValidFrom]
		,b.[RegValidTo]
		,b.[RegStatus]
		,b.[LastServiceDate]
		,b.[SourceCode]
		,b.[SourceDesc]
		,b.[AWO]
		,b.[LastTransERA]
		,b.[LastTransERADate]
		,b.[LastTransERAByPerson]
		,b.[LastTransERADateByPerson]
		,b.[LastInteraction]
		,b.[LastInteractionDate]
		,b.[Prioritas]
		,b.[Reply]
		,b.[PlatNum_Cantik]
		,b.[Flag]
		,b.[CoverageArea_ERA]
		,b.[Reply SMS by WA]
		,b.[report SMS by MSISDN]
		,b.[report SMS by PlatNum]
		,b.[Month Supply]
		,b.[RO]
		,b.[FlagMSISDN]
		,b.[Week]
		,b.[FLAG DATA]
		,b.[Note Rutin Service]
		,b.[Note Pernah Bayar EES]
		,b.[Opt_In]
		,b.[CCA Before]
		,b.[CCA]
		,b.[Periode_Call]
		,c.campaign_id
		,c.[PREFERENSI HARGA]
		,c.[Rencana Tanggal Bayar]
		,c.[CONNECT RESPONSE CALL]
		,c.[CONNECT RESPONSE WA]
		,c.[CONTACT RESPONSE WA]
		,c.[Reason Interest Not Paid]
		,c.[Reason Interest Not Paid Lainnya]
		,c.[Nama Baru]
		,c.[Alamat Baru]
		,c.[Plat Number Baru]
		,c.[Mobile Phone Baru]
		,c.[Nama Contact]
		,c.[Alamat Email]
		,c.[Notes]
		,c.[Other Request]
		,c.[Tanggal konfrimasi mutasi finance]
		,c.[Tanggal Validasi]
		,c.[Konfrimasi Jangka Waktu Pembayaran]
		,c.[Nominal Pembayaran]
		,c.[Keterangan Konfrimasi]
		,c.[Tanggal Update FU]
		,c.[Tanggal Kirim Surat]
		,c.[Update di SAP]
		,c.[Request Info UEES]
		,c.[Info UEES dikirim ke]
		,c.[Tanggal Kirim Info UEES]
		,c.[Promo]
		,c.[Kode Voucher Promo]
		,c.[Konfrimasi WA]
		,c.[No Telp dengan WA]
		,c.[Rekonsel FA]
		,c.[Rekonsel Date FA]
		,c.[Q1]
		,c.[No Telp Lain]
		,c.[Konfirmasi]
		,c.[Sign-off status]
		,c.[Result FU]
		,c.[1st FU]
		,d.[Connect Response]
		,d.[Contact Response]
		,d.[Description]
		,e.[Campaign Result]
		,e.[Campaign Status]
		,d.[Last Response Time]
		,d.[phone_called]
		,e.[Agent Id]
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