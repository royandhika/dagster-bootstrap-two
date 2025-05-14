{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_mrs_iso',
		tags=['12hourly']
	)
}}

with prospect as (
    select 
        tp.id
		,tp.customer_id 
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrs_iso_tms_prospect_intelix') }} tp
    where main_campaign in ('ISO_CSI')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
		,max(case when field_name_id in ('01 Title') then data_content end) [01 Title]
        ,max(case when field_name_id in ('02 AccountID') then data_content end) [02 AccountID]
        ,max(case when field_name_id in ('03 AccountName') then data_content end) [03 AccountName]
        ,max(case when field_name_id in ('04 PICName') then data_content end) [04 PICName]
        ,max(case when field_name_id in ('05 Address') then data_content end) [05 Address]
        ,max(case when field_name_id in ('06 City') then data_content end) [06 City]
        ,max(case when field_name_id in ('07 Province') then data_content end) [07 Province]
        ,max(case when field_name_id in ('08 ZipCode') then data_content end) [08 ZipCode]
        ,max(case when field_name_id in ('09 HomePhone') then data_content end) [09 HomePhone]
        ,max(case when field_name_id in ('10 MobilePhone') then data_content end) [10 MobilePhone]
        ,max(case when field_name_id in ('11 OfficePhone') then data_content end) [11 OfficePhone]
        ,max(case when field_name_id in ('12 VIN') then data_content end) [12 VIN]
        ,max(case when field_name_id in ('13 PlatNumber') then data_content end) [13 PlatNumber]
        ,max(case when field_name_id in ('14 ProductYear') then data_content end) [14 ProductYear]
        ,max(case when field_name_id in ('15 Model') then data_content end) [15 Model]
        ,max(case when field_name_id in ('16 PKBDate') then data_content end) [16 PKBDate]
        ,max(case when field_name_id in ('17 Branch') then data_content end) [17 Branch]
        ,max(case when field_name_id in ('18 BranchDesc') then data_content end) [18 BranchDesc]
        ,max(case when field_name_id in ('19 SAName') then data_content end) [19 SAName]
        ,max(case when field_name_id in ('20 MainActivity') then data_content end) [20 MainActivity]
        ,max(case when field_name_id in ('21 ServiceType') then data_content end) [21 ServiceType]
        ,max(case when field_name_id in ('22 PM') then data_content end) [22 PM]
        ,max(case when field_name_id in ('23 MileAge') then data_content end) [23 MileAge]
        ,max(case when field_name_id in ('24 SupplyPeriod') then data_content end) [24 SupplyPeriod]
        ,max(case when field_name_id in ('Parameter 1') then data_content end) [Parameter 1]
        ,max(case when field_name_id in ('Parameter 2') then data_content end) [Parameter 2]
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrs_iso_tms_prospect_detail_intelix') }} pd
    where exists (
    	select prospect_id 
    	from prospect p
    	where pd.prospect_id = p.id
	)
    group by prospect_id
)
,campaign_result as (
    select 
        cr.prospect_id
		,cr.campaign_id
		,cr.file_id
        ,max(case when field_name_id in ('notes') then data_content end) [notes]
        ,max(case when field_name_id in ('q1-berdasarkan-data-yang-kami-miliki-bpkibu-nama-cust-baru-saja-melakukan-service-kendaraan-xxx-pada-bulan-xxx-lalu-apakah-benar-pakbu') then data_content end) [q1-berdasarkan-data-yang-]
        ,max(case when field_name_id in ('q2-selain-nomor-telepon-ini-mungkin-bapakibu-nama-cust-memiliki-alamat-email-yang-dapat-kami-hubungi-jika-tidak-ada-alamat-email-pada-supply-data') then data_content end) [q2-selain-nomor-telepon-i]
        ,max(case when field_name_id in ('q3-untuk-alamat-domisili-bapakibu-berdasarkan-data-kami-adalah-di-xxx-apakah-alamatnya-sudah-sesuai-pakbu') then data_content end) [q3-untuk-alamat-domisili-]
        ,max(case when field_name_id in ('q31-baik-bapakibu-dapat-diinformasikan-untuk-alamat-domisili-saat-ini') then data_content end) [q31-baik-bapakibu-dapat-d]
        ,max(case when field_name_id in ('q4-baik-bapakibunama-cust-apabila-berkenan-dapat-diinformasikan-bidang-usaha-bapakibu-saat-ini-untuk-kelengkapan-data-kami') then data_content end) [q4-baik-bapakibunama-cust]
        ,max(case when field_name_id in ('q41-bidang-usaha-customer') then data_content end) [q41-bidang-usaha-customer]
        ,max(case when field_name_id in ('q5-apakah-bapak--ibunama-cust-sudah-merasa-cukup-mudah-untuk-menemukan-lokasi-atau-mendapatkan-service-dari-bengkel-kami') then data_content end) [q5-apakah-bapak--ibunama-]
        ,max(case when field_name_id in ('q6-apakah-bapak--ibu-nam-cust-sudah-merasa-puas-dengan-keseluruhan-layanan-service-yang-kami-berikan') then data_content end) [q6-apakah-bapak--ibu-nam-]
        ,max(case when field_name_id in ('q7-apakah-bapakibunama-cust-bersedia-menggunakan-layanan-kami-kembali-kedepannya-pakbu') then data_content end) [q7-apakah-bapakibunama-cu]
        ,max(case when field_name_id in ('q8-apakah-bapakibu-bersedia-merekomendasikan-pembelian-product-isuzu-kepada-rekan-bapakibu-10-untuk-merekomendasikan-dan-1-untuk-tidak-merekomendasikan-bapakibunama-cust-memberikan-di-angka-berapa-pakbu') then data_content end) [q8-apakah-bapakibu-bersed]
        ,max(case when field_name_id in ('q9-apakah-ada-keluhan-atau-perbaikan-yang-bapak--ibu-harapkan-dari-layanan-service-kami-untuk-ke-depannya-') then data_content end) [q9-apakah-ada-keluhan-ata]
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrs_iso_tms_prospect_campaign_result_intelix') }} cr
    where exists (
    	select file_id 
    	from prospect p
    	where cr.file_id = p.file_id
	)
    group by prospect_id, campaign_id, file_id
)
,response_detail as (
    select 
		cr.prospect_id
		,cr.file_id
		,cr.module
        ,attempt = count(cr.module) over(partition by cr.prospect_id)
        ,created_time = max(cr.created_time)
        ,max(case when cr.field_name_id = 'category_ticket_1' then mc.name end) [Connect Response]
		,max(case when cr.field_name_id = 'category_ticket_2' then mc.name end) [Contact Response]
		,max(case when cr.field_name_id = 'category_ticket_3' then mc.name end) [Campaign Status]
		,max(case when cr.field_name_id = 'category_ticket_4' then mc.name end) [Campaign Result]
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrs_iso_tms_prospect_response_detail_intelix') }} cr
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_mrs_iso_tms_master_category_intelix') }} mc 
        on cr.data_content = mc.id
    where exists (
    	select file_id 
    	from prospect p
    	where cr.file_id = p.file_id
	)
    group by cr.prospect_id, cr.file_id, cr.module
)
,response_detail_map as (
    select 
        *
        ,sort = row_number() over(partition by prospect_id order by created_time desc)
    from response_detail 
)
,final as (
    select
        a.id
		,a.file_id
		,a.customer_id
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
        ,b.[01 Title]
        ,b.[02 AccountID]
        ,b.[03 AccountName]
        ,b.[04 PICName]
        ,b.[05 Address]
        ,b.[06 City]
        ,b.[07 Province]
        ,b.[08 ZipCode]
        ,b.[09 HomePhone]
        ,b.[10 MobilePhone]
        ,b.[11 OfficePhone]
        ,b.[12 VIN]
        ,b.[13 PlatNumber]
        ,b.[14 ProductYear]
        ,b.[15 Model]
        ,b.[16 PKBDate]
        ,b.[17 Branch]
        ,b.[18 BranchDesc]
        ,b.[19 SAName]
        ,b.[20 MainActivity]
        ,b.[21 ServiceType]
        ,b.[22 PM]
        ,b.[23 MileAge]
        ,b.[24 SupplyPeriod]
        ,b.[Parameter 1]
        ,b.[Parameter 2]
		,c.campaign_id
        ,c.[notes]
        ,c.[q1-berdasarkan-data-yang-]
        ,c.[q2-selain-nomor-telepon-i]
        ,c.[q3-untuk-alamat-domisili-]
        ,c.[q31-baik-bapakibu-dapat-d]
        ,c.[q4-baik-bapakibunama-cust]
        ,c.[q41-bidang-usaha-customer]
        ,c.[q5-apakah-bapak--ibunama-]
        ,c.[q6-apakah-bapak--ibu-nam-]
        ,c.[q7-apakah-bapakibunama-cu]
        ,c.[q8-apakah-bapakibu-bersed]
        ,c.[q9-apakah-ada-keluhan-ata]
		,e.[Connect Response]
		,e.[Contact Response]
		,e.[Campaign Status]
		,e.[Campaign Result]
		,[Attempt Call] = e.attempt 
		,uploaddate = getdate()
	from prospect a
	left join prospect_detail b 
        on a.id = b.prospect_id
	left join campaign_result c 
        on a.id = c.prospect_id 
		and a.main_campaign = c.campaign_id
	left join response_detail_map e 
        on a.id = e.prospect_id 
        and e.sort = 1
)
select * from final