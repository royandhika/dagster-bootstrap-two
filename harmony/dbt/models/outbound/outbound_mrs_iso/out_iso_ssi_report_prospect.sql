{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_mrs_iso',
		tags=['outbound']
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
    where main_campaign in ('ISO_SSI')
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
        ,max(case when field_name_id in ('03 Account') then data_content end) [03 Account]
        ,max(case when field_name_id in ('04 Address') then data_content end) [04 Address]
        ,max(case when field_name_id in ('05 City') then data_content end) [05 City]
        ,max(case when field_name_id in ('06 Region') then data_content end) [06 Region]
        ,max(case when field_name_id in ('07 Model') then data_content end) [07 Model]
        ,max(case when field_name_id in ('08 ProdDesc') then data_content end) [08 ProdDesc]
        ,max(case when field_name_id in ('09 ModelYear') then data_content end) [09 ModelYear]
        ,max(case when field_name_id in ('10 PlatNumber') then data_content end) [10 PlatNumber]
        ,max(case when field_name_id in ('11 InvoiceDate') then data_content end) [11 InvoiceDate]
        ,max(case when field_name_id in ('12 GIDate') then data_content end) [12 GIDate]
        ,max(case when field_name_id in ('13 PaymentType') then data_content end) [13 PaymentType]
        ,max(case when field_name_id in ('14 Tenor') then data_content end) [14 Tenor]
        ,max(case when field_name_id in ('15 LeasingCompany') then data_content end) [15 LeasingCompany]
        ,max(case when field_name_id in ('16 Branch') then data_content end) [16 Branch]
        ,max(case when field_name_id in ('17 BranchDesc') then data_content end) [17 BranchDesc]
        ,max(case when field_name_id in ('18 Salesman') then data_content end) [18 Salesman]
        ,max(case when field_name_id in ('19 VIN') then data_content end) [19 VIN]
        ,max(case when field_name_id in ('20 ProdCode') then data_content end) [20 ProdCode]
        ,max(case when field_name_id in ('21 MobilePhone') then data_content end) [21 MobilePhone]
        ,max(case when field_name_id in ('22 HomePhone') then data_content end) [22 HomePhone]
        ,max(case when field_name_id in ('23 OfficePhone') then data_content end) [23 OfficePhone]
        ,max(case when field_name_id in ('24 ContactPerson') then data_content end) [24 ContactPerson]
        ,max(case when field_name_id in ('25 FuncText') then data_content end) [25 FuncText]
        ,max(case when field_name_id in ('26 Periode Call') then data_content end) [26 Periode Call]
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
        ,max(case when field_name_id in ('q1-berdasarkan-data-yang-kami-miliki-bpkibu-baru-saja-melakukan-pembelian-mobil-xxx-pada-bulan-detail-lalu-apakah-benar-pakbu') then data_content end) [q1-berdasarkan-data]
        ,max(case when field_name_id in ('q21-jika-untuk-contact-bapakibu-yang-bisa-dihubungi-selain-melalui-telepon-di-nomor-ini-adalah-melalui-email-alamat') then data_content end) [q21-jika-untuk-cont]
        ,max(case when field_name_id in ('q22-jika-selain-nomor-telepon-ini-mungkin-bapakibu-memiliki-alamat-email-yang-dapat-kami-hubungi') then data_content end) [q22-jika-selain-nom]
        ,max(case when field_name_id in ('q3-berdasarkan-data-kami-untuk-alamat-domisili-bapakibu-adalah-di-xxx-apakah-alamatnya-sudah-sesuai-pakbu') then data_content end) [q3-berdasarkan-data]
        ,max(case when field_name_id in ('q31-baik-bapakibu-dapat-diinformasikan-untuk-alamat-domisili-saat-ini') then data_content end) [q31-baik-bapakibu-d]
        ,max(case when field_name_id in ('q4-baik-bapakibu-apabila-berkenan-dapat-diinformasikan-bidang-usaha-bapakibu-saat-ini-untuk-kelengkapan-data-kami') then data_content end) [q4-baik-bapakibu-ap]
        ,max(case when field_name_id in ('q41-sebutkan-bidang-usaha') then data_content end) [q41-sebutkan-bidang]
        ,max(case when field_name_id in ('q5-apakah-bantuan-dan-informasi-yang-diberikan-oleh-sales-person-kami-sudah-cukup-membantu-bapakibu') then data_content end) [q5-apakah-bantuan-d]
        ,max(case when field_name_id in ('q6-apakah-proses-pembelian-dan-pengiriman-barang-di-cabang--dealer-kami-sudah-cukup-mudah-bagi-bapakibu') then data_content end) [q6-apakah-proses-pe]
        ,max(case when field_name_id in ('q7-apakah-bapakibu-bersedia-melakukan-pembelian-kembali-pada-product-astra-isuzu') then data_content end) [q7-apakah-bapakibu-]
        ,max(case when field_name_id in ('q8-apakah-bapakibu-bersedia-merekomendasikan-pembelian-product-isuzu-kepada-rekan-bapakibu-skala-dari-1---10-10-untuk-merekomendasikan-dan-1-untuk-tidak-merekomendasikan-bapakibu-memberikan-di-angka-berapa-pakbu') then data_content end) [q8-apakah-bapakibu-]
        ,max(case when field_name_id in ('q9-apakah-ada-keluhan-atau-perbaikan-yang-bapak--ibu-harapkan-dari-layanan-service-kami-untuk-ke-depannya-') then data_content end) [q9-apakah-ada-keluh]
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
        ,attempt = cr.module
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
    group by cr.prospect_id, cr.file_id, cr.module, cr.module
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
        ,b.[03 Account]
        ,b.[04 Address]
        ,b.[05 City]
        ,b.[06 Region]
        ,b.[07 Model]
        ,b.[08 ProdDesc]
        ,b.[09 ModelYear]
        ,b.[10 PlatNumber]
        ,b.[11 InvoiceDate]
        ,b.[12 GIDate]
        ,b.[13 PaymentType]
        ,b.[14 Tenor]
        ,b.[15 LeasingCompany]
        ,b.[16 Branch]
        ,b.[17 BranchDesc]
        ,b.[18 Salesman]
        ,b.[19 VIN]
        ,b.[20 ProdCode]
        ,b.[21 MobilePhone]
        ,b.[22 HomePhone]
        ,b.[23 OfficePhone]
        ,b.[24 ContactPerson]
        ,b.[25 FuncText]
        ,b.[26 Periode Call]
        ,b.[Parameter 1]
        ,b.[Parameter 2]
		,c.campaign_id
        ,c.[notes]
        ,c.[q1-berdasarkan-data]
        ,c.[q21-jika-untuk-cont]
        ,c.[q22-jika-selain-nom]
        ,c.[q3-berdasarkan-data]
        ,c.[q31-baik-bapakibu-d]
        ,c.[q4-baik-bapakibu-ap]
        ,c.[q41-sebutkan-bidang]
        ,c.[q5-apakah-bantuan-d]
        ,c.[q6-apakah-proses-pe]
        ,c.[q7-apakah-bapakibu-]
        ,c.[q8-apakah-bapakibu-]
        ,c.[q9-apakah-ada-keluh]
		,e.[Connect Response]
		,e.[Contact Response]
		,e.[Campaign Status]
		,e.[Campaign Result]
		,e.attempt [Attempt Call]
		,uploaddate = getdate()
	from prospect a
	left join prospect_detail b 
        on a.id = b.prospect_id
	left join campaign_result c 
        on a.id = c.prospect_id 
		and a.main_campaign = c.campaign_id
	left join response_detail e 
        on a.id = e.prospect_id 
)
select * from final