{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_tafteleacquisition',
		tags=['3hourly']
	)
}}

with prospect as (
    select 
        tp.id
        ,tp.file_id
        ,tp.main_campaign
        ,tp.group_tele
        ,tp.start_date
        ,tp.end_date
        ,customername = tp.cust_name
		,prospectno = tp.customer_id
        ,tp.cust_nik
        ,tp.home_phone
        ,tp.mobile_phone
        ,tp.office_phone
        ,tp.other_phone1
        ,tp.other_phone2
        ,tp.other_phone3
        ,tp.other_phone4
        ,tp.other_phone5
        ,tp.other_phone6
        ,tp.other_phone7
        ,tp.note
        ,tp.admin_assigned_by
        ,tp.admin_assign_time
        ,tp.admin_assign_note
        ,tp.head_unit_id
        ,tp.head_unit_assign_time
        ,tp.head_unit_note
        ,tp.spv_id
        ,tp.spv_assign_time
        ,tp.spv_notes
        ,tp.agent_id
        ,tp.agent_pickup_time
        ,tp.last_phone_call
        ,tp.last_id_history_contact
        ,tp.first_category
        ,tp.last_category
        ,tp.last_campaign_agree
        ,tp.first_response_time
        ,tp.last_response_time
        ,tp.last_agent_notes
        ,tp.qa_id
        ,tp.qa_id_time_frame
        ,tp.qa_response_time
        ,tp.qa_notes
        ,tp.is_pickup
        ,tp.is_recycle_headunit
        ,tp.is_recycle_spv
        ,tp.is_predial
        ,tp.status
        ,tp.is_customer
        ,tp.created_by
        ,tp.created_time
        ,tp.last_info_time
        ,tp.appointmentDate
        ,tp.appointmentTime
        ,tp.call_attempt
        ,tp.duration
        ,tp.filter1
        ,tp.filter2
        ,tp.filter3
        ,tp.filter4
        ,tp.filter5
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_tms_prospect_intelix') }} tp
    where main_campaign in ('COMPLETION', 'MATCHING')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,prospect_detail as (
    select 
        prospect_id
		,max(case when field_name_id in ('area_cde') then data_content end) as area_cde
        ,max(case when field_name_id in ('campaign_result') then data_content end) as campaign_result
        ,max(case when field_name_id in ('customer_name') then data_content end) as customer_name
        ,max(case when field_name_id in ('dp_percent') then data_content end) as dp_percent
        ,max(case when field_name_id in ('dukcapil_customer') then data_content end) as dukcapil_customer
        ,max(case when field_name_id in ('dukcapil_customer_tidak_ditemukan') then data_content end) as dukcapil_customer_tidak_ditemukan
        ,max(case when field_name_id in ('dukcapil_customer_tidak_ditemukan_notes') then data_content end) as dukcapil_customer_tidak_ditemukan_notes
        ,max(case when field_name_id in ('dukcapil_customer_tidak_sesuai') then data_content end) as dukcapil_customer_tidak_sesuai
        ,max(case when field_name_id in ('dukcapil_guarantor') then data_content end) as dukcapil_guarantor
        ,max(case when field_name_id in ('dukcapil_guarantor_tidak_ditemukan') then data_content end) as dukcapil_guarantor_tidak_ditemukan
        ,max(case when field_name_id in ('dukcapil_guarantor_tidak_ditemukan_notes') then data_content end) as dukcapil_guarantor_tidak_ditemukan_notes
        ,max(case when field_name_id in ('dukcapil_guarantor_tidak_sesuai') then data_content end) as dukcapil_guarantor_tidak_sesuai
        ,max(case when field_name_id in ('dukcapil_spouse') then data_content end) as dukcapil_spouse
        ,max(case when field_name_id in ('dukcapil_spouse_tidak_ditemukan') then data_content end) as dukcapil_spouse_tidak_ditemukan
        ,max(case when field_name_id in ('dukcapil_spouse_tidak_ditemukan_notes') then data_content end) as dukcapil_spouse_tidak_ditemukan_notes
        ,max(case when field_name_id in ('dukcapil_spouse_tidak_sesuai') then data_content end) as dukcapil_spouse_tidak_sesuai
        ,max(case when field_name_id in ('dukcapil_stnk') then data_content end) as dukcapil_stnk
        ,max(case when field_name_id in ('dukcapil_stnk_tidak_ditemukan') then data_content end) as dukcapil_stnk_tidak_ditemukan
        ,max(case when field_name_id in ('dukcapil_stnk_tidak_ditemukan_notes') then data_content end) as dukcapil_stnk_tidak_ditemukan_notes
        ,max(case when field_name_id in ('dukcapil_stnk_tidak_sesuai') then data_content end) as dukcapil_stnk_tidak_sesuai
        ,max(case when field_name_id in ('fraud_detection') then data_content end) as fraud_detection
        ,max(case when field_name_id in ('fu_matching_-_so') then data_content end) as [fu_matching_-_so]
        ,max(case when field_name_id in ('fu_so_-_matching') then data_content end) as [fu_so_-_matching]
        ,max(case when field_name_id in ('get_contact') then data_content end) as get_contact
        ,max(case when field_name_id in ('guarantor') then data_content end) as guarantor
        ,max(case when field_name_id in ('list_priority') then data_content end) as list_priority
        ,max(case when field_name_id in ('mobile_phone_1') then data_content end) as mobile_phone_1
        ,max(case when field_name_id in ('mobile_phone_2') then data_content end) as mobile_phone_2
        ,max(case when field_name_id in ('mobile_phone_3') then data_content end) as mobile_phone_3
        ,max(case when field_name_id in ('nama_agent') then data_content end) as nama_agent
        ,max(case when field_name_id in ('nama_guarantor') then data_content end) as nama_guarantor
        ,max(case when field_name_id in ('nama_kawin_tercatat') then data_content end) as nama_kawin_tercatat
        ,max(case when field_name_id in ('nama_kawin_tercatat_pra') then data_content end) as nama_kawin_tercatat_pra
        ,max(case when field_name_id in ('nama_kawin_tercatat_spouse') then data_content end) as nama_kawin_tercatat_spouse
        ,max(case when field_name_id in ('nama_kawin_tidak_tercatat') then data_content end) as nama_kawin_tidak_tercatat
        ,max(case when field_name_id in ('nama_project') then data_content end) as nama_project
        ,max(case when field_name_id in ('notes_matching') then data_content end) as notes_matching
        ,max(case when field_name_id in ('ocr') then data_content end) as ocr
        ,max(case when field_name_id in ('office_name') then data_content end) as office_name
        ,max(case when field_name_id in ('product_offering_name') then data_content end) as product_offering_name
        ,max(case when field_name_id in ('prospect_no') then data_content end) as prospect_no
        ,max(case when field_name_id in ('reason_cancel') then data_content end) as reason_cancel
        ,max(case when field_name_id in ('reason_contact_tidak_sesuai') then data_content end) as reason_contact_tidak_sesuai
        ,max(case when field_name_id in ('reason_pending_matching') then data_content end) as reason_pending_matching
        ,max(case when field_name_id in ('reason_tidak_sesuai') then data_content end) as reason_tidak_sesuai
        ,max(case when field_name_id in ('sales_officer_name') then data_content end) as sales_officer_name
        ,max(case when field_name_id in ('salesman_dealer_name') then data_content end) as salesman_dealer_name
        ,max(case when field_name_id in ('start_executing_date') then data_content end) as start_executing_date
        ,max(case when field_name_id in ('status_perkawinan') then data_content end) as status_perkawinan
        ,max(case when field_name_id in ('submit_matching_date') then data_content end) as submit_matching_date
        ,max(case when field_name_id in ('supplier_branch_name') then data_content end) as supplier_branch_name
        ,max(case when field_name_id in ('tipe_data_matching') then data_content end) as tipe_data_matching
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_tms_prospect_detail_intelix') }} pd
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
        ,max(case when field_name_id in ('alamat_cabang_spouse_other_business_address') then data_content end) as alamat_cabang_spouse_other_business_address
        ,max(case when field_name_id in ('alamat_guarantor_job') then data_content end) as alamat_guarantor_job
        ,max(case when field_name_id in ('asset_for_online_transport') then data_content end) as asset_for_online_transport
        ,max(case when field_name_id in ('bekerja_di_guarantor_job') then data_content end) as bekerja_di_guarantor_job
        ,max(case when field_name_id in ('bergerak_bidang_spouse_other_business_address') then data_content end) as bergerak_bidang_spouse_other_business_address
        ,max(case when field_name_id in ('cabang_spouse_business') then data_content end) as cabang_spouse_business
        ,max(case when field_name_id in ('choice_digunakan_dimana') then data_content end) as choice_digunakan_dimana
        ,max(case when field_name_id in ('choice_kegunaan') then data_content end) as choice_kegunaan
        ,max(case when field_name_id in ('choice_siapa') then data_content end) as choice_siapa
        ,max(case when field_name_id in ('contacted_person_relationship') then data_content end) as contacted_person_relationship
        ,max(case when field_name_id in ('cpr_name') then data_content end) as cpr_name
        ,max(case when field_name_id in ('customer_busines_address') then data_content end) as customer_busines_address
        ,max(case when field_name_id in ('customer_job_address') then data_content end) as customer_job_address
        ,max(case when field_name_id in ('customer_other_business_address') then data_content end) as customer_other_business_address
        ,max(case when field_name_id in ('customer_other_job_address') then data_content end) as customer_other_job_address
        ,max(case when field_name_id in ('customer_other_residence_address') then data_content end) as customer_other_residence_address
        ,max(case when field_name_id in ('customer_residence_address') then data_content end) as customer_residence_address
        ,max(case when field_name_id in ('dikenal_dengan_spouse_other_residence_address') then data_content end) as dikenal_dengan_spouse_other_residence_address
        ,max(case when field_name_id in ('end_executing_date') then data_content end) as end_executing_date
        ,max(case when field_name_id in ('ext_guarantor_job') then data_content end) as ext_guarantor_job
        ,max(case when field_name_id in ('fu_completion_so') then data_content end) as fu_completion_so
        ,max(case when field_name_id in ('fu_so_completion') then data_content end) as fu_so_completion
        ,max(case when field_name_id in ('golongan_guarantor_job') then data_content end) as golongan_guarantor_job
        ,max(case when field_name_id in ('guarantor_job_address') then data_content end) as guarantor_job_address
        ,max(case when field_name_id in ('jabatan_spouse_business') then data_content end) as jabatan_spouse_business
        ,max(case when field_name_id in ('jam_operasional_spouse_other_business_address') then data_content end) as jam_operasional_spouse_other_business_address
        ,max(case when field_name_id in ('jumlah_cabang_spouse_other_business_address') then data_content end) as jumlah_cabang_spouse_other_business_address
        ,max(case when field_name_id in ('jumlah_karyawan_guarantor_job') then data_content end) as jumlah_karyawan_guarantor_job
        ,max(case when field_name_id in ('keterangan_tidak_bisa_dilalui_spouse_residence') then data_content end) as keterangan_tidak_bisa_dilalui_spouse_residence
        ,max(case when field_name_id in ('last_call') then data_content end) as last_call
        ,max(case when field_name_id in ('lb_guarantor_job') then data_content end) as lb_guarantor_job
        ,max(case when field_name_id in ('lb_guarantor_job_month') then data_content end) as lb_guarantor_job_month
        ,max(case when field_name_id in ('lt_spouse_residence') then data_content end) as lt_spouse_residence
        ,max(case when field_name_id in ('lt_spouse_residence_month') then data_content end) as lt_spouse_residence_month
        ,max(case when field_name_id in ('lu_spouse_business_month') then data_content end) as lu_spouse_business_month
        ,max(case when field_name_id in ('lu_spouse_other_business_address') then data_content end) as lu_spouse_other_business_address
        ,max(case when field_name_id in ('mobil_pertama') then data_content end) as mobil_pertama
        ,max(case when field_name_id in ('nama_agent') then data_content end) as nama_agent
        ,max(case when field_name_id in ('nama_agent_fu') then data_content end) as nama_agent_fu
        ,max(case when field_name_id in ('nama_panggilan_customer_other_job_address') then data_content end) as nama_panggilan_customer_other_job_address
        ,max(case when field_name_id in ('nama_panggilan_guarantor_job') then data_content end) as nama_panggilan_guarantor_job
        ,max(case when field_name_id in ('nama_panggilan_spouse_other_business_address') then data_content end) as nama_panggilan_spouse_other_business_address
        ,max(case when field_name_id in ('nama_usaha_spouse_other_business_address') then data_content end) as nama_usaha_spouse_other_business_address
        ,max(case when field_name_id in ('nik_guarantor_job') then data_content end) as nik_guarantor_job
        ,max(case when field_name_id in ('no_telpon_guarantor_job') then data_content end) as no_telpon_guarantor_job
        ,max(case when field_name_id in ('no_telpon_spouse_business') then data_content end) as no_telpon_spouse_business
        ,max(case when field_name_id in ('note_spouse_other_business_address') then data_content end) as note_spouse_other_business_address
        ,max(case when field_name_id in ('notes') then data_content end) as notes
        ,max(case when field_name_id in ('notes_completion') then data_content end) as notes_completion
        ,max(case when field_name_id in ('omset_perbulan_spouse_other_business_address') then data_content end) as omset_perbulan_spouse_other_business_address
        ,max(case when field_name_id in ('other_stnk') then data_content end) as other_stnk
        ,max(case when field_name_id in ('patokan_guarantor_job') then data_content end) as patokan_guarantor_job
        ,max(case when field_name_id in ('penilaian_matching') then data_content end) as penilaian_matching
        ,max(case when field_name_id in ('posisi_guarantor_job') then data_content end) as posisi_guarantor_job
        ,max(case when field_name_id in ('posisi_rumah_spouse_residence') then data_content end) as posisi_rumah_spouse_residence
        ,max(case when field_name_id in ('reason_no') then data_content end) as reason_no
        ,max(case when field_name_id in ('reason_tidak_sesuai_penilaian') then data_content end) as reason_tidak_sesuai_penilaian
        ,max(case when field_name_id in ('spouse_business_address') then data_content end) as spouse_business_address
        ,max(case when field_name_id in ('spouse_job_address') then data_content end) as spouse_job_address
        ,max(case when field_name_id in ('spouse_other_business_address') then data_content end) as spouse_other_business_address
        ,max(case when field_name_id in ('spouse_other_job_address') then data_content end) as spouse_other_job_address
        ,max(case when field_name_id in ('spouse_other_residence_address') then data_content end) as spouse_other_residence_address
        ,max(case when field_name_id in ('spouse_residence_address') then data_content end) as spouse_residence_address
        ,max(case when field_name_id in ('status_karyawan_guarantor_job') then data_content end) as status_karyawan_guarantor_job
        ,max(case when field_name_id in ('status_rumah_spouse_other_residence_address') then data_content end) as status_rumah_spouse_other_residence_address
        ,max(case when field_name_id in ('status_tempat_spouse_business') then data_content end) as status_tempat_spouse_business
        ,max(case when field_name_id in ('stnk') then data_content end) as stnk
        ,max(case when field_name_id in ('tahun_kendaran_tambahan') then data_content end) as tahun_kendaran_tambahan
        ,max(case when field_name_id in ('tempat_kerja_berbentuk_guarantor_job') then data_content end) as tempat_kerja_berbentuk_guarantor_job
        ,max(case when field_name_id in ('tempat_kerja_guarantor_job') then data_content end) as tempat_kerja_guarantor_job
        ,max(case when field_name_id in ('tidak_bisa_dilalui_spouse_residence') then data_content end) as tidak_bisa_dilalui_spouse_residence
        ,max(case when field_name_id in ('tipe_data_completion') then data_content end) as tipe_data_completion
        ,max(case when field_name_id in ('way_of_payment') then data_content end) as way_of_payment
        ,max(case when field_name_id in ('working_date') then data_content end) as working_date
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_tms_prospect_campaign_result_intelix') }} cr
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
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_tms_prospect_history_contact_intelix') }} a
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
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_cc_master_category_intelix') }} b
        on a.LOV1 = b.id
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_cc_master_category_intelix') }} c
        on a.LOV2 = c.id
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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tafteleacquisition_tms_prospect_time_frame_intelix') }} a
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
		,a.customername
        ,a.prospectno
        ,a.home_phone
        ,a.mobile_phone
        ,a.office_phone
        ,a.other_phone1
        ,a.other_phone2
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
        ,b.area_cde
        ,matching_result = b.campaign_result
        -- ,b.customer_name
        ,b.dp_percent
        ,b.dukcapil_customer
        ,b.dukcapil_customer_tidak_ditemukan
        ,b.dukcapil_customer_tidak_ditemukan_notes
        ,b.dukcapil_customer_tidak_sesuai
        ,b.dukcapil_guarantor
        ,b.dukcapil_guarantor_tidak_ditemukan
        ,b.dukcapil_guarantor_tidak_ditemukan_notes
        ,b.dukcapil_guarantor_tidak_sesuai
        ,b.dukcapil_spouse
        ,b.dukcapil_spouse_tidak_ditemukan
        ,b.dukcapil_spouse_tidak_ditemukan_notes
        ,b.dukcapil_spouse_tidak_sesuai
        ,b.dukcapil_stnk
        ,b.dukcapil_stnk_tidak_ditemukan
        ,b.dukcapil_stnk_tidak_ditemukan_notes
        ,b.dukcapil_stnk_tidak_sesuai
        ,b.fraud_detection
        ,[fu_matching_-_so] = try_convert(datetime, replace(b.[fu_matching_-_so], 'T', ' ')) 
        ,[fu_so_-_matching] = try_convert(datetime, replace(b.[fu_so_-_matching], 'T', ' ')) 
        ,b.get_contact
        ,b.guarantor
        ,b.list_priority
        ,b.mobile_phone_1
        ,b.mobile_phone_2
        ,b.mobile_phone_3
        ,b.nama_agent
        ,b.nama_guarantor
        ,b.nama_kawin_tercatat
        ,b.nama_kawin_tercatat_pra
        ,b.nama_kawin_tercatat_spouse
        ,b.nama_kawin_tidak_tercatat
        ,b.nama_project
        ,b.notes_matching
        ,b.ocr
        ,b.office_name
        ,b.product_offering_name
        ,b.prospect_no
        ,b.reason_cancel
        ,b.reason_contact_tidak_sesuai
        ,b.reason_pending_matching
        ,b.reason_tidak_sesuai
        ,b.sales_officer_name
        ,b.salesman_dealer_name
        ,start_executing_date = try_convert(datetime, replace(b.start_executing_date, 'T', ' '))
        ,b.status_perkawinan
        ,submit_matching_date = try_convert(datetime, replace(b.submit_matching_date, 'T', ' '))
        ,b.supplier_branch_name
        ,b.tipe_data_matching
		,c.campaign_id
        ,c.alamat_cabang_spouse_other_business_address
        ,c.alamat_guarantor_job
        ,c.asset_for_online_transport
        ,c.bekerja_di_guarantor_job
        ,c.bergerak_bidang_spouse_other_business_address
        ,c.cabang_spouse_business
        ,c.choice_digunakan_dimana
        ,c.choice_kegunaan
        ,c.choice_siapa
        ,c.contacted_person_relationship
        ,c.cpr_name
        ,c.customer_busines_address
        ,c.customer_job_address
        ,c.customer_other_business_address
        ,c.customer_other_job_address
        ,c.customer_other_residence_address
        ,c.customer_residence_address
        ,c.dikenal_dengan_spouse_other_residence_address
        ,end_executing_date = try_convert(datetime, replace(c.end_executing_date, 'T', ' '))
        ,c.ext_guarantor_job
        ,c.fu_completion_so
        ,c.fu_so_completion
        ,c.golongan_guarantor_job
        ,c.guarantor_job_address
        ,c.jabatan_spouse_business
        ,c.jam_operasional_spouse_other_business_address
        ,c.jumlah_cabang_spouse_other_business_address
        ,c.jumlah_karyawan_guarantor_job
        ,c.keterangan_tidak_bisa_dilalui_spouse_residence
        ,last_call = try_convert(datetime, replace(c.last_call, 'T', ' '))
        ,c.lb_guarantor_job
        ,c.lb_guarantor_job_month
        ,c.lt_spouse_residence
        ,c.lt_spouse_residence_month
        ,c.lu_spouse_business_month
        ,c.lu_spouse_other_business_address
        ,c.mobil_pertama
        -- ,c.nama_agent
        ,c.nama_agent_fu
        ,c.nama_panggilan_customer_other_job_address
        ,c.nama_panggilan_guarantor_job
        ,c.nama_panggilan_spouse_other_business_address
        ,c.nama_usaha_spouse_other_business_address
        ,c.nik_guarantor_job
        ,c.no_telpon_guarantor_job
        ,c.no_telpon_spouse_business
        ,c.note_spouse_other_business_address
        ,c.notes
        ,c.notes_completion
        ,c.omset_perbulan_spouse_other_business_address
        ,c.other_stnk
        ,c.patokan_guarantor_job
        ,c.penilaian_matching
        ,c.posisi_guarantor_job
        ,c.posisi_rumah_spouse_residence
        ,c.reason_no
        ,c.reason_tidak_sesuai_penilaian
        ,c.spouse_business_address
        ,c.spouse_job_address
        ,c.spouse_other_business_address
        ,c.spouse_other_job_address
        ,c.spouse_other_residence_address
        ,c.spouse_residence_address
        ,c.status_karyawan_guarantor_job
        ,c.status_rumah_spouse_other_residence_address
        ,c.status_tempat_spouse_business
        ,c.stnk
        ,c.tahun_kendaran_tambahan
        ,c.tempat_kerja_berbentuk_guarantor_job
        ,c.tempat_kerja_guarantor_job
        ,c.tidak_bisa_dilalui_spouse_residence
        ,c.tipe_data_completion
        ,c.way_of_payment
        ,working_date = try_convert(datetime, replace(c.working_date, 'T', ' '))
		,d.[Connect Response]
		,d.[Contact Response]
		,d.[Description]
		,e.[Campaign Result]
		,[Campaign Status] = replace(e.[Campaign Status], '&lt;', '<')
		-- ,d.[Last Response Time]
		,e.[Agent Id]
		,d.[Appointment Date]
		,d.[Appointment Time]
		,d.[Attempt Call]
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