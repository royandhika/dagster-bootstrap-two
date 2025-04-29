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
    where main_campaign in ('SURVEY')
		{% if is_incremental() %}
    	and modified_time >= '{{ var('min_date') }}'
    	and modified_time <= '{{ var('max_date') }}'
		{% endif %}
)
,campaign_result as (
    select 
        cr.prospect_id
        ,cr.campaign_id 
        ,cr.file_id 
        ,max(case when field_name_id in ('alamat_c2') then data_content end) as alamat_c2
        ,max(case when field_name_id in ('application_number') then data_content end) as application_number
        ,max(case when field_name_id in ('area') then data_content end) as area
        ,max(case when field_name_id in ('asset_digunakan_dimana') then data_content end) as asset_digunakan_dimana
        ,max(case when field_name_id in ('assignment_date') then data_content end) as assignment_date
        ,max(case when field_name_id in ('cabang') then data_content end) as cabang
        ,max(case when field_name_id in ('cat_rumah_gh') then data_content end) as cat_rumah_gh
        ,max(case when field_name_id in ('child') then data_content end) as child
        ,max(case when field_name_id in ('choice_employee') then data_content end) as choice_employee
        ,max(case when field_name_id in ('choice_investor') then data_content end) as choice_investor
        ,max(case when field_name_id in ('choice_self_employee') then data_content end) as choice_self_employee
        ,max(case when field_name_id in ('company_c1') then data_content end) as company_c1
        ,max(case when field_name_id in ('company_name_c2') then data_content end) as company_name_c2
        ,max(case when field_name_id in ('customer') then data_content end) as customer
        ,max(case when field_name_id in ('customer_name') then data_content end) as customer_name
        ,max(case when field_name_id in ('department_c2') then data_content end) as department_c2
        ,max(case when field_name_id in ('dikenal_dengan_bh') then data_content end) as dikenal_dengan_bh
        ,max(case when field_name_id in ('dp') then data_content end) as dp
        ,max(case when field_name_id in ('family') then data_content end) as family
        ,max(case when field_name_id in ('fasilitas_umum_terdekat_gh') then data_content end) as fasilitas_umum_terdekat_gh
        ,max(case when field_name_id in ('fixline_so') then data_content end) as fixline_so
        ,max(case when field_name_id in ('floor_c2') then data_content end) as floor_c2
        ,max(case when field_name_id in ('hubungan_cust_dengan_an_stnk') then data_content end) as hubungan_cust_dengan_an_stnk
        ,max(case when field_name_id in ('industry_name_c2') then data_content end) as industry_name_c2
        ,max(case when field_name_id in ('jabatan_go') then data_content end) as jabatan_go
        ,max(case when field_name_id in ('jenis_industri_go') then data_content end) as jenis_industri_go
        ,max(case when field_name_id in ('jumlah_karyawan_go') then data_content end) as jumlah_karyawan_go
        ,max(case when field_name_id in ('jumlah_pegawai_sb') then data_content end) as jumlah_pegawai_sb
        ,max(case when field_name_id in ('jumlah_tanggungan_bh') then data_content end) as jumlah_tanggungan_bh
        ,max(case when field_name_id in ('kecamatan_c2') then data_content end) as kecamatan_c2
        ,max(case when field_name_id in ('kelurahan_c2') then data_content end) as kelurahan_c2
        ,max(case when field_name_id in ('kode_pos_c2') then data_content end) as kode_pos_c2
        ,max(case when field_name_id in ('kode_surveyor_c2') then data_content end) as kode_surveyor_c2
        ,max(case when field_name_id in ('lama_kantor_berdiri_po') then data_content end) as lama_kantor_berdiri_po
        ,max(case when field_name_id in ('lama_kerja_so') then data_content end) as lama_kerja_so
        ,max(case when field_name_id in ('lama_tinggal_bh') then data_content end) as lama_tinggal_bh
        ,max(case when field_name_id in ('lama_usaha_berdiri_go') then data_content end) as lama_usaha_berdiri_go
        ,max(case when field_name_id in ('lama_usaha_berdiri_pb') then data_content end) as lama_usaha_berdiri_pb
        ,max(case when field_name_id in ('months_in_business_c2') then data_content end) as months_in_business_c2
        ,max(case when field_name_id in ('nama_agent') then data_content end) as nama_agent
        ,max(case when field_name_id in ('nama_gedung_c1') then data_content end) as nama_gedung_c1
        ,max(case when field_name_id in ('negative_survey_c2') then data_content end) as negative_survey_c2
        ,max(case when field_name_id in ('nik_so') then data_content end) as nik_so
        ,max(case when field_name_id in ('note_survey_c2') then data_content end) as note_survey_c2
        ,max(case when field_name_id in ('notes_bentuk_gedung_c1') then data_content end) as notes_bentuk_gedung_c1
        ,max(case when field_name_id in ('notes_gedung_c1') then data_content end) as notes_gedung_c1
        ,max(case when field_name_id in ('notes_problem') then data_content end) as notes_problem
        ,max(case when field_name_id in ('notes_ruko_c2') then data_content end) as notes_ruko_c2
        ,max(case when field_name_id in ('notes_rumah_c2') then data_content end) as notes_rumah_c2
        ,max(case when field_name_id in ('number_of_employee_c1') then data_content end) as number_of_employee_c1
        ,max(case when field_name_id in ('pagar_rumah_bh') then data_content end) as pagar_rumah_bh
        ,max(case when field_name_id in ('parent') then data_content end) as parent
        ,max(case when field_name_id in ('patokan_c1') then data_content end) as patokan_c1
        ,max(case when field_name_id in ('pic') then data_content end) as pic
        ,max(case when field_name_id in ('posisi_rumah_bh') then data_content end) as posisi_rumah_bh
        ,max(case when field_name_id in ('profesi_go') then data_content end) as profesi_go
        ,max(case when field_name_id in ('question_bpkb_home') then data_content end) as question_bpkb_home
        ,max(case when field_name_id in ('question_guarantor_home') then data_content end) as question_guarantor_home
        ,max(case when field_name_id in ('question_guarantor_office') then data_content end) as question_guarantor_office
        ,max(case when field_name_id in ('question_personal_bussines') then data_content end) as question_personal_bussines
        ,max(case when field_name_id in ('question_personal_home') then data_content end) as question_personal_home
        ,max(case when field_name_id in ('question_personal_office') then data_content end) as question_personal_office
        ,max(case when field_name_id in ('question_spouse_bussines') then data_content end) as question_spouse_bussines
        ,max(case when field_name_id in ('question_spouse_home') then data_content end) as question_spouse_home
        ,max(case when field_name_id in ('question_spouse_office') then data_content end) as question_spouse_office
        ,max(case when field_name_id in ('reason_asset_digunakan_siapa') then data_content end) as reason_asset_digunakan_siapa
        ,max(case when field_name_id in ('reason_company_cabang') then data_content end) as reason_company_cabang
        ,max(case when field_name_id in ('reason_company_pusat') then data_content end) as reason_company_pusat
        ,max(case when field_name_id in ('reason_kegunaan_asset') then data_content end) as reason_kegunaan_asset
        ,max(case when field_name_id in ('rt_c2') then data_content end) as rt_c2
        ,max(case when field_name_id in ('rw_c2') then data_content end) as rw_c2
        ,max(case when field_name_id in ('scoring') then data_content end) as scoring
        ,max(case when field_name_id in ('spouse') then data_content end) as spouse
        ,max(case when field_name_id in ('status_kekaryawanan_po') then data_content end) as status_kekaryawanan_po
        ,max(case when field_name_id in ('status_kepemilikan_rumah_bh') then data_content end) as status_kepemilikan_rumah_bh
        ,max(case when field_name_id in ('stnk') then data_content end) as stnk
        ,max(case when field_name_id in ('sub_industry_c1') then data_content end) as sub_industry_c1
        ,max(case when field_name_id in ('submite_date') then data_content end) as submite_date
        ,max(case when field_name_id in ('sumber_informasi') then data_content end) as sumber_informasi
        ,max(case when field_name_id in ('third_party') then data_content end) as third_party
        ,max(case when field_name_id in ('tipe_data') then data_content end) as tipe_data
        ,max(case when field_name_id in ('tipe_rumah_bh') then data_content end) as tipe_rumah_bh
        ,max(case when field_name_id in ('unit_digunakan_untuk_c1') then data_content end) as unit_digunakan_untuk_c1
        ,max(case when field_name_id in ('validasi_alamat_c2') then data_content end) as validasi_alamat_c2
        ,max(case when field_name_id in ('validasi_alamat_pb') then data_content end) as validasi_alamat_pb
        ,max(case when field_name_id in ('website_perusahaan_c1') then data_content end) as website_perusahaan_c1
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
		,c.campaign_id
        ,c.alamat_c2
        ,c.application_number
        ,c.area
        ,c.asset_digunakan_dimana
        ,c.assignment_date
        ,c.cabang
        ,c.cat_rumah_gh
        ,c.child
        ,c.choice_employee
        ,c.choice_investor
        ,c.choice_self_employee
        ,c.company_c1
        ,c.company_name_c2
        ,c.customer
        ,c.customer_name
        ,c.department_c2
        ,c.dikenal_dengan_bh
        ,c.dp
        ,c.family
        ,c.fasilitas_umum_terdekat_gh
        ,c.fixline_so
        ,c.floor_c2
        ,c.hubungan_cust_dengan_an_stnk
        ,c.industry_name_c2
        ,c.jabatan_go
        ,c.jenis_industri_go
        ,c.jumlah_karyawan_go
        ,c.jumlah_pegawai_sb
        ,c.jumlah_tanggungan_bh
        ,c.kecamatan_c2
        ,c.kelurahan_c2
        ,c.kode_pos_c2
        ,c.kode_surveyor_c2
        ,c.lama_kantor_berdiri_po
        ,c.lama_kerja_so
        ,c.lama_tinggal_bh
        ,c.lama_usaha_berdiri_go
        ,c.lama_usaha_berdiri_pb
        ,c.months_in_business_c2
        ,c.nama_agent
        ,c.nama_gedung_c1
        ,c.negative_survey_c2
        ,c.nik_so
        ,c.note_survey_c2
        ,c.notes_bentuk_gedung_c1
        ,c.notes_gedung_c1
        ,c.notes_problem
        ,c.notes_ruko_c2
        ,c.notes_rumah_c2
        ,c.number_of_employee_c1
        ,c.pagar_rumah_bh
        ,c.parent
        ,c.patokan_c1
        ,c.pic
        ,c.posisi_rumah_bh
        ,c.profesi_go
        ,c.question_bpkb_home
        ,c.question_guarantor_home
        ,c.question_guarantor_office
        ,c.question_personal_bussines
        ,c.question_personal_home
        ,c.question_personal_office
        ,c.question_spouse_bussines
        ,c.question_spouse_home
        ,c.question_spouse_office
        ,c.reason_asset_digunakan_siapa
        ,c.reason_company_cabang
        ,c.reason_company_pusat
        ,c.reason_kegunaan_asset
        ,c.rt_c2
        ,c.rw_c2
        ,c.scoring
        ,c.spouse
        ,c.status_kekaryawanan_po
        ,c.status_kepemilikan_rumah_bh
        ,c.stnk
        ,c.sub_industry_c1
        ,c.submite_date
        ,c.sumber_informasi
        ,c.third_party
        ,c.tipe_data
        ,c.tipe_rumah_bh
        ,c.unit_digunakan_untuk_c1
        ,c.validasi_alamat_c2
        ,c.validasi_alamat_pb
        ,c.website_perusahaan_c1
		,d.[Connect Response]
		,d.[Contact Response]
		,d.[Description]
		,e.[Campaign Result]
		,e.[Campaign Status]
		-- ,d.[Last Response Time]
		,e.[Agent Id]
		,d.[Appointment Date]
		,d.[Appointment Time]
		,d.[Attempt Call]
		,uploaddate = getdate()
	from prospect a
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