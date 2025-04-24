{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['id'],
        group='outbound_deskcollfif',
		tags=['outbound']
	)
}}

select
	a.no_lkd 
	,a.no_seq
	,a.id
	,d.stg_overdue
	,d.stg_inst_no
	,a.call_result 
	,a.contract_number 
	,b.start_call
	,b.end_call
	,duration = datediff(second, b.start_call, b.end_call)
	,d.stg_obj_code
	,a.ptp_date
	,ptp_date2 = case 
		when a.call_result = 'PTD-D' then format(a.created_time, 'dd/MM/yyyy')
		when a.call_result = 'PTP' then format(ptp_date, 'dd/MM/yyyy')
	end
	,a.notepad
	,a.phone_number 
	,a.call_time
	,a.user_id 
	,b.cycle
	,d.stg_sub_type
	,d.potensi
	,d.stg_kode_cabang_fif
	,d.stg_tanggal_lkd
	,class = ''
	,delq = ''
	,uploaddate = getdate()
from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcollfif_acs_call_history_daily_intelix') }} a
left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcollfif_acs_call_history_detail_daily_intelix') }} b
	on b.call_history_id = a.id 
left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcollfif_acs_reference_intelix') }} c
	on c.reference = 'CALL_RESULT' 
  	and c.value = a.call_result 
left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcollfif_acs_customer_profile_ext_intelix') }} d 
	on d.contract_number = a.contract_number
where a.stg_obj_code is not null 
    {% if is_incremental() %}
    and a.created_time >= '{{ var('min_date') }}'
    and a.created_time <= '{{ var('max_date') }}'
    {% endif %}