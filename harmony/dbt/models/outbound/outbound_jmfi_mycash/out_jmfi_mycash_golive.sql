{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['data_supply_id'],
        group='outbound_jmfi_mycash',
		tags=['12hourly']
	)
}}

with src as (
    select
        *
        ,rn = row_number() over(partition by agreement_no, notes order by uploaddate desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_jmfi_mycash_golive_api') }}
	{% if is_incremental() %}
	where coalesce(date_progress_result, interest_date) >= '{{ var('min_date') }}'
		and coalesce(date_progress_result, interest_date) <= '{{ var('max_date') }}'
	{% endif %}
)
,final as (
    select 
        data_supply_id
        ,area_business
        ,branch_full_name_2
        ,interest_date = convert(date, interest_date)
        ,agent_id
        ,customer_name
        ,agreement_no
        ,cat_product
        ,notes
        ,ticket_number
        ,whatsapp_cabang
        ,date_whatsapp_cabang = convert(date, date_whatsapp_cabang)
        ,notes_whatsapp_cabang
        ,bi_check
        ,date_bi_check = convert(date, date_bi_check)
        ,notes_bi_check
        ,survey_reason
        ,date_survey_reason = convert(date, date_survey_reason)
        ,notes_survey_reason
        ,progress_result
        ,date_progress_result = convert(date, date_progress_result)
        ,notes_progress_result
        ,reason
        ,date_reason = convert(date, date_reason)
        ,notes_reason
        ,nama_spv
        ,no_telp_spv
        ,golive_date = convert(date, golive_date)
        ,ntf_golive
        ,ntf_interest_order
        ,tanggal_janji_survey = convert(date, tanggal_janji_survey)
        ,agent_name
        ,agent_email
        ,lov_gabungan
        ,status_saat_ini
        ,uploaddate = getdate()
    from src 
    where rn = 1
)
select * from final