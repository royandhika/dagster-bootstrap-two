{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['agreement_no', 'data_supply_id'],
        group='outbound_jmfi_mycash',
		tags=['12hourly']
	)
}}

with src as (
    select
        *
        ,rn = row_number() over(partition by agreement_no, data_supply_id order by uploaddate desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_jmfi_mycash_report_prospect_api') }}
	{% if is_incremental() %}
	where uploaddate >= '{{ var('min_date') }}'
		and uploaddate <= '{{ var('max_date') }}'
	{% endif %}
)
,final as (
    select 
        DataSupplyTenor
        ,agreement_no
        ,angsuran
        ,appointment_date = try_convert(datetime, appointment_date)
        ,area_business
        ,asset_code
        ,assignment_date = try_convert(datetime, assignment_date)
        ,attempt
        ,book_criteria_cust
        ,branch_full_name_2
        ,campaign_result
        ,campaign_result_detail
        ,cat_product
        ,connected_result
        ,contacted_result
        ,current_assigned_agent_email
        ,current_assigned_agent_id
        ,current_assigned_agent_name
        ,customer_id
        ,customer_name
        ,data_supply_date = try_convert(datetime, data_supply_date)
        ,data_supply_id
        ,data_supply_submission_id
        ,disburse
        ,interaction_date = try_convert(datetime, interaction_date)
        ,interaction_id
        ,is_appointment
        ,is_hot_prospect
        ,is_interest_waiting_for_order_need_follow_up
        ,is_need_follow_up
        ,jenis
        ,merk
        ,notes
        ,otr
        ,phone_number
        ,profession
        ,reference_phone_number
        ,[source]
        ,status_kepemilikan_rumah
        ,tahun_kendaraan
        ,tenor
        ,tenor_left
        ,tipe
        ,uploaddate = getdate()
    from src 
    where rn = 1
)
select * from final