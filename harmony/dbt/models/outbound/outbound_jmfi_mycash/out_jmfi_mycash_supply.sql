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
        ,rn = row_number() over(partition by data_supply_id order by uploaddate desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_jmfi_mycash_supply_api') }}
	{% if is_incremental() %}
	where coalesce(last_daily_result_api_sent_at, last_call_attempt_at, data_supply_date) >= '{{ var('min_date') }}'
		and coalesce(last_daily_result_api_sent_at, last_call_attempt_at, data_supply_date) <= '{{ var('max_date') }}'
	{% endif %}
)
,final as (
    select 
        agreement_no
        ,appointment_date = try_convert(datetime, appointment_date)
        ,appointment_total
        ,area_business
        ,asset_code
        ,assignment_date = try_convert(datetime, assignment_date)
        ,book_criteria_cust
        ,branch_full_name_2
        ,call_attempt_total
        ,cat_product
        ,current_assigned_agent_email
        ,current_assigned_agent_id
        ,current_assigned_agent_name
        ,customer_id
        ,customer_name
        ,data_supply_date = try_convert(datetime, data_supply_date)
        ,data_supply_id
        ,data_supply_submission_id
        ,hot_prospect_api_sent_at = try_convert(datetime, hot_prospect_api_sent_at)
        ,is_appointment
        ,is_assigned
        ,is_current_assigned_agent_same_as_last_month
        ,is_hot_prospect
        ,is_interest_waiting_for_order_need_follow_up
        ,is_need_follow_up
        ,is_no_need_to_call
        ,is_original_assigned_agent_same_as_last_month
        ,is_reassigned
        ,last_call_attempt_at = try_convert(datetime, last_call_attempt_at)
        ,last_daily_result_api_sent_at = try_convert(datetime, last_daily_result_api_sent_at)
        ,last_interaction_id
        ,last_month_current_assigned_agent_email
        ,last_month_current_assigned_agent_id
        ,last_month_current_assigned_agent_name
        ,original_assigned_agent_email
        ,original_assigned_agent_id
        ,original_assigned_agent_name
        ,profession
        ,real_category_product
        ,reassignment_total
        ,tenor
        ,tenor_left
        ,valid_status
        ,uploaddate = getdate()
    from src 
    where rn = 1
)
select * from final