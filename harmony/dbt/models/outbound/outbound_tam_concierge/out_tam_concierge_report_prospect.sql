{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['data_supply_id'],
        group='outbound_tam_concierge',
		tags=['12hourly']
	)
}}

with src as (
    select
        *
        ,rn = row_number() over(partition by data_supply_id order by uploaddate desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_tam_concierge_report_prospect_api') }}
	{% if is_incremental() %}
	where uploaddate >= '{{ var('min_date') }}'
		and uploaddate <= '{{ var('max_date') }}'
	{% endif %}
)
,final as (
    select 
        assigned_at
        ,attempt
        ,call_result_text
        ,campaign_id
        ,campaign_name
        ,campaign_result
        ,campaign_result_detail
        ,campaign_team_id
        ,campaign_team_name
        ,category
        ,color
        ,connected_result
        ,contacted_result
        ,current_assigned_agent_email
        ,current_assigned_agent_id
        ,current_assigned_agent_name
        ,data_supply_date
        ,data_supply_id
        ,data_supply_source
        ,data_supply_submission_id
        ,date_received
        ,driver_number
        ,id_concierge
        ,import_data_supply_id
        ,interaction_agent_email
        ,interaction_agent_id
        ,interaction_agent_name
        ,interaction_appointment_date
        ,interaction_branch_id
        ,interaction_branch_name
        ,interaction_date
        ,interaction_id
        ,interaction_phone_number
        ,interaction_source
        ,is_appointment
        ,is_assigned
        ,is_current_assigned_agent_same_as_last_month
        ,is_done
        ,is_hot_prospect
        ,is_need_follow_up
        ,is_no_need_to_call
        ,is_original_assigned_agent_same_as_last_month
        ,is_reassigned
        ,keluhan
        ,last_month_current_assigned_agent_email
        ,last_month_current_assigned_agent_id
        ,last_month_current_assigned_agent_name
        ,location
        ,name
        ,original_assigned_agent_email
        ,original_assigned_agent_id
        ,original_assigned_agent_name
        ,owner_number
        ,police_number
        ,reassignment_total
        ,service_type
        ,specialist
        ,submission_user_id
        ,tahun_pembelian
        ,ticket_number
        ,type_data
        ,valid_day_total
        ,valid_day_until
        ,vin
        ,uploaddate = getdate()
    from src 
    where rn = 1
)
select * from final