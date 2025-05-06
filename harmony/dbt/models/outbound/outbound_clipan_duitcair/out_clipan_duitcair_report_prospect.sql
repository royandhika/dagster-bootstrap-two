{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['agreement_no', 'data_supply_id'],
        group='outbound_clipan_duitcair',
		tags=['12hourly']
	)
}}

with src as (
    select
        *
        ,rn = row_number() over(partition by agreement_no, data_supply_id order by uploaddate desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_clipan_duitcair_report_prospect_api') }}
	{% if is_incremental() %}
	where coalesce(interaction_date, data_supply_date) >= '{{ var('min_date') }}'
		and coalesce(interaction_date, data_supply_date) <= '{{ var('max_date') }}'
	{% endif %}
)
,final as (
    select 
        [_update]
        ,agreement_no
        ,assigned_at
        ,attempt
        ,bank
        ,bank_account_name
        ,batch_id
        ,bpkb_no
        ,branch
        ,brand
        ,call_result_text
        ,campaign
        ,campaign_id
        ,campaign_name
        ,campaign_result
        ,campaign_result_detail
        ,campaign_team_id
        ,campaign_team_name
        ,category
        ,company_address
        ,company_name
        ,complete_legal_address
        ,complete_residence_address
        ,connected_result
        ,contacted_result
        ,contract_status
        ,current_assigned_agent_email
        ,current_assigned_agent_id
        ,current_assigned_agent_name
        ,data_supply_date
        ,data_supply_id
        ,data_supply_source
        ,data_supply_submission_id
        ,debitur_account_no
        ,debitur_name
        ,debitur_type
        ,dob
        ,effective_rate
        ,home_ownership
        ,home_phone
        ,id_expired_date
        ,id_number
        ,id_spouse
        ,id_spouse_expired_date
        ,import_data_supply_id
        ,industry_type
        ,installment_amount
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
        ,job_position
        ,job_type
        ,last_month_current_assigned_agent_email
        ,last_month_current_assigned_agent_id
        ,last_month_current_assigned_agent_name
        ,length_of_stay
        ,length_of_work
        ,machine_number
        ,marriage_status
        ,max_overdue
        ,max_overdue_last_6_month
        ,mobile_phone
        ,mobile_phone_2
        ,model_cat
        ,model_type
        ,monthly_fixed_income
        ,monthly_spouse_income
        ,monthly_variable_income
        ,mother_name
        ,ntf
        ,original_assigned_agent_email
        ,original_assigned_agent_id
        ,original_assigned_agent_name
        ,other_income
        ,passenger_commercial
        ,police_number
        ,prepayment_amount
        ,prioritas
        ,ready_release_document_date
        ,reassignment_total
        ,remaining_tenor
        ,spouse_name
        ,status
        ,submission_user_id
        ,tenor
        ,ticket_number
        ,tipe_data
        ,valid_day_total
        ,valid_day_until
        ,vehicle_color
        ,vehicle_year
        ,vin
        ,uploaddate = getdate()
    from src 
    where rn = 1
)
select * from final