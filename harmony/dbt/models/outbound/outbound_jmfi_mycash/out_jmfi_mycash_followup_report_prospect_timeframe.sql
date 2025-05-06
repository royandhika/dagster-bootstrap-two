{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
        unique_key=['interaction_id'],
        group='outbound_jmfi_mycash',
		tags=['12hourly']
	)
}}

with src as (
	select 
		*
		,interest_date = min(case 
			when campaign_result_detail = 'Interest - Order' then interaction_date
			else '9999-12-31'
		end) over(partition by data_supply_id)
	from {{ ref('out_jmfi_mycash_report_prospect_timeframe') }}
	{% if is_incremental() %}
	where interaction_date >= '{{ var('min_date') }}'
		and interaction_date <= '{{ var('max_date') }}'
	{% endif %}
)
,final as (
	select 
		DataSupplyTenor
		,agreement_no
		,angsuran
		,appointment_date
		,area_business
		,asset_code
		,assignment_date
		,attempt = row_number() over(partition by data_supply_id, case 
			when interaction_date <= interest_date then 'regular'
			when interaction_date > interest_date then 'follow_up'
		end order by interaction_date asc)
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
		,data_supply_date
		,data_supply_id
		,data_supply_submission_id
		,disburse
		,interaction_date
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
		,valid_status
		,call_type = case 
			when interaction_date <= interest_date then 'regular'
			when interaction_date > interest_date then 'follow_up'
		end
		,uploaddate = getdate()
	from src
)
select * from final