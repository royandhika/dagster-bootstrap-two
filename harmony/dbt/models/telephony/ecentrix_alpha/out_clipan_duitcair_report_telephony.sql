{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_dm',
        unique_key=['call_id'],
        group='ecentrix_alpha',
		tags=['12hourly']
    )
}}

with src as (
    select 
        call_id
        ,a_number = dbo.clean_phone_number(a_number)
        ,channel
        ,extension_id
        ,agent_id
        ,context
        ,direction
        ,last_status
        ,b.description
        ,reason
        ,reason_text
        ,start_time
        ,end_time
        ,duration
        ,customer_data
        ,last_update_time
        ,hangup_side
        ,agent_log_id
        ,ivr_time
        ,queue_time
        ,acd_time
        ,ring_time
        ,service_time
        ,hold_time
        ,talk_time
        ,originate_ring_time
        ,originate_time
        ,flag_call_back
        ,uploaddate = getdate()
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'ecentrix_alpha_ecentrix_session_log_intelix') }}  a
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'ecentrix_alpha_ecentrix_reference_intelix') }} b 
        on a.last_status = b.code 
        and b.type = 'CALL_STATUS'
    where context = 'outbound-clipan'
        {% if is_incremental() %}
        and last_update_time >= '{{ var('min_date') }}'
    	and last_update_time <= '{{ var('max_date') }}'
		{% endif %}
)
select * from src