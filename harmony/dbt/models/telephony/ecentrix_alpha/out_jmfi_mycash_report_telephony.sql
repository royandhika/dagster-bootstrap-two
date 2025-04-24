{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_dm',
        unique_key=['call_id'],
        group='ecentrix_alpha',
		tags=['telephony']
    )
}}

with src as (
    select 
        a.id
        ,call_id
        ,customer_data
        ,a_number
        ,[ext id] = extension_id
        ,agent_id
        ,direction
        ,start_time
        ,end_time
        ,ivr_time
        ,acd_response_time = acd_time
        ,queue_time
        ,ring_time
        ,talk_time
        ,originate_time
        ,last_status
        ,last_status_desc = b.description
        ,context
        ,report_time = start_time
        ,originate_ring_time
        ,service_time
        ,uploaddate = getdate()
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'ecentrix_alpha_ecentrix_session_log_intelix') }} a
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'ecentrix_alpha_ecentrix_reference_intelix') }} b 
        on a.last_status = b.code 
        and b.type = 'CALL_STATUS'
    where context = 'outbound-jmfi'
        {% if is_incremental() %}
        and last_update_time >= '{{ var('min_date') }}'
    	and last_update_time <= '{{ var('max_date') }}'
		{% endif %}
)
select * from src