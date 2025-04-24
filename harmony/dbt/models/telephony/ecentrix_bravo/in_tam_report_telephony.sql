{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_dm',
        unique_key=['id'],
        group='ecentrix_bravo',
		tags=['telephony']
    )
}}

with src as (
    select 
        a.id
        ,[Caller Number] = a_number
        ,Extension = extension_id 
        ,Agent = agent_id 
        ,Direction
        ,[Date] = convert(date, last_update_time)
        ,[Time] = format(last_update_time, 'HH:mm:ss')
        ,[IVR Time] = ivr_time 
        ,acd_response_time = acd_time 
        ,queue_time
        ,ring_time
        ,talk_time
        ,hold_time
        ,originate_time
        ,last_status = b.description 
        ,hangup_side
        ,report_time = last_update_time 
        ,uploaddate = getdate()
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'ecentrix_bravo_ecentrix_session_log_intelix') }} a
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'ecentrix_bravo_ecentrix_reference_intelix') }} b 
        on a.last_status = b.code
        and b.type = 'CALL_STATUS'
    where context = 'inbound-tam'
        {% if is_incremental() %}
        and last_update_time >= '{{ var('min_date') }}'
    	and last_update_time <= '{{ var('max_date') }}'
		{% endif %}
)
select * from src