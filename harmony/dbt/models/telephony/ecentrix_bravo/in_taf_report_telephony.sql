{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_dm',
        unique_key=['id'],
        group='ecentrix_bravo',
		tags=['12hourly']
    )
}}

with src as (
    select 
        a.id
        ,[A-number] = a.a_number
        ,[Ext Id] = a.extension_id
        ,agent_id = a.agent_id 
        ,direction = a.direction 
        ,start_time = a.start_time 
        ,end_time = a.end_time 
        ,ivr_time = a.ivr_time
        ,acd_response_time = a.acd_time
        ,queue_time = a.queue_time
        ,ring_time = a.ring_time
        ,talk_time = a.talk_time
        ,originate_time = a.originate_time
        ,last_status = b.description
        ,context = a.context 
        ,report_time = a.start_time 
        ,uploaddate = getdate()
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'ecentrix_bravo_ecentrix_session_log_intelix') }} a
    left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'ecentrix_bravo_ecentrix_reference_intelix') }} b 
        on a.last_status = b.code
        and b.type = 'CALL_STATUS'
    where context = 'inbound-taf'
        {% if is_incremental() %}
        and last_update_time >= '{{ var('min_date') }}'
    	and last_update_time <= '{{ var('max_date') }}'
		{% endif %}
)
select * from src