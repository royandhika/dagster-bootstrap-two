{{
    config(
        schema=env_var('ENV_SCHEMA') + '_dm',
        group='ecentrix_alpha'
    )
}}

select top 100 a.*
from {{ source(env_var('ENV_SCHEMA') + '_dl', 'ecentrix_alpha_ecentrix_session_log_intelix') }} a
left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'ecentrix_alpha_ecentrix_reference_intelix') }} b
    on a.id = b.id
where a.last_update_time >= '{{ var('min_date') }}' 
    and a.last_update_time <= '{{ var('max_date') }}'