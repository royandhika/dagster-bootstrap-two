{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['salesorderno'],
        group='pss_awo_pkb',
        tags=['daily']
    )
}}

with salesorder as (
    select 
        * 
        ,rank = row_number() over(partition by salesorderno order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_pkb_salesorder_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,clean as (
    select 
        salesorderid = id
        ,salesorderno
        ,pkbid
        ,pkbno
        ,so
        ,createdtime
        ,lastmodifiedtime
        ,uploaddate = getdate()
    from salesorder
    where rank = 1
)
select * from clean