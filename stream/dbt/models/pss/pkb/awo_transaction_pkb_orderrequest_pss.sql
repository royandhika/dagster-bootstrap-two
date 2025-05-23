{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['orderno'],
        group='pss_awo_pkb',
        tags=['daily']
    )
}}

with orderrequest as (
    select 
        * 
        ,rank = row_number() over(partition by orderno order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_pkb_orderrequest_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,clean as (
    select 
        orderid = id
        ,orderno
        ,so
        ,activitydocumentid
        ,orderstatus
        ,orderdocumentdate
        -- ,createdby
        -- ,lastmodifiedby
        ,createdtime
        ,lastmodifiedtime
        ,uploaddate = getdate()
    from orderrequest
    where rank = 1
)
select * from clean