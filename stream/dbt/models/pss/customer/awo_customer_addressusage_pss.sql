{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['customeraddressusageid'],
        group='pss_awo_customer',
        tags=['daily']
    )
}}

with customer as (
    select 
        * 
        ,rn = row_number() over(partition by id order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_customer_addressusage_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,clean as (
    select
        customeraddressusageid = id
        ,customeraddressid
        ,customerid
        ,customerno
        ,typeofaddress
        ,createdtime
        ,lastmodifiedtime
        ,uploaddate = getdate()
    from customer 
    where rn = 1
)
select * from clean