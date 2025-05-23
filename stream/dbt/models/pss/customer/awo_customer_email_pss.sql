{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['customerno'],
        group='pss_awo_customer',
        tags=['daily']
    )
}}

with customer as (
    select 
        *
        ,rn = row_number() over(partition by customerno order by coalesce(lastmodifiedtime, createdtime) desc) 
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_customer_email_pss_staging') }}
    where isdefault = 1
        {% if is_incremental() %}
        and coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
        {% endif %}
)
,clean as (
    select
        customeremailid = id
        ,customeraddressid
        ,customerno
        ,email = lower(dbo.clean_email_address(email))
        ,isdefault
        ,createdtime
        ,lastmodifiedtime
        ,uploaddate = getdate()
    from customer 
    where rn = 1
)
select * from clean