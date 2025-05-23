{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['bookingno'],
        group='pss_awo_pkb',
        tags=['daily']
    )
}}

with booking as (
    select 
        *
        ,rank = row_number() over(partition by bookingno order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_pkb_booking_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,clean as (
    select 
        bookingno 
        ,pkbno
        ,so
        ,equipmentno
        ,customerno 
        ,bookingstatus
        ,bookingdate
        ,createdtime 
        ,lastmodifiedtime 
        ,uploaddate = getdate()
    from booking a 
    where rank = 1
)
select * from clean