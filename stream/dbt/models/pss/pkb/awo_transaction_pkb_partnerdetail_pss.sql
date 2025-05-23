{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['pkbno', 'partnercode'],
        group='pss_awo_pkb',
        tags=['daily']
    )
}}

with partner as (
    select 
        * 
        ,rank = row_number() over(partition by pkbno, partnerfunctioncode order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_pkb_partnerdetail_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,city as (
    select * from {{ ref('awo_detail_city_pss') }}
)
,clean as (
    select 
        a.id
        ,pkbid = a.documentid
        ,pkbno
        ,a.so
        ,partnercode = a.partnerfunctioncode
        ,partnerdesc = a.partnerfunctiondescription
        ,a.customerid
        ,customerno = a.partnerno
        ,customername = a.partnername
        ,a.address
        ,telephone = dbo.clean_phone_number(a.telephone)
        ,c.citycode
        ,c.citydesc
        ,a.postalcode
        ,a.createdtime
        ,a.lastmodifiedtime
        ,uploaddate = getdate()
    from partner a
    left join city c 
        on a.cityid = c.id
    where a.rank = 1
)
select * from clean