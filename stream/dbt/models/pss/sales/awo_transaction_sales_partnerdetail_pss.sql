{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['equipmentno', 'partnercode'],
        group='pss_awo_sales',
        tags=['daily']
    )
}}

with partner as (
    select 
        * 
        ,rank = row_number() over(partition by equipmentno, partnerfunctionid order by createdtime desc, lastmodifiedtime desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_sales_partnerdetail_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,detail as (
    select * from {{ ref('awo_detail_partnerfunction_pss') }}
)
,clean as (
    select 
        a.id
        ,a.equipmentid
        ,a.equipmentno
        ,partnercode = b.partnerfunctioncode
        ,partnerdesc = b.partnerfunctiondesc
        ,a.customerid
        ,a.customerno
        ,a.vendorid
        ,a.vendorno
        ,a.employeeid
        ,a.employeeno
        ,a.createdtime
        ,a.lastmodifiedtime
        ,uploaddate = getdate()
    from partner a
    left join detail b
        on a.partnerfunctionid = b.id 
    where a.rank = 1
)
select * from clean