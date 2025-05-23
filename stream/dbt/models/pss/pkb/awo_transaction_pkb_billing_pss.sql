{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['billingno', 'billingitem'],
        group='pss_awo_pkb',
        tags=['daily']
    )
}}

with billing as (
    select 
        * 
        ,rank = row_number() over(partition by billingno, billingitem order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_pkb_billing_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,billingtype as (
    select * from {{ ref('awo_detail_billingtype_pss') }}
)
,clean as (
    select 
        billingid = a.id
        ,a.billingno
        ,cancellationdocid
        ,a.pkbno
        ,a.salesorderserviceid
        ,a.so
        ,a.billingstatus
        ,b.billingtypecode
        ,b.billingtypedesc
        ,a.salesorderitem
        ,a.billingitem
        ,a.materialnumber
        ,a.billingitemtext
        ,a.totalincvat
        ,a.createdtime
        ,a.lastmodifiedtime
        ,uploaddate = getdate()
    from billing a
    left join billingtype b
      on a.billingtypeid = b.id
    where a.rank = 1
)
select * from clean