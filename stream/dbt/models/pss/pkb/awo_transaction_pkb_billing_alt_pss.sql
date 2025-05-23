{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['billingno'],
        group='pss_awo_pkb',
        tags=['daily']
    )
}}

with billing as (
    select 
        * 
        ,rank = row_number() over(partition by billingno order by billingdate desc, synchronizedon desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_pkb_billing_alt_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(synchronizedon, billingdate) >= '{{ var('min_date') }}'
        and coalesce(synchronizedon, billingdate) <= '{{ var('max_date') }}'
    {% endif %}
)
,chargeto as (
    select *
    from {{ ref('awo_detail_chargeto_pss') }}
)
,clean as (
    select
        billingid = a.id 
        ,billingno 
        ,pkbno 
        ,salesorderno 
        ,so 
        ,billingstatus 
        ,billingtypecode 
        ,billingtypedesc = billingtypedescription 
        ,totalincvat = totaltotalincvat 
        ,a.chargetocode
        ,b.chargetodesc
        ,billingdate 
        ,pkbdate 
        ,createdtime = billingdate 
        ,lastmodifiedtime = case 
            when synchronizedon = '1900-01-01 00:00:00.000' then null 
            else synchronizedon
        end
        ,uploaddate = getdate()
    from billing a
    left join chargeto b 
        on a.chargetocode = b.chargetocode
    where a.rank = 1
)
select * from clean