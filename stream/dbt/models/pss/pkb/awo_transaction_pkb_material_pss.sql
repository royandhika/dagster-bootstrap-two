{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['pkbmaterialid', 'so'],
        group='pss_awo_pkb',
        tags=['daily']
    )
}}

with material as (
    select 
        * 
        ,rank = row_number() over(partition by id, so order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_pkb_material_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,clean as (
    select 
        pkbmaterialid = id
        ,pkbno
        ,so
        ,pkboperationid
        ,pkboperationitem
        ,materialid
        ,materialitem = item
        ,materialdesc = materialdescription
        ,materialitemstatus
        ,createdtime
        ,lastmodifiedtime
        ,uploaddate = getdate()
    from material
    where rank = 1
)
select * from clean