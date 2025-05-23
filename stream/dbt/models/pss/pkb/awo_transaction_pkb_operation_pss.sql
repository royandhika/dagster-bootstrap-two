{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['pkboperationid', 'so'],
        group='pss_awo_pkb',
        tags=['daily']
    )
}}

with partner as (
    select 
        * 
        ,rank = row_number() over(partition by id, so order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_pkb_operation_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,jobtype as (
    select * from {{ ref('awo_detail_jobtype_pss') }}
)
,activitytype as (
    select * from {{ ref('awo_detail_activitytype_pss') }}
)
,clean as (
    select 
        pkboperationid = a.id
        ,pkbid
        ,pkbno
        ,so
        ,mechanicid
        ,b.jobtypecode 
        ,b.jobtypedesc
        ,c.activitytypecode 
        ,c.activitytypedesc
        ,operationitem = item
        ,operationdesc = operationdescription
        ,operationitemstatus
        ,pm = case 
            when right(dbo.clean_number(b.jobtypecode), 2) = '00' then 'PM' 
            when b.jobtypecode like 'PERIOD%' then 'PM'
        end
        ,pmcategory = case 
            when right(dbo.clean_number(b.jobtypecode), 2) = '00' then dbo.clean_number(b.jobtypedesc)
            when b.jobtypecode like 'PERIOD%' 
                and charindex('/', operationdescription) = 0 
                and len(dbo.clean_number(operationdescription)) <= 6 
                and try_convert(bigint, dbo.clean_number(operationdescription)) >= 1000 
                and right(dbo.clean_number(operationdescription), 2) = '00' then dbo.clean_number(operationdescription) 
        end 
        ,issuggested 
        ,isdrop
        ,a.createdtime
        ,a.lastmodifiedtime
        ,uploaddate = getdate()
    from partner a
    left join jobtype b
        on a.jobtypeid = b.id 
    left join activitytype c 
        on a.typeid = c.id
    where a.rank = 1
)
select * from clean