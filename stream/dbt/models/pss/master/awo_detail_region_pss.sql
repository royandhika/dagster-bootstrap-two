{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['id'],
        group='pss_awo_master',
        tags=['daily']
    )
}}

with dl as (
	select 
        *
        ,rank = row_number() over(partition by id order by coalesce(lastmodifiedtime, createdtime) desc) 
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_detail_region_pss_staging') }}
    {% if is_incremental() %}
    where convert(date, uploaddate) = convert(date, getdate())
    {% endif %}
)
,clean as (
    select 
        id
        ,regioncode
        ,regiondesc = upper(regiondescription)
        ,createdtime
        ,lastmodifiedtime
        ,uploaddate = getdate()
    from dl 
    where rank = 1
)
select * from clean