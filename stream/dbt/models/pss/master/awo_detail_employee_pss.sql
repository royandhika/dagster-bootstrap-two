{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['employeeno'],
        group='pss_awo_master',
        tags=['daily']
    )
}}

with employee as (
	select 
        * 
        --employeeno tidak beraturan, coba dibersihkan
        ,rank = row_number() over(partition by right(replace(employeeno, ' ', ''), 6) order by createdtime desc, lastmodifiedtime desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_detail_employee_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,position as (
    select * from {{ ref('awo_detail_position_pss') }}
)
,dealer as (
    select * from {{ ref('awo_detail_dealer_pss') }}
)
,clean as (
    select 
        a.id
        ,employeeno = right(replace(a.employeeno, ' ', ''), 6)    --fix whitespace
        ,a.employeename
        ,gender = case 
            when a.genderid = 1 then 'MALE'
            when a.genderid = 2 then 'FEMALE'
        end 
        ,a.employeestatusid 
        ,a.businesspartnernoid
        ,b.dealercode
        ,c.positioncode 
        ,c.positiondesc 
        ,a.validfrom 
        ,a.validto 
        ,a.title 
        ,a.mechanicstatus
        ,a.initial
        ,a.createdtime
        ,a.lastmodifiedtime
        ,uploaddate = getdate()
    from employee a 
    left join dealer b 
        on a.businessareaid = b.businessareaid
    left join position c 
        on a.positionid = c.id
    where rank = 1
)
select * from clean