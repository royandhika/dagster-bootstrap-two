{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['dealercode'],
        group='pss_awo_master',
        tags=['daily']
    )
}}

with salesoffice as (
	select 
        *
        ,rank = row_number() over(partition by id order by coalesce(lastmodifiedtime, createdtime) desc) 
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_detail_dealer_salesoffice_pss_staging') }}
)
,businessarea as (
	select 
        * 
        ,rank = row_number() over(partition by id order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_detail_dealer_businessarea_pss_staging') }}
)
,city as (
    select * from {{ ref('awo_detail_city_pss') }}
)
,region as (
    select * from {{ ref('awo_detail_region_pss') }}
)
,so_clean as (
    select * 
    from salesoffice 
    where rank = 1
)
,ba_clean as (
    select * 
    from businessarea 
    where rank = 1
)
,dealer as (
    select 
        businessareaid = b.id
        ,salesofficeid = a.id 
        ,dealercode = coalesce(businessareacode, salesofficecode)
        ,dealerdesc = coalesce(businessareadescription, salesofficedescription)
        ,companycode = case 
            when companycodeid = 1 then '0001' 
            when companycodeid = 2 then '0002' 
            when companycodeid = 3 then '0003' 
            when companycodeid = 4 then '0004' 
            when companycodeid = 5 then '0005' 
            when companycodeid = 6 then '0006' 
            when companycodeid = 7 then '0015' 
            when companycodeid = 8 then '0172' 
            when companycodeid = 9 then '0173' 
            when companycodeid = 10 then '0174' 
            when companycodeid = 11 then '0363' 
            when companycodeid = 57 then 'A000' 
            when companycodeid = 58 then '0362' 
            when companycodeid = 66 then '0007' 
        end
        ,companydesc = case 
            when companycodeid = 1 then 'Astra Intrl - Head Office' 
            when companycodeid = 2 then 'Astra Intrl - Toyota SO' 
            when companycodeid = 3 then 'Astra Intrl - Daihatsu SO' 
            when companycodeid = 4 then 'Astra Intrl - Isuzu SO' 
            when companycodeid = 5 then 'Astra Intrl - Honda SO' 
            when companycodeid = 6 then 'Astra Intrl - Multi Br SO' 
            when companycodeid = 7 then 'Astra Intrl - Share Service' 
            when companycodeid = 8 then 'Astra Intrl - UD Trucks SO' 
            when companycodeid = 9 then 'Astra Intrl - BMW SO' 
            when companycodeid = 10 then 'Astra Intrl - Peugeot SO' 
            when companycodeid = 11 then 'Astra Intrl - Astraworld' 
            when companycodeid = 57 then 'Astra Intrl' 
            when companycodeid = 58 then 'Astra Intrl - Astra Parts' 
            when companycodeid = 66 then 'Astra Intrl - Lexus SO' 
        end
        ,a.cityid
        -- ,b.createdtime
        -- ,b.lastmodifiedtime
        -- ,uploaddate = getdate()
    from so_clean a
    full outer join ba_clean b
        on a.salesofficecode = b.businessareacode 
)
,final as (
    select 
        a.businessareaid
        ,a.salesofficeid 
        ,a.dealercode 
        ,a.dealerdesc 
        ,a.companycode 
        ,a.companydesc 
        ,b.citycode 
        ,b.citydesc 
        ,c.regioncode 
        ,c.regiondesc
    from dealer a  
    left join city b 
        on a.cityid = b.id
    left join region c 
        on b.regioncode = c.regioncode
)
select * from final