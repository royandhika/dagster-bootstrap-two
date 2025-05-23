{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['pkbno'],
        group='pss_awo_pkb',
        tags=['daily']
    )
}}

with pkb as (
    select 
        * 
        ,rn = row_number() over(partition by pkbno order by createdtime desc, lastmodifiedtime desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_pkb_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
-- ,dealer as (
--     select * from {{ ref('awo_detail_dealer_pss') }}
-- )
-- ,city as (
--     select * from {{ ref('awo_detail_city_pss') }}
-- )
-- ,region as (
--     select * from {{ ref('awo_detail_region_pss') }}
-- )
,pkbtype as (
    select 
        id
        ,pkbtypecode 
        ,pkbtypedesc
    from {{ ref('awo_detail_pkbtype_pss') }}
)
,clean as (
    select
        pkbno
        ,pkbid = a.id
        ,e.pkbtypecode
        ,e.pkbtypedesc 
        ,documentstatuscode = documentstatus
        ,documentstatusdesc = case 
            when documentstatus = 0 then 'Created'
            when documentstatus = 1 then 'Estimated'
            when documentstatus = 2 then 'Released'
            when documentstatus = 3 then 'Canceled'
            when documentstatus = 4 then 'WIP'
            when documentstatus = 5 then 'PartiallyConfirmed'
            when documentstatus = 6 then 'Confirmed'
            when documentstatus = 7 then 'Completed'
            when documentstatus = 8 then 'PartialBilled'
            when documentstatus = 9 then 'FinalBilled'
            when documentstatus = 10 then 'Closed'
        end
        ,equipmentid
        ,equipmentno = dbo.clean_alphanumeric(equipmentno)
        ,so = so2
        ,a.dealercode
        -- ,b.dealercode
        -- ,b.dealerdesc
        ,dealerdesc = a.dealerdescription
        -- ,c.citycode
        -- ,c.citydesc
        ,city = upper(a.dealercity)  --fix uppercase
        ,region = upper(a.dealerregion)  --fix uppercase
        -- ,d.regioncode
        -- ,d.regiondesc
        ,postalcode
        ,customerno
        ,decisionmaker_no = decisionmakercustomerno
        ,decisionmaker_name = decisionmaker
        ,decisionmaker_phone = dbo.clean_phone_number(a.decisionmakerphone)
        ,decisionmaker_flag = case when dbo.clean_alphabet(a.decisionmakerphone) = '' then null else dbo.clean_alphabet(a.decisionmakerphone) end
        ,contactperson_no = contactpersoncustomerno
        ,contactperson_name = contactperson
        ,contactperson_phone = dbo.clean_phone_number(a.contactpersonphone)
        ,contactperson_flag = case when dbo.clean_alphabet(a.contactpersonphone) = '' then null else dbo.clean_alphabet(a.contactpersonphone) end
        ,platnum = dbo.clean_platnum(policeregno)
        ,prodcode = productcode
        ,proddesc = productdescription
        ,colorcode
        ,colordesc = colordescription
        ,modelyear
        ,model
        ,brand 
        ,segment 
        ,bodystyle = upper(bodystyle)  --fix uppercase
        ,fuel 
        ,transmission
        ,doorstyle
        ,drivetrain
        ,engine
        ,servicecategory
        ,kilometer
        ,serviceadvisor
        ,dealercode_sales = dealerorigincode 
        ,dealerdesc_sales = dealerorigindescription
        ,gidate = try_convert(date, right(gidate, 4) + '-' + substring(gidate, 4, 2) + '-' + left(gidate, 2))
        ,pkbdate
        ,finishdatetime
        ,releasedtime
        ,a.createdtime
        ,a.lastmodifiedtime
        ,uploaddate = getdate()
    from pkb a
    -- left join dealer b 
    --     on a.businessareaid = b.businessareaid
    -- left join city c 
    --     on a.cityid = c.id
    -- left join region d 
    --     on a.regionid = d.id
    left join pkbtype e 
        on a.pkbtypeid = e.id
    where a.rn = 1
)
select * from clean 
where pkbtypecode != '200'