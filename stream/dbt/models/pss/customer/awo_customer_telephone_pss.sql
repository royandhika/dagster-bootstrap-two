{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['customerno'],
        group='pss_awo_customer',
        tags=['daily']
    )
}}

{% if is_incremental() %}
with updated as (
    select customerno 
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_customer_telephone_pss_staging') }}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
) 
,customer as (
    select 
        a.* 
        ,rn = row_number() over(partition by a.customerno, a.phonetype order by coalesce(a.lastmodifiedtime, a.createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_customer_telephone_pss_staging') }} a
    where isdefault = 1
        and exists (
            select customerno 
            from updated b
            where a.customerno = b.customerno
        )
)
{% else %}
with customer as (
    select 
        * 
        ,rn = row_number() over(partition by customerno, phonetype order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_customer_telephone_pss_staging') }}
    where isdefault = 1
)
{% endif %}
,clean as (
    select
        customerphoneid = id
        ,customeraddressid
        ,customerid
        ,customerno
        ,isdefault
        ,phonetype
        ,telephoneno = dbo.clean_phone_number(telephoneno)
        ,extension
        ,createdtime
        ,lastmodifiedtime
        ,uploaddate = getdate()
    from customer 
    where rn = 1
)
,trans as (
    select customerid, customerno
        ,mobilephone = case 
            when phonetype = 'Mobile' then telephoneno
            else null
        end
        ,homephone = case 
            when phonetype = 'Home' then telephoneno
            else null
        end
        ,officephone = case 
            when phonetype = 'Office' then telephoneno
            else null
        end
        ,whatsapp = case 
            when phonetype = 'Whatsapp' then telephoneno
            else null
        end
        ,createdtime
        ,lastmodifiedtime
    from clean
)
,final as (
    select
        customerid
        ,customerno
        ,mobilephone = max(mobilephone)
        ,homephone = max(homephone)
        ,officephone = max(officephone)
        ,whatsapp = max(whatsapp)
        ,createdtime = max(createdtime)
        ,lastmodifiedtime = max(lastmodifiedtime)
        ,uploaddate = getdate()
    from trans
    group by customerid, customerno
)
select * from final