{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['customerno'],
        group='pss_awo_customer',
        tags=['daily']
    )
}}

with customer as (
    select 
        * 
        ,rn = row_number() over(partition by customerno order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_customer_pss_staging') }}
    where grouping not in ('Suspect', 'Employee')
        {% if is_incremental() %}
        and coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
        {% endif %}
)
,clean as (
    select
        customerid = id
        ,customerno
        ,mainpartner
        ,customertitle = case 
            when customername1 like 'PT%' or customername1 like 'CV%' or customername1 like 'YAYASAN%' or customername1 like 'BADAN%' or customername1 like 'KOPERASI%' then 'Company'
            when customertitle = 'Mr' then 'Mr.'
            when customertitle = 'Ms' then 'Ms.'
            when customertitle is null then 'Mr.'
            else customertitle
        end 
        ,customername1
        ,customername2
        ,customername3
        ,customername4
        ,searchterm1
        ,searchterm2
        ,grouping
        ,ktp
        ,birthday = concat(right(birthday, 4), '-', substring(birthday, 4, 2), '-', left(birthday, 2))
        ,birthplace
        ,gender = case 
            when customername1 like 'PT%' or customername1 like 'CV%' or customername1 like 'YAYASAN%' or customername1 like 'BADAN%' or customername1 like 'KOPERASI%' then null
            when customertitle = 'Mr' then 'MALE'
            when customertitle = 'Ms' then 'FEMALE'
            when customertitle is null then 'MALE'
            when customertitle = 'Company' then null
        end 
        ,isprospect
        ,isloyalty
        ,taxvatregno
        ,taxname1
        ,taxname2
        ,taxaddress1
        ,taxaddress2
        ,taxpostalcode
        ,taxwapu
        ,taxcity
        ,createdtime
        ,lastmodifiedtime
        ,uploaddate = getdate()
    from customer 
    where rn = 1
)
select * from clean