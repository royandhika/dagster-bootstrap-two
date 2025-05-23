{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['equipmentno'],
        group='pss_awo_sales',
        tags=['daily']
    )
}}

with faktur as (
	select 
        * 
        ,rank = row_number() over(partition by dbo.clean_alphanumeric(serialno) order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_sales_invoice_tso_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,clean as (
    select 
        equipmentno = dbo.clean_alphanumeric(serialno)
        ,customerno
        ,stnkname1 = customername
        ,stnkname2 = customername2
        ,stnkaddress1 = customeraddress1
        ,stnkaddress2 = customeraddress2
        ,stnkaddress3 = customeraddress3
        ,stnkaddress4 = customeraddress4
        ,stnkaddress5 = customeraddress5
        ,stnkrtrw = rtrw
        ,stnksubdistrict = kelurahan
        ,stnkdistrict = kecamatan
        ,stnkcity = fakturatpmtsokabupaten
        ,stnkcity2 = fakturatpmkabupaten
        ,stnkregion = provinsi
        ,stnkdate
        ,platnum = dbo.clean_platnum(policeno)
        ,bpkbdate = branchgoodsreceiptbpkbdate
        ,fakturatpmtsoid
        ,fakturatpmid
        ,businessareaid
        ,dealercode = businessareacode
        ,salesorganizationid
        ,salesorganizationcode
        ,ktpno
        -- ,noktportdp
        ,phone = phoneno
        ,pekerjaan
        ,email
        ,areaname
        ,regioncode
        -- ,salestypecode
        -- ,salestypedescription
        -- ,salesorderno
        ,createdby
        ,lastmodifiedby
        ,createdtime
        ,lastmodifiedtime
        ,uploaddate = getdate()
    from faktur 
    where rank = 1
)
select * from clean