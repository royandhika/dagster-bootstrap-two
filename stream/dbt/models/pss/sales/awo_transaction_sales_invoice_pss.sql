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
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_sales_invoice_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,clean as (
    select 
        equipmentno = dbo.clean_alphanumeric(serialno)
        ,customerno
        ,stnkname1 = firstnameonstnk
        ,stnkname2 = secondnameonstnk
        ,stnkaddress1 = firstaddressonstnk
        ,stnkaddress2 = secondaddressonstnk
        ,stnkrt = rt
        ,stnkrw = rw
        ,stnksubdistrict = kelurahan
        ,stnkdistrict = kecamatan
        ,stnkcity = city
        ,stnkregion = fakturatpmkabupaten
        ,stnkpostalcode = postalcode
        ,stnkdate
        ,platnum = dbo.clean_platnum(policeno)
        ,bpkbdate = branchgoodsreceiptbpkbdate
        ,bbnareaid
        ,bbnareacode
        ,fakturatpmnontsoid
        ,fakturatpmid
        ,businessareaid
        ,dealercode = businessareacode
        ,salesorganizationid
        ,salesorganizationcode
        -- ,salestypecode
        -- ,salestypedescription
        -- ,salesorderno
        ,phone = phoneno
        ,birthdate
        ,pekerjaan
        ,pendidikan
        ,email
        ,religion
        ,kewarganegaraan
        ,kelurahancode
        ,kecamatancode
        ,district
        ,createdby
        ,lastmodifiedby
        ,createdtime
        ,lastmodifiedtime
        ,uploaddate = getdate()
    from faktur 
    where rank = 1
)
select * from clean