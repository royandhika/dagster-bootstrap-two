{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['equipmentno'],
        group='pss_awo_sales',
        tags=['daily']
    )
}}

with sales as (
	select 
		*
		,rank = row_number() over(partition by dbo.clean_alphanumeric(vin) order by createdtime desc, lastmodifiedtime desc)
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_sales_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,clean as (
	select  
		billingno
		,billingcancel
		,billingtype
		,salesorderid = salesorderfuid
		,salesorderno
		,equipmentid
		,equipmentno = dbo.clean_alphanumeric(equipmentno)
		,so = so2
		,salesorganizationcode
		,salesorganizationdesc = so
		,dealercode = sourcecode
		,dealerdesc = sourcedesc
		,region = upper(region)  --fix uppercase
		,divisioncode
		,customerid
		,customerno
		,mainpartner
		,invoiceprice
		,paymenttype
		,downpayment
		,tenor
		,leasingcompany
		,pricetype
		,pricelisttypedescription
		,platnum = dbo.clean_platnum(platnum)
		,prodcode
		,proddesc
		,colorcode = color
		,colordesc = colordescription
		,modelyear
		,model
		,brand = case 
			when brand is null then (
				case 
					when salesorganizationcode = '0002' then 'TOYOTA'
					when salesorganizationcode = '0003' then 'DAIHATSU'
					when salesorganizationcode = '0004' then 'ISUZU'
					when salesorganizationcode = '0173' then 'BMW'
					when salesorganizationcode = '0174' then 'PEUGEOT'
					when salesorganizationcode = '0007' then 'LEXUS'
					when salesorganizationcode = '0172' then 'UD Trucks'
				end) 
			else brand 
		end
		,segment
		,bodystyle = upper(bodystyle)  --fix uppercase
		,fuel
		,transmission
		,doorstyle
		,drivetrain
		,engine
		,stnkname
		,stnkaddress
		,stnkdate
		,stnkregion = stnkprovince
		,contactperson_name = cpname
		,contactperson_title = cptitle
		,contactperson_mobilephone = dbo.clean_phone_number(cpcellphone)
		,contactperson_homephone = dbo.clean_phone_number(cphomphone)
		,contactperson_officephone = dbo.clean_phone_number(cpofcphone)
		,contactperson_email = dbo.clean_email_address(cpemailaddr)
		,contactperson_jobposition = cpjobposition
		,salesmanno = salesmanid
		,salesmanname
		,supervisorno = supervisorid
		,supervisorname = supervisor
		,invoicedate
		,gidate
		,gicreatedtime
		,gilastmodifiedtime
		,createdtime
		,lastmodifiedtime
		,uploaddate = getdate()
	from sales
	where rank = 1
)
select * from clean
where len(equipmentno) > 10
-- where billingtype in ('Cancel of Cred Memo','FU: Cancell','FU: Invoice','FU: Return')