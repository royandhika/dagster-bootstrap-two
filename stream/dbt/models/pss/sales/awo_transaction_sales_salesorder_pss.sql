{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['salesorderno'],
        group='pss_awo_sales',
        tags=['daily']
    )
}}

with sales as (
	select 
		* 
		,rank = row_number() over(partition by salesorderno order by coalesce(solastmodifiedtime, socreatedtime) desc)
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_transaction_sales_salesorder_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(solastmodifiedtime, socreatedtime) >= '{{ var('min_date') }}'
        and coalesce(solastmodifiedtime, socreatedtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,clean as (
	select  
		salesorderno
		,salesorderid = id
		,salesordertypedescription
		,paymenttype = [payment type]
		,tenor
		,leasingcompany = partnername
		,dealercode = salesofficecode
		,dealerdesc = salesofficedescription
		,salesorganizationcode
		,salesorganizationdesc = companydescription
		,salesorderdate
		,createdtime = socreatedtime
		,lastmodifiedtime = solastmodifiedtime
		,uploaddate = getdate()
	from sales
	where rank = 1
)
select * from clean