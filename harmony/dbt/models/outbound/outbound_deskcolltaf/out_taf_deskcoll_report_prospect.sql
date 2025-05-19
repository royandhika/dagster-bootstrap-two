{{
	config(
		materialized='incremental',
		schema=env_var('ENV_SCHEMA') + '_dm',
		unique_key=['contract_number', 'supply_date'],
        group='outbound_deskcolltaf',
		tags=['3hourly']
	)
}}

with supply as (
	select 
		contract_number 
		,branch
		,customer_id
		,custname 
		,companyname 
		,supply_date = convert(date, data_date)
		,phone_number = mobilephn1
		,ovddays
		,data_date
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcolltaf_acs_customer_profile_ext_intelix') }} a
	{% if is_incremental() %}
	where convert(date, data_date) = convert(date, '{{ var('min_date') }}')
		or convert(date, data_date) = convert(date, '{{ var('max_date') }}')
	{% endif %}
)
,call_history as (
	select 
		a.id
		,a.contract_number
		,branch = a.branch_id
		,a.phone_number 
		,a.no_lkd
		,a.user_id
		,a.ptp_date
		,call_result = da.description
		,nctc_reason = do.description 
		,a.ptp_status
		,a.created_time
		,supply_date = convert(date, a.created_time)
		,a.notepad
		,a.overdue_days
		,rn = row_number() over(partition by a.contract_number, convert(date, a.created_time) order by a.created_time desc)
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcolltaf_acs_call_history_daily_intelix') }} a
	left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcolltaf_acs_reference_intelix') }} da 
		on da.reference = 'CALL_RESULT' 
		and da.value = a.call_result 
	left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcolltaf_acs_reference_intelix') }} do 
		on do.reference = 'COLL_RESULT' 
		and do.value = a.nctc_reason 
	{% if is_incremental() %}
	where a.created_time >= '{{ var('min_date') }}'
		and a.created_time <= '{{ var('max_date') }}'
	{% endif %}
)
,attempts as (
	select 
		contract_number
		,supply_date = convert(date, a.created_time)
		,attempt = count(id) over(partition by a.contract_number, convert(date, a.created_time))
		,created_time
		,rn = row_number() over(partition by a.contract_number, convert(date, a.created_time) order by a.created_time desc)
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcolltaf_acs_call_history_daily_intelix') }} a
	where exists (
		select 1 
		from call_history b  
		where a.contract_number = b.contract_number
			and convert(date, a.created_time) = convert(date, b.created_time)
	)
)
,call_history_detail as (
	select 
		call_history_id
		,dialing_mode = case 
			when dialing_mode = 1 then 'auto_dial'
			when dialing_mode = 2 then 'semi_auto'
			when dialing_mode = 3 then 'manual'
		end
		,call_date = convert(date, start_call)
		,start_call
		,end_call
		,duration = datediff(second, start_call, end_call)
		,class = b.name
		,cycle
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcolltaf_acs_call_history_detail_daily_intelix') }} a 
	left join {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcolltaf_acs_class_intelix') }} b
		on a.class_id = b.class_id
	where exists (
		select 1 
		from call_history b  
		where b.id = a.call_history_id
			and b.rn = 1
	)
)
,payment_today as (
	select 
		contract_number
		,paid_amount
		,payment_date = created_time
	from {{ source(env_var('ENV_SCHEMA') + '_dl', 'outbound_deskcolltaf_acs_payment_today_intelix') }}
	{% if is_incremental() %}
	where convert(date, created_time) = convert(date, '{{ var('min_date') }}')
		or convert(date, created_time) = convert(date, '{{ var('max_date') }}')
	{% endif %}
)
,final as (
	select 
		contract_number = coalesce(a.contract_number, b.contract_number) 
		,branch = coalesce(a.branch, b.branch) 
		,c.class
		,supply_date = coalesce(a.supply_date, b.supply_date)
		,phone_number = coalesce(a.phone_number, b.phone_number)
		,a.customer_id
		,a.custname 
		,a.companyname 
		,b.no_lkd
		,overdue = coalesce(a.ovddays, b.overdue_days)
		,agent = b.user_id
		,call_result = b.call_result 
		,b.ptp_date 
		,b.notepad 
		,c.call_date 
		,c.start_call 
		,c.end_call 
		,c.duration 
		,c.cycle 
		,c.dialing_mode
		,e.attempt 
		,b.nctc_reason 
		,b.ptp_status 
		,is_payment = case 
			when d.contract_number is null then 0 
			else 1 
		end 
		,d.payment_date
		,uploaddate = getdate()
	from supply a 
	full join call_history b 
		on a.contract_number = b.contract_number
		and convert(date, b.created_time) = convert(date, a.data_date)
	left join call_history_detail c 
		on b.id = c.call_history_id 
	left join payment_today d 
		on coalesce(a.contract_number, b.contract_number) = d.contract_number
		and convert(date, a.data_date) = convert(date, d.payment_date)
	left join attempts e 
		on b.contract_number = e.contract_number
		and convert(date, b.supply_date) = convert(date, e.created_time)
		and e.rn = 1
	where b.rn = 1
)
select * from final 