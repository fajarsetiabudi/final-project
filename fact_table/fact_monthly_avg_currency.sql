-- Create fact monthly average currency
drop table if exists fact_monthly_avg_currency; 

create table fact_monthly_avg_currency (
	currency_id varchar,
	currency_name varchar,
	end_of_month date,
	avg_rate float
);

insert into fact_daily_avg_currency (
	currency_id,
	currency_name,
	end_of_month,
	avg_rate
)
(
select 
	currency_id,
	currency_name,
	date_trunc('month', "timestamp") + interval '1 month' - interval '1 day' as end_of_month,
	avg(rate) as avg_rate
from topic_currency
where date_trunc('month', "timestamp") = date_trunc('month', {{ macros.ds }}::date) - interval '1 month'
group by currency_id , currency_name , date_trunc('month', "timestamp")
order by currency_id
)
;
