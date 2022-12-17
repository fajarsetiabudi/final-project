--Create dim currency (id, currency_name, currency_code)
drop table if exists dim_currency; 

create table dim_currency (
	id uuid unique,
	currency_name varchar,
	currency_code varchar unique,
	primary key(id)
);

insert into dim_currency (
  id, 
  currency_name,
  currency_code
)
(
select 
	gen_random_uuid() as id, currency_name, currency_code 
from(
	select distinct currency_name , currency_id as currency_code
	from final_project.topic_currency 
	) tc
)
on conflict (currency_code) do nothing
;
