drop table if exists fact_total_per_state; 

create table fact_total_per_state (
	state varchar unique,
	total_city varchar,
	total_office varchar
);

insert into fact_total_per_state (
	state,
	total_city,
	total_office
)
(
select 
	state
	, count(distinct city) as total_city
	, count(distinct office) as total_office
from (
	select 
		name as office,
		case when offices_city is null or offices_city = '' then 'others' else offices_city end as city,
		case when offices_state_code is null or offices_state_code = '' then 'others' else offices_state_code end as state
	from sample_training_companies
) stc 
group by state
)
on conflict (state) do update 
set total_city = excluded.total_city,
	total_office = excluded.total_office
;
