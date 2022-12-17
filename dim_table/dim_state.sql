-- Create dim state (id, country_id, state_code)
drop table if exists dim_state; 

create table dim_state (
	id uuid unique,
	country_id varchar,
	state_code varchar,
	primary key(id),
	foreign key(country_id) references final_project.dim_country(country_code),
	constraint country_state unique (country_id, state_code)
);

insert into dim_state (
  id, 
  country_id,
  state_code
)
(
select 
	gen_random_uuid() as id, 
	case when country_id is null then 'others' else country_id end as country_id,
	state_code
from (
	-- get data from companies
	select distinct 
		offices_country_code as country_id,
		offices_state_code as state_code
	from final_project.sample_training_companies
	
	union
	
	-- get data from zips
	select distinct 
		'others' as country_id,
		state as state_code
	from final_project.sample_training_zips
	) stu
where state_code is not null and state_code != ''
)
on conflict (country_id, state_code) do nothing 
;