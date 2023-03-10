-- Create dim country (id, country_code)


create table dim_country (
	id uuid unique,
	country_code varchar unique,
	primary key(id)
);

insert into final_project.dim_country (
  id, 
  country_code
)
(
select 
	gen_random_uuid() as id, 
	case when country_code is null then 'others' else country_code end as country_code
from (
	select distinct 
		offices_country_code as country_code
	from final_project.sample_training_companies
	) stc
)
on conflict (country_code) do nothing
;