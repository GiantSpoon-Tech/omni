create or replace table `looker-studio-pro-452620.landing.basis_utms_unioned-0929` as


with 

f1 as (
SELECT 
  line_item,
  tag_placement,
  name,
  end_date,
  start_date,
  size,
  formats,
  url

FROM `looker-studio-pro-452620.landing.basis_utms_pivoted_flight1_2`
where name is not null
),

f2 as (
SELECT 
  line_item,
  tag_placement,
  name,
  end_date,
  start_date,
  size,
  formats,
  url

FROM `looker-studio-pro-452620.landing.basis_utms_pivoted_flight2_2`
where name is not null
),

f3 as (
SELECT 
  line_item,
  tag_placement,
  name,
  cast(end_date as date) as end_date,
  cast(start_date as date) as start_date,
  size,
  formats,
  url

FROM `looker-studio-pro-452620.landing.basis_utms_pivoted_flight3_2`
where name is not null
),

f4 as (
SELECT 
  line_item,
  tag_placement,
  name,
  cast(end_date as date) as end_date,
  cast(start_date as date) as start_date,
  "null" as size,
  formats,
  url

FROM `looker-studio-pro-452620.landing.basis_utms_pivoted_flight4_1`
where name is not null
),

f5 as (
SELECT 
  line_item,
  tag_placement,
  name,
  cast(end_date as date) as end_date,
  cast(start_date as date) as start_date,
  "null" as size,
  formats,
  url

FROM `looker-studio-pro-452620.landing.basis_utms_pivoted_flight4_2`
where name is not null
),

final as( 
select * from f1 
union all select * from f2
union all select * from f3
union all select * from f4
union all select * from f5) 

select * from final 
--where name like "%x%"