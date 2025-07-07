create or replace view looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_filtered_raw as
select * from
looker-studio-pro-452620.repo_mart.mart__olipop__crossplatform
where campaign_name like '%_gs_%'