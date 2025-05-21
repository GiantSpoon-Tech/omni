create or replace view `looker-studio-pro-452620.repo_stg.basis_utms_stg_view` as

-- @code:        [EDIT HERE] stg__basis__utms.sql
-- @code:        repo_stg__basis_utms_stg_view.sql
-- @layer:       staging
-- @description: Parses UTM parameters from raw Basis URL strings into structured columns.
--               Extracts utm_source, utm_medium, utm_campaign, utm_term, utm_content, and a
--               unique ID from the URL for downstream joining. Also generates a cleaned 
--               creative name (`cleaned_creative_name`) to serve as a robust join key 
--               with delivery data.
-- @source:      looker-studio-pro-452620.20250327_data_model.basis_utms_pivoted
-- @target:      looker-studio-pro-452620.repo_stg.basis_utms_stg_view
-- @join_key:    id + cleaned_creative_name 


SELECT
`line_item` as package_name,
`tag_placement` as placement,
`formats`,
`size`,
`start_date`,
`end_date`,
`creative_num`,
`name` as creative_name,
lower(
replace(
    
    REGEXP_EXTRACT(name, r'^(?:\d+_)?([^_]+.*?)(?:_\d+x\d+.*)?$'),
    ' ' ,
    ''
    )
 ) AS cleaned_creative_name,
`asset_link`,
`edo_tag`,
`disqo_tag`,
`video_amp_tag`,
`3p_or_1p_tag`,
url,
REGEXP_EXTRACT(url, r'-(\d+)&utm_term') AS id,
REGEXP_EXTRACT(url, 'utm_source=(.*?)&') AS utm_source,
REGEXP_EXTRACT(url, 'utm_medium=(.*?)&') AS utm_medium,
REGEXP_EXTRACT(url, 'utm_campaign=(.*?)&') AS utm_campaign,
REGEXP_EXTRACT(url, 'utm_term=(.*)') AS utm_term,
REGEXP_EXTRACT(url, 'utm_content=(.*?)&') AS utm_content

FROM
  `looker-studio-pro-452620.20250327_data_model.basis_utms_pivoted`;

--ARCHIVE
  -- create or replace table `looker-studio-pro-452620.repo_stg.basis_utms` as
  -- with 
  -- a  as (
  --     SELECT
  --     `Click-through URL` as url,
  --     concat(
  --       regexp_extract(`Click-through URL`,r'-(\d+)&utm_term'),
  --        " || ", Creative) AS id,
  --     REGEXP_EXTRACT(`Click-through URL`, 'utm_source=(.*?)&') AS utm_source,
  --     REGEXP_EXTRACT(`Click-through URL`, 'utm_medium=(.*?)&') AS utm_medium,
  --     REGEXP_EXTRACT(`Click-through URL`, 'utm_campaign=(.*?)&') AS utm_campaign,
  --     REGEXP_EXTRACT(`Click-through URL`, 'utm_term=(.*)') AS utm_term,
  --     REGEXP_EXTRACT(`Click-through URL`, 'utm_content=(.*?)&') AS utm_content,
  --     REGEXP_EXTRACT(`Click-through URL`, r'utm_content=[^_]+_([^_]+)') ad_raw, 
  --     Creative, 
      

  -- create or replace view `looker-studio-pro-452620.20250327_data_model.basis_utms_pivoted` as
  -- >>>>>>> refs/heads/dev

  --   FROM
  --     `looker-studio-pro-452620`.`20250327_data_model`.`basis_utms_fromDCM` 
  --   ),
  
  -- b as (
  --   SELECT
  --   url,
  --   REGEXP_EXTRACT(url, r'-(\d+)&utm_term') AS id,
  --   REGEXP_EXTRACT(url, 'utm_source=(.*?)&') AS utm_source,
  --   REGEXP_EXTRACT(url, 'utm_medium=(.*?)&') AS utm_medium,
  --   REGEXP_EXTRACT(url, 'utm_campaign=(.*?)&') AS utm_campaign,
  --   REGEXP_EXTRACT(url, 'utm_term=(.*)') AS utm_term,
  --   REGEXP_EXTRACT(url, 'utm_content=(.*?)&') AS utm_content,
  --   REGEXP_EXTRACT(url, r'utm_content=[^_]+_([^_]+)') ad_raw,
  --   cast(NULL as string) as Creative

  -- FROM
  --   `looker-studio-pro-452620`.`20250327_data_model`.`basis_utms_raw_25Q1`)

  -- select * from a union all select * from b