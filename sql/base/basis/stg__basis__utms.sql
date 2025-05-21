-- @file: stg__basis__utms_parsed.sql
-- @layer: staging
-- @description: Parses UTM parameters from raw Basis URL strings into structured columns.
--               Extracts utm_source, utm_medium, utm_campaign, utm_term, utm_content,
--               and a unique ID for downstream joining with delivery logs.
-- @source: looker-studio-pro-452620.20250327_data_model.basis_utms_pivoted
-- @target: basis_utms_stg (used as a staging table for joins with Basis delivery data)
-- extract utms from url into seperate columns

create or replace view `looker-studio-pro-452620.repo_stg.basis_utms_stg_view` as

SELECT
`line_item` as package_name,
`tag_placement` as placement,
`formats`,
`size`,
`start_date`,
`end_date`,
`creative_num`,
`name` as creative_name,
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