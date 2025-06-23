--
--create or replace table `looker-studio-pro-452620.repo_tables.basis` as

CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.basis_plus_utms_v2`
OPTIONS (
  description = "Basis delivery joined to parsed UTM parameters. EDIT HERE: dev/sql/base/basis/stg2__basis__delivery_with_utms.sql.",
  labels = [ ("layer","stg2"), ("version", "final")]            -- optional
)
AS

--@code:        [EDIT HERE] mart__basis__delivery_with_utms.sql
--@layer:       staging
--@description: Joins Basis delivery data with parsed UTM metadata by cleaned creative name.
--@join keys:   `del_key` and utm_key   (placement ID + clean_creative_name).
--@filters:     meaningful traffic (impressions + clicks > 0), 
--              excludes GE campaigns, 
--              and limits to specific delivery IDs.
--@source:      looker-studio-pro-452620.final_views.basis_view
--@source:      looker-studio-pro-452620.repo_stg.basis_utms_stg_view
--@target:      looker-studio-pro-452620.repo_stg.basis_plus_utms
WITH
  del AS (
  SELECT
    *,
    -- IFNULL(
    --   concat(REGEXP_EXTRACT(placement, r'CP_(\d+)')," || ",cleaned_creative_name),
    --   CONCAT(placement," || ", creative_name)) AS del_key,

  FROM
    --`looker-studio-pro-452620.final_views.basis_view`)
    `looker-studio-pro-452620.repo_stg.basis_delivery` )

SELECT
  del.*,
  utm.creative_name as creative__utms, 
  package_name as package__utms,
  utm.placement as placement__utms,
  utm.id as pl_id__utm,
  utm_source, utm_medium, utm_campaign, utm_term, utm_content,
  concat(
    lower(utm.placement),
    --utm.id
    " || ", utm.cleaned_creative_name) as utm_key,
  
  --COUNT(DISTINCT utm_content) OVER (PARTITION BY placement) AS utm_content_count
FROM
  del
--LEFT outer JOIN
LEFT JOIN
  `looker-studio-pro-452620.repo_stg.basis_utms_stg_view` utm 
ON
  del_key = concat(
    --utm.id
    lower(utm.placement)
    ," || ", utm.cleaned_creative_name)
where 
--utm_medium != ""
--impressions + clicks > 0 and
campaign not like '%GE%' and
campaign not like 'Ritual%'

  -- and del.id IN (
  --   '3127238', '3127241', '3127243', '3127244', '3127245',
  --   '3127177', '3127183', '3127186', '3127189', '3127195',
  --   '3127196', '3127201', '3127211', '3127214', '3127219',
  --   '3169446', '3127252', '3127257'
  -- )

ORDER BY
  date DESC
NULLS first