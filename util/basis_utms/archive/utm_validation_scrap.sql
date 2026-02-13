create or temp table looker-studio-pro-452620.utm_scrap.stg3_b_plus_utms_PnS_table as (
WITH
-- Extract and filter delivery data
del AS (
    SELECT
        *,
    FROM
`looker-studio-pro-452620.repo_stg.basis_delivery`
    WHERE
        -- Parameterize these filters for easier modification
        campaign NOT LIKE '%GE%' AND
        campaign NOT LIKE 'Ritual%'
    -- Useful for debugging or understanding source data order, keep for now.
    ORDER BY
        date DESC
        NULLS first
),

-- Extract and clean UTM parameters from URLs
-- Transformations aim to normalize creative names and extract key UTM components.

utm1 as (
SELECT
      REGEXP_EXTRACT( 
        name, r'^(?:\d+_)?([^_]+.*?)(?:\d+x\d+.*)?$'), ' ', '' ) 
  )                                         AS cleaned_creative_name,
  -- Final cleaned name, handling various formatting inconsistencies:
  -- 1. Strip trailing _N×N pattern, 2. Kill "0x0", 3. Lowercase, 4. Remove non-alphanumeric chars, 5. Collapse whitespace
  REGEXP_REPLACE(                                            -- 7. collapse whitespace to “_”
    REGEXP_REPLACE(                                          -- 6. drop chars that are *not* A–Z, a–z, 0–9 or space
      LOWER(                                                 -- 5. lower-case
    r'\s+', ''
   ) AS cleaned_creative_name_2,
  url                                       AS url,
  -- Consider using SAFE_REGEXP_EXTRACT to handle URLs with missing or malformed parts
  -- and return NULL instead of an empty string.
  SAFE.REGEXP_EXTRACT(url, r'-(\d+)&utm_term')   AS id,
  REGEXP_EXTRACT(url, 'utm_source=(.*?)&')  AS utm_source,
  REGEXP_EXTRACT(url, 'utm_medium=(.*?)&')  AS utm_medium,
  REGEXP_EXTRACT(url, 'utm_campaign=(.*?)&')AS utm_campaign,
  source
FROM
looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab),

-- Select distinct UTM combinations
-- Duplicates may exist, so DISTINCT ensures unique combinations.

utm AS  (
    select distinct
        (concat(lower(placement)," || ", cleaned_creative_name_2)) as utm_utm_key
from 
utm1)

-- Combine delivery data with UTM parameters
-- Using a FULL JOIN to preserve all rows, even if there are unmatched entries.
-- If only matched data is needed, consider using an INNER JOIN for better efficiency.
SELECT
  del.*,
  utm.placement as placement__utms,
  utm_source, utm_medium, utm_campaign, utm_term, utm_content,
  utm.utm_utm_key as utm_key,
  coalesce(del.del_key,utm_utm_key) as master_key
FROM del
FULL JOIN
    utm ON
---impressions + clicks > 0 and
        del_key = utm.utm_utm_key -- Join condition using the constructed keys
ORDER BY
  date DESC
NULLS first)
