CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform` AS


-- * SECTION [1]: SOURCE PREP
--   Build a normalized text blob for deterministic ADIF/social tagging.
  WITH source_rows AS (
    -- ? Normalize source text fields used by inclusion rules
    SELECT
      t.*,
      LOWER(
        CONCAT(
          IFNULL(t.account_name, ''), ' ',
          IFNULL(t.campaign_name, ''), ' ',
          IFNULL(t.ad_group_name, ''), ' ',
          IFNULL(t.ad_name, '')
        )
      ) AS _search_blob
    FROM `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl` AS t
  ),


-- * SECTION [2]: ROW FLAGGING
--   Tag rows and keep lineage metadata for downstream QA.
  flagged_rows AS (
    -- ? Add inclusion flags, normalized social platform, and row-level key
    SELECT
      source_rows.* EXCEPT(_search_blob),
      REGEXP_CONTAINS(_search_blob, r'(^|[^a-z0-9])adif([^a-z0-9]|$)') AS is_adif,
      REGEXP_CONTAINS(_search_blob, r'(^|[^a-z0-9])social([^a-z0-9]|$)') AS is_social,
      CASE
        WHEN source_relation = 'facebook_ads' THEN 'meta'
        WHEN source_relation = 'instagram_ads' THEN 'meta'
        WHEN source_relation = 'pinterest_ads' THEN 'pinterest'
        WHEN source_relation = 'tiktok_ads' THEN 'tiktok'
        WHEN source_relation = 'linkedin_ads' THEN 'linkedin'
        WHEN source_relation = 'snapchat_ads' THEN 'snapchat'
        ELSE source_relation
      END AS social_platform,
      'looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl' AS layer_source_table,
      CONCAT(
        IFNULL(source_relation, ''), '|',
        CAST(date_day AS STRING), '|',
        IFNULL(CAST(campaign_id AS STRING), ''), '|',
        IFNULL(CAST(ad_group_id AS STRING), ''), '|',
        IFNULL(CAST(ad_id AS STRING), '')
      ) AS layer_row_key
    FROM source_rows
  )


-- * SECTION [3]: FINAL OUTPUT
--   Emit account-scoped rows at original source grain with WP_ campaign guardrail.
  SELECT
    *
  FROM flagged_rows
  WHERE TRIM(account_name) IN (
    'ADIF USA',
    'A Diamond is Forever - US',
    'A Diamond is Forever',
    'De Beers Group'
  )
    AND REGEXP_CONTAINS(IFNULL(campaign_name, ''), r'WP_');
