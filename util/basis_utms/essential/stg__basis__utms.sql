CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.basis_utms_stg_view_2507` AS
/*=======================================================================================
  View:    looker-studio-pro-452620.repo_stg.basis_utms_stg_view_2507
  Layer:   STAGING
  Purpose:
    • Parse UTM parameters from Basis click-through URLs into structured columns.  
    • Generate a cleaned creative name to use as a join key with delivery data.  
    • Expose a unique numeric ID extracted from each URL.  

  Source:  looker-studio-pro-452620.landing.basis_utms_unioned  
  Target:  looker-studio-pro-452620.repo_stg.basis_utms_stg_view  
  Join Key: LOWER(placement) + cleaned_creative_name (aligned to repo_stg.basis_delivery.del_key)  

  Last Edit: 2025-07-16 (formatting & documentation only — logic unchanged)
=======================================================================================*/


WITH normalized AS (
  SELECT
      /* ---------- Metadata --------------------------------------------------------- */
      `line_item`      AS package_name,
      `tag_placement`  AS placement,
      `formats`,
      `size`,
      `start_date`,
      `end_date`,

      /* ---------- Creative --------------------------------------------------------- */
      `name`           AS creative_name,

      /* Delivery-aligned creative cleaner used in repo_stg.basis_delivery.del_key */
      LOWER(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            LOWER(
              REPLACE(
                REGEXP_EXTRACT(
                  name,
                  r'^(?:\d+_)?([^_]+.*?)(?:_\d+x\d+.*)?$'
                ),
                ' ',
                ''
              )
            ),
            r'(^peacock_|_peacock$)',
            ''
          ),
          r'[^a-zA-Z0-9]',
          ''
        )
      ) AS cleaned_creative_name,

      /* Secondary cleaner retained for QA parity */
      LOWER(
        REPLACE(
          REGEXP_EXTRACT(name, r'^([A-Za-z0-9\s_]+?)(?:_?\d+x\d+.*)?$'),
          ' ',
          ''
        )
      ) AS cleaned_creative_name2,

      /* ---------- URL & UTM parsing ----------------------------------------------- */
      url,
      REGEXP_EXTRACT(url, r'-(\d+)&utm_term')      AS id,
      REGEXP_EXTRACT(url, r'utm_source=(.*?)&')    AS utm_source,
      REGEXP_EXTRACT(url, r'utm_medium=(.*?)&')    AS utm_medium,
      REGEXP_EXTRACT(url, r'utm_campaign=(.*?)&')  AS utm_campaign,
      REGEXP_EXTRACT(url, r'utm_term=(.*)')        AS utm_term,
      REGEXP_EXTRACT(url, r'[?&]utm_content=([^&#]*)') AS utm_content
  FROM
      `looker-studio-pro-452620.landing.basis_utms_unioned`
)
SELECT
  *,
  CONCAT(LOWER(placement), ' || ', cleaned_creative_name) AS utm_key_delivery_aligned
FROM normalized;
    

/*=======================================================================================
  Version History (high-level)
  ----------------------------------------------------------------------------------------
  • 2025-07-16 v2507   – Reformatted headers & comments; consolidated archive section.  
  • 2025-04-xx v??     – View repointed from 20250327_data_model.basis_utms_pivoted to
                         landing.basis_utms_unioned for broader coverage.  
  • 2025-03-27 v1      – Initial staging logic created (parsed UTMs from Basis DCM export).  
=======================================================================================*/

-- OLD VERSION
  -- create or replace view `looker-studio-pro-452620.repo_stg.basis_utms_stg_view_2507` as

  -- -- @code:        [EDIT HERE] stg__basis__utms.sql
  -- -- @code:        repo_stg__basis_utms_stg_view.sql
  -- -- @layer:       staging
  -- -- @description: Parses UTM parameters from raw Basis URL strings into structured columns.
  -- --               Extracts utm_source, utm_medium, utm_campaign, utm_term, utm_content, and a
  -- --               unique ID from the URL for downstream joining. Also generates a cleaned 
  -- --               creative name (`cleaned_creative_name`) to serve as a robust join key 
  -- --               with delivery data.
  -- -- @source:      looker-studio-pro-452620.20250327_data_model.basis_utms_pivoted
  -- -- @target:      looker-studio-pro-452620.repo_stg.basis_utms_stg_view
  -- -- @join_key:    id + cleaned_creative_name 


  -- SELECT
  -- `line_item` as package_name,
  -- `tag_placement` as placement,
  -- `formats`,
  -- `size`,
  -- `start_date`,
  -- `end_date`,
  -- --`creative_num`,
  -- `name` as creative_name,
  -- -- [cleaned_creative_name] standardizes creative names to DCM Creative
  --   -- Extracts the core creative name by:
  --   --   1. Removing any leading numeric prefix (e.g., "123_")
  --   --   2. Capturing the main name up to (but not including) any trailing size suffix (e.g., "_300x250")
  --   --   3. Removing all spaces from the result
  --   --   4. Converting to lowercase for normalization
  --   -- Example: "123_Spring Sale_300x250" → "springsale"
  --   lower(replace(REGEXP_EXTRACT(name, r'^(?:\d+_)?([^_]+.*?)(?:\d+x\d+.*)?$'),
  --       ' ' ,
  --       ''
  --       )
  --     ) AS cleaned_creative_name,

  --   LOWER(
  --     REPLACE(
  --       REGEXP_EXTRACT(name, r'^([A-Za-z0-9\s_]+?)(?:_?\d+x\d+.*)?$'),
  --       ' ',
  --       ''
  --     )
  --   ) AS cleaned_creative_name2,
  -- --`asset_link`,
  -- --`edo_tag`,
  -- --`disqo_tag`,
  -- --`video_amp_tag`,
  -- --`3p_or_1p_tag`,
  -- url,
  -- REGEXP_EXTRACT(url, r'-(\d+)&utm_term') AS id,
  -- REGEXP_EXTRACT(url, 'utm_source=(.*?)&') AS utm_source,
  -- REGEXP_EXTRACT(url, 'utm_medium=(.*?)&') AS utm_medium,
  -- REGEXP_EXTRACT(url, 'utm_campaign=(.*?)&') AS utm_campaign,
  -- REGEXP_EXTRACT(url, 'utm_term=(.*)') AS utm_term,
  -- --REGEXP_EXTRACT(url, 'utm_content=(.*?)&') AS utm_content,
  -- REGEXP_EXTRACT(
  --   url,
  --   r'[?&]utm_content=([^&#]*)'
  -- ) AS utm_content

  -- FROM
  --  -- `looker-studio-pro-452620.20250327_data_model.basis_utms_pivoted`;
  --  --`looker-studio-pro-452620.20250327_data_model.basis_utms_pivoted_unioned`;
  --  looker-studio-pro-452620.landing.basis_utms_unioned
  
  -- # where tag_placement = "MASSMUTUAL003CP_3188385_[CPM]_CTV_MidFunnel_PMP(Peacock)_Q2_National"

  -- --ARCHIVE
  --   -- create or replace table `looker-studio-pro-452620.repo_stg.basis_utms` as
  --   -- with 
  --   -- a  as (
  --   --     SELECT
  --   --     `Click-through URL` as url,
  --   --     concat(
  --   --       regexp_extract(`Click-through URL`,r'-(\d+)&utm_term'),
  --   --        " || ", Creative) AS id,
  --   --     REGEXP_EXTRACT(`Click-through URL`, 'utm_source=(.*?)&') AS utm_source,
  --   --     REGEXP_EXTRACT(`Click-through URL`, 'utm_medium=(.*?)&') AS utm_medium,
  --   --     REGEXP_EXTRACT(`Click-through URL`, 'utm_campaign=(.*?)&') AS utm_campaign,
  --   --     REGEXP_EXTRACT(`Click-through URL`, 'utm_term=(.*)') AS utm_term,
  --   --     REGEXP_EXTRACT(`Click-through URL`, 'utm_content=(.*?)&') AS utm_content,
  --   --     REGEXP_EXTRACT(`Click-through URL`, r'utm_content=[^_]+_([^_]+)') ad_raw, 
  --   --     Creative, 
        

  --   -- create or replace view `looker-studio-pro-452620.20250327_data_model.basis_utms_pivoted` as
  --   -- >>>>>>> refs/heads/dev

  --   --   FROM
  --   --     `looker-studio-pro-452620`.`20250327_data_model`.`basis_utms_fromDCM` 
  --   --   ),
    
  --   -- b as (
  --   --   SELECT
  --   --   url,
  --   --   REGEXP_EXTRACT(url, r'-(\d+)&utm_term') AS id,
  --   --   REGEXP_EXTRACT(url, 'utm_source=(.*?)&') AS utm_source,
  --   --   REGEXP_EXTRACT(url, 'utm_medium=(.*?)&') AS utm_medium,
  --   --   REGEXP_EXTRACT(url, 'utm_campaign=(.*?)&') AS utm_campaign,
  --   --   REGEXP_EXTRACT(url, 'utm_term=(.*)') AS utm_term,
  --   --   REGEXP_EXTRACT(url, 'utm_content=(.*?)&') AS utm_content,
  --   --   REGEXP_EXTRACT(url, r'utm_content=[^_]+_([^_]+)') ad_raw,
  --   --   cast(NULL as string) as Creative

  --   -- FROM
  --   --   `looker-studio-pro-452620`.`20250327_data_model`.`basis_utms_raw_25Q1`)

  --   -- select * from a union all select * from b
