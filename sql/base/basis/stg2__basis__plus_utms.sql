---------------------------- 1st Query ---------------------------------------
----------------------------does not include supp utms---------------------------------------

CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.basis_plus_utms_v3`
  --CREATE OR REPLACE table looker-studio-pro-452620.repo_stg.basis_plus_utms_table
  OPTIONS ( description = "Basis delivery joined to parsed UTM parameters. EDIT HERE: dev/sql/base/basis/stg2__basis__delivery_with_utms.sql.",
    labels = [ ("layer",
      "stg2"),
    ("version",
      "final")]            -- optional
    ) AS
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
      `looker-studio-pro-452620.repo_stg.basis_delivery` )
  SELECT
    del.*,
    utm.creative_name AS creative__utms,
    --package_name as package__utms,
    utm.placement AS placement__utms,
    utm.id AS pl_id__utm,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_term,
    utm_content,
    CONCAT( LOWER(utm.placement), " || ", utm.cleaned_creative_name) AS utm_key,
    --COUNT(DISTINCT utm_content) OVER (PARTITION BY placement) AS utm_content_count
  FROM
    del
  LEFT JOIN
    `looker-studio-pro-452620.repo_stg.basis_utms_stg_view_2507` utm
  ON
    del_key = CONCAT(
      --utm.id
      LOWER(utm.placement)," || ", utm.cleaned_creative_name)
  WHERE
    campaign NOT LIKE '%GE%'
    AND campaign NOT LIKE 'Ritual%'
  ORDER BY
    date DESC
  NULLS FIRST
    ;

  
---------------------------- 2nd Query ---------------------------------------
----------------------------includes sup utms ---------------------------------------

  CREATE OR REPLACE VIEW looker-studio-pro-452620.repo_stg.basis_plus_utms_v3_PnS_table AS (
  WITH
    del AS (
        SELECT
            *,
        FROM
    `looker-studio-pro-452620.repo_stg.basis_delivery`
        WHERE
        campaign not like '%GE%' and
        campaign not like 'Ritual%'
        -- ORDER BY
        --     date DESC
        --     NULLS first 
    ),

    utm1 as (
    SELECT
      `tag_placement`                           AS placement,
      `name`                                    AS creative_name,
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
      )                                         AS cleaned_creative_name,
      -- Final cleaned name following the same transformations
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
      ) AS cleaned_creative_name_2,
      url                                       AS url,
      REGEXP_EXTRACT(url, r'-(\d+)&utm_term')   AS id,
      REGEXP_EXTRACT(url, 'utm_source=(.*?)&')  AS utm_source,
      REGEXP_EXTRACT(url, 'utm_medium=(.*?)&')  AS utm_medium,
      REGEXP_EXTRACT(url, 'utm_campaign=(.*?)&')AS utm_campaign,
      REGEXP_EXTRACT(url, 'utm_term=(.*)')      AS utm_term,
      REGEXP_EXTRACT( url, r'[?&]utm_content=([^&#]*)' ) AS utm_content,
      source
    FROM
    looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab),

    utm AS  (
        select distinct
            placement,
            cleaned_creative_name_2,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            utm_term,
            (concat(lower(placement)," || ", cleaned_creative_name_2)) as utm_utm_key
    from 
    utm1)
        
    SELECT
      del.*,
      utm.placement as placement__utms,
      utm_source, utm_medium, utm_campaign, utm_term, utm_content,
      utm.utm_utm_key as utm_key,
      coalesce(del.del_key,utm_utm_key) as master_key
    FROM
      del
    full JOIN
        utm ON
            del_key = 
            --concat( lower(del.placement)," || ", utm.cleaned_creative_name)
            utm.utm_utm_key

    where 
    -- utm_medium != "" and
    --impressions + clicks > 0 and
    campaign not like '%GE%' and
    campaign not like 'Ritual%'



    ORDER BY
      date DESC
    NULLS first);



---------------------------- 3nd Query 7/18 ---------------------------------------
----------------------------includes sup utms ---------------------------------------

CREATE OR REPLACE VIEW looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table AS (


WITH
del AS (
    SELECT
        *,
    FROM
`looker-studio-pro-452620.repo_stg.basis_delivery`
    WHERE
    campaign not like '%GE%' and
    campaign not like 'Ritual%'
    -- ORDER BY
    --     date DESC
    --     NULLS first 
),

utm4 as (
  select
    placement,
    -- LOWER(
    --   REPLACE( 
    --     REGEXP_EXTRACT(creative, r'^(?:\d+_)?([^_]+.*?)(?:\d+x\d+.*)?$'), ' ', '' ) 
    --   )                                      AS cleaned_creative_name,
    -- -- Final cleaned name following the same transformations
    LOWER(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          LOWER(
            REPLACE(
              REGEXP_EXTRACT(
                creative,
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
    ) AS cleaned_creative_name_2,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    (concat(lower(placement)," || ",
      LOWER(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            LOWER(
              REPLACE(
                REGEXP_EXTRACT(
                  creative,
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
      )
    )) as utm_utm_key
  
  from looker-studio-pro-452620.repo_stg.dcm_plus_utms_upload
),

utm1 as (
SELECT
  `tag_placement`                           AS placement,
  `name`                                    AS creative_name,
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
                                           AS cleaned_creative_name,
  -- Final cleaned name following the same transformations
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
   ) AS cleaned_creative_name_2,
  url                                       AS url,
  REGEXP_EXTRACT(url, r'-(\d+)&utm_term')   AS id,
  REGEXP_EXTRACT(url, 'utm_source=(.*?)&')  AS utm_source,
  REGEXP_EXTRACT(url, 'utm_medium=(.*?)&')  AS utm_medium,
  REGEXP_EXTRACT(url, 'utm_campaign=(.*?)&')AS utm_campaign,
  REGEXP_EXTRACT(url, 'utm_term=(.*)')      AS utm_term,
  REGEXP_EXTRACT( url, r'[?&]utm_content=([^&#]*)' ) AS utm_content,
  source
FROM
looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab),

utm AS  (
    select distinct
        placement,
        cleaned_creative_name_2,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        (concat(lower(placement)," || ", cleaned_creative_name_2)) as utm_utm_key
from 
utm1 union distinct select * from utm4),

joined as (    
SELECT
  del.*,
  utm.placement as placement__utms,
  utm_source, utm_medium, utm_campaign, utm_term, utm_content,
  utm.utm_utm_key as utm_key,
  coalesce(del.del_key,utm_utm_key) as master_key
FROM
  del
full JOIN
    utm ON
        del_key = 
        --concat( lower(del.placement)," || ", utm.cleaned_creative_name)
        utm.utm_utm_key

where 
-- utm_medium != "" and
--impressions + clicks > 0 and
campaign not like '%GE%' and
campaign not like 'Ritual%')

, ranked AS (
  SELECT
    joined.*,
    ROW_NUMBER() OVER (                        -- keep one row per duplicate set
      PARTITION BY TO_JSON_STRING(joined)      -- identical across *all* columns
      ORDER BY placement                    -- arbitrary “first” tie-breaker
    ) AS rn
  FROM joined
)


/* ---------- 3. Deliver deduped result ---------- */
SELECT * EXCEPT(rn)
FROM   ranked
WHERE  rn = 1 and impressions >0
ORDER BY
  date DESC
NULLS first)
