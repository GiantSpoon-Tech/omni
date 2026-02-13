create or REPLACE table looker-studio-pro-452620.utm_scrap.stg3_b_plus_utms_PnS_table as (
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
        REPLACE( 
        REGEXP_EXTRACT( 
            name, r'^(?:\d+_)?([^_]+.*?)(?:\d+x\d+.*)?$'), ' ', '' ) 
    )                                         AS cleaned_creative_name,
    -- Final cleaned name following the same transformations
    REGEXP_REPLACE(                                            -- 7. collapse whitespace to “_”
        REGEXP_REPLACE(                                          -- 6. drop chars that are *not* A–Z, a–z, 0–9 or space
        LOWER(                                                 -- 5. lower-case
            REGEXP_REPLACE(                                      -- 4. strip trailing _N×N pattern
            REGEXP_REPLACE( name, r'0x0', ''),        -- 3. kill “0x0”
            r'(?:_?\d+x\d+.*)?$', ''
            )
        ),
        r'[^A-Za-z0-9\s]', ''
        ),
        r'\s+', ''
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
FROM del
full JOIN
utm ON
    del_key = 
    --concat( lower(del.placement)," || ", utm.cleaned_creative_name)
    utm.utm_utm_key

    WHERE 
    -- utm_medium != "" and
    --impressions + clicks > 0 and
        campaign not like '%GE%' and
        campaign not like 'Ritual%'
    ORDER BY
        date DESC
        NULLS first
)