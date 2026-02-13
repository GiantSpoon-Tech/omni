select SUM(impressions) FROM looker-studio-pro-452620.final_views.basis_view WHERE date >= DATE '2025-01-01' and package_roadblock like '%MASS%' ;


select distinct del_key FROM `looker-studio-pro-452620.repo_stg.basis_plus_utms_v3` WHERE 
date >= DATE '2025-01-01' and package_roadblock like '%MASS%' 
-- and utm_source is null
--GROUP BY 3,4 ;



select * from looker-studio-pro-452620.utm_scrap.sgt2_b_utms_combinedSupPvt



select SUM(impressions), utm_source FROM `looker-studio-pro-452620.repo_stg.basis_plus_utms_table` WHERE 
date >= DATE '2025-01-01' and package_roadblock like '%MASS%' 
 and id = "3127266"
 GROUP BY 2 ;

select * FROM `looker-studio-pro-452620.repo_stg.basis_plus_utms_v3`
WHERE 
date >= DATE '2025-01-01' and 
utm_key = "massmutual003cp_3188401_[cpm]_audio_midfunnel_pmp(iheart)_q2_national || schedulingconflict30"
--and id = "3127266" 
--GROUP BY 2 ;

with 
dcm  as (
    SELECT
    `Click-through URL` as url,
    regexp_extract(`Click-through URL`,r'-(\d+)&utm_term')      AS id,
    placement,
    REGEXP_EXTRACT(`Click-through URL`, 'utm_source=(.*?)&')    AS utm_source,
    REGEXP_EXTRACT(`Click-through URL`, 'utm_medium=(.*?)&')    AS utm_medium,
    REGEXP_EXTRACT(`Click-through URL`, 'utm_campaign=(.*?)&')  AS utm_campaign,
    REGEXP_EXTRACT(`Click-through URL`, 'utm_term=(.*)')        AS utm_term,
    REGEXP_EXTRACT(`Click-through URL`, 'utm_content=(.*?)&')   AS utm_content,
    --REGEXP_EXTRACT(`Click-through URL`, r'utm_content=[^_]+_([^_]+)') ad_raw, 
    Creative                                                    as creative_name, 
    LOWER(
    regexp_replace(
      REGEXP_REPLACE(
        LOWER(
          REPLACE(
            REGEXP_EXTRACT(
              Creative,
              r'^(?:\d+_)?([^_]+.*?)(?:_\d+x\d+.*)?$'   -- strip numeric prefix & size suffix
            ),
            ' ',                                        -- remove spaces
            ''
          )
        ),
        r'(^peacock_|_peacock$)',                       -- drop “peacock_” or “_peacock”
        ''
      ),
    r'[^a-zA-Z0-9]',
    ''
    )  
  ) AS cleaned_creative_name,
    --    "a" as source_table
  
  FROM
    `looker-studio-pro-452620`.`20250327_data_model`.`basis_utms_fromDCM` 
    where REGEXP_EXTRACT(`Click-through URL`, 'utm_source=(.*?)&') = "basis"
  ),
 
pvt as (
  SELECT
     url,
    id,
    placement,                              
    utm_source,
    utm_medium,
    utm_campaign,
    utm_term,
    utm_content,
    creative_name, 
    cleaned_creative_name,
--    "b" as source_table

FROM
  `looker-studio-pro-452620.repo_stg.basis_utms_stg_view`
  where utm_source = "basis"
  ),

sup  as (
    SELECT
    Final_URL                                        as url,
    regexp_extract(`Final_URL`,r'-(\d+)&utm_term')      AS id,
    REGEXP_REPLACE(                                -- ② strip leftover dashes/underscores at end
           REGEXP_REPLACE(                       -- ① remove creative_name (literal, case-sens.)
             DCM_placement,
             scrap,
             ''
           ),
           r'[\s_-]+$',
           ''
  )  AS placement,                              
    
    REGEXP_EXTRACT(`Final_URL`, 'utm_source=(.*?)&')    AS utm_source,
    REGEXP_EXTRACT(`Final_URL`, 'utm_medium=(.*?)&')    AS utm_medium,
    REGEXP_EXTRACT(`Final_URL`, 'utm_campaign=(.*?)&')  AS utm_campaign,
    REGEXP_EXTRACT(`Final_URL`, 'utm_term=(.*)')        AS utm_term,
    REGEXP_EXTRACT(`Final_URL`, 'utm_content=(.*?)&')   AS utm_content,
    scrap                                                    as creative_name, 
    LOWER(
    regexp_replace(
      REGEXP_REPLACE(
        LOWER(
          REPLACE(
            REGEXP_EXTRACT(
              scrap,
              r'^(?:\d+_)?([^_]+.*?)(?:_\d+x\d+.*)?$'   -- strip numeric prefix & size suffix
            ),
            ' ',                                        -- remove spaces
            ''
          )
        ),
        r'(^peacock_|_peacock$)',                       -- drop “peacock_” or “_peacock”
        ''
      ),
    r'[^a-zA-Z0-9]',
    ''
    )  
  ) AS cleaned_creative_name,
    
    
--    "c" as source_table
  FROM looker-studio-pro-452620.utm_scrap.utm_supp_table where utm_source = "basis"
    
  )

select * from dcm 
  union distinct  select * from pvt 
  union distinct select * from sup


  -- 712 + 193 = 905