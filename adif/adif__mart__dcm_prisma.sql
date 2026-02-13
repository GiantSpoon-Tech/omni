CREATE OR REPLACE TABLE
  `looker-studio-pro-452620.repo_mart.adif__mart_dcm_plus_utms` AS

with a as (
SELECT
  * 
  EXCEPT (
    --package_id,
    n_of_placements
    --package_id AS dcm_PACKAGE_ID
  )
FROM
  looker-studio-pro-452620.DCM.20250505_costModel_v5
),
  
b as (
  SELECT
    DISTINCT * EXCEPT (placement_name,
      p_package_friendly)
  FROM
    looker-studio-pro-452620.Prisma.prisma_processed_plusDCMimps)

SELECT
  COALESCE(a.package_id, b.package_id) AS package_id,
  a.* EXCEPT (package_id),
  b.* EXCEPT (package_id)
FROM
  a
FULL OUTER JOIN
  b
ON
  a.package_id = b.package_id;

-- -- V2
-- CREATE OR REPLACE TABLE `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm` AS
-- WITH a AS (
--   SELECT
--     * EXCEPT(package_id, n_of_placements),
--     package_id,
--     "dcm" as data_source
--   FROM `looker-studio-pro-452620.repo_stg.dcm_plus_utms`
--   where p_advertiser_name = "Forevermark US"
-- ),
-- b AS (
--   SELECT DISTINCT
--     * EXCEPT(placement_name, p_package_friendly, placement_id),
--     "prisma" as data_source
--   FROM `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
--   where advertiser_name = "Forevermark US"
  
-- )
-- SELECT
--   COALESCE(a.package_id, b.package_id) AS package_id,
--   coalesce(a.date,b.date) as date,
--   COALESCE(a.data_source,b.data_source) as data_source,
--   a.* EXCEPT(package_id, date, data_source),
--   b.* EXCEPT(package_id, date, data_source),
  
-- FROM a
-- FULL OUTER JOIN b
-- ON a.package_id = b.package_id and a.date = b.date;


-- --V3
-- -- how to avoid duplicates?
-- CREATE OR REPLACE TABLE
--   `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm` AS
-- WITH
--   a AS (
--   SELECT
--     * EXCEPT (package_id,
--       n_of_placements),
--     package_id,
--   FROM
--     `looker-studio-pro-452620`.`repo_stg`.`dcm_plus_utms`
--   WHERE
--     p_advertiser_name = "Forevermark US" ),
--   b AS (
--   SELECT
--     DISTINCT * EXCEPT (placement_name,
--       p_package_friendly,
--       placement_id),
--     placement_id as p_placement_id
--   FROM
--     `looker-studio-pro-452620`.`20250327_data_model`.`prisma_expanded_full`
--   WHERE
--     advertiser_name = "Forevermark US" ),
  
--   -- Deduplicate CTE a based on package_id and date
--   deduplicated_a AS (
--   SELECT
--     a.*
--   FROM
--     `a`
--   QUALIFY
--     ROW_NUMBER() OVER (PARTITION BY package_id, placement_id, date ORDER BY 1) = 1 ),
--   -- Deduplicate CTE b based on package_id and date
--   deduplicated_b AS (
--   SELECT
--     b.*
--   FROM
--     `b`
--   QUALIFY
--     ROW_NUMBER() OVER (PARTITION BY package_id, p_placement_id, date ORDER BY 1) = 1 )
-- SELECT
--   COALESCE(da.package_id, db.package_id) AS package_id,
--   COALESCE(da.date, db.date) AS date,

--   da.* EXCEPT (package_id,
--     date),
--   db.* EXCEPT (package_id,
--     date)
-- FROM
--   `deduplicated_a` AS da
-- FULL OUTER JOIN
--   `deduplicated_b` AS db
-- ON
--   da.package_id = db.package_id and
--   da.placement_id = db.p_placement_id
--   AND da.date = db.date;
