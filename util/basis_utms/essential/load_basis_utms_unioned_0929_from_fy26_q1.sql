-- Incremental backfill script:
-- Add missing FY26 Q1 rows from landing.basis_utms_pivoted_fy26_q1
-- into landing.basis_utms_unioned-0929 (idempotent / safe to rerun).

DECLARE target_rows_before INT64;
DECLARE rows_inserted INT64;
DECLARE target_rows_after INT64;

SET target_rows_before = (
  SELECT COUNT(*)
  FROM `looker-studio-pro-452620.landing.basis_utms_unioned-0929`
);

CREATE TEMP TABLE rows_to_insert AS
SELECT DISTINCT
  CAST(src.line_item AS STRING) AS line_item,
  CAST(src.tag_placement AS STRING) AS tag_placement,
  CAST(src.name AS STRING) AS name,
  SAFE_CAST(src.end_date AS DATE) AS end_date,
  SAFE_CAST(src.start_date AS DATE) AS start_date,
  LOWER(
    COALESCE(
      REGEXP_EXTRACT(CAST(src.name AS STRING), r'(?i)(\d{2,4}x\d{2,4})'),
      REGEXP_EXTRACT(CAST(src.tag_placement AS STRING), r'(?i)(\d{2,4}x\d{2,4})'),
      REGEXP_EXTRACT(CAST(src.line_item AS STRING), r'(?i)(\d{2,4}x\d{2,4})')
    )
  ) AS size,
  CAST(src.formats AS STRING) AS formats,
  CAST(src.url AS STRING) AS url
FROM `looker-studio-pro-452620.landing.basis_utms_pivoted_fy26_q1` AS src
WHERE src.name IS NOT NULL
  AND TRIM(CAST(src.name AS STRING)) != ''
  AND src.url IS NOT NULL
  AND TRIM(CAST(src.url AS STRING)) != ''
  AND NOT EXISTS (
    SELECT 1
    FROM `looker-studio-pro-452620.landing.basis_utms_unioned-0929` AS tgt
    WHERE IFNULL(tgt.line_item, '') = IFNULL(CAST(src.line_item AS STRING), '')
      AND IFNULL(tgt.tag_placement, '') = IFNULL(CAST(src.tag_placement AS STRING), '')
      AND IFNULL(tgt.name, '') = IFNULL(CAST(src.name AS STRING), '')
      AND IFNULL(tgt.end_date, DATE '1900-01-01') = IFNULL(SAFE_CAST(src.end_date AS DATE), DATE '1900-01-01')
      AND IFNULL(tgt.start_date, DATE '1900-01-01') = IFNULL(SAFE_CAST(src.start_date AS DATE), DATE '1900-01-01')
      AND IFNULL(tgt.size, '') = IFNULL(
        LOWER(
          COALESCE(
            REGEXP_EXTRACT(CAST(src.name AS STRING), r'(?i)(\d{2,4}x\d{2,4})'),
            REGEXP_EXTRACT(CAST(src.tag_placement AS STRING), r'(?i)(\d{2,4}x\d{2,4})'),
            REGEXP_EXTRACT(CAST(src.line_item AS STRING), r'(?i)(\d{2,4}x\d{2,4})')
          )
        ),
        ''
      )
      AND IFNULL(tgt.formats, '') = IFNULL(CAST(src.formats AS STRING), '')
      AND IFNULL(tgt.url, '') = IFNULL(CAST(src.url AS STRING), '')
  );

SET rows_inserted = (SELECT COUNT(*) FROM rows_to_insert);

INSERT INTO `looker-studio-pro-452620.landing.basis_utms_unioned-0929`
  (line_item, tag_placement, name, end_date, start_date, size, formats, url)
SELECT
  line_item, tag_placement, name, end_date, start_date, size, formats, url
FROM rows_to_insert;

SET target_rows_after = (
  SELECT COUNT(*)
  FROM `looker-studio-pro-452620.landing.basis_utms_unioned-0929`
);

SELECT
  target_rows_before,
  rows_inserted,
  target_rows_after;
