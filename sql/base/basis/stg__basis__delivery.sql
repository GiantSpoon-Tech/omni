-- @file: stg__basis__delivery.sql
-- @layer: staging
-- @description:
--   Creates the staging view **repo_stg.basis_delivery**.
--   Adds two data-quality / join helper fields to `basis_master2`:
--     • **id** – numeric placement identifier extracted from `placement`
--     • **cleaned_creative_name** – lower-cased, space-stripped creative name
--       with numeric prefixes, size suffixes, and the word “peacock” removed.
--   Downstream models use these fields to join with UTM metadata and
--   to de-duplicate creatives.
--
-- @source: giant-spoon-299605.data_model_2025.basis_master2
-- @target: repo_stg.basis_delivery   -- ✓ view
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW `repo_stg.basis_delivery` AS
SELECT
  *,
  REGEXP_EXTRACT(placement, r'CP_(\d+)') AS id,
  LOWER(
    regexp_replace(
      REGEXP_REPLACE(
        LOWER(
          REPLACE(
            REGEXP_EXTRACT(
              creative_name,
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
  IFNULL(
    concat(
      lower(placement),
      --REGEXP_EXTRACT(placement, r'CP_(\d+)'),
      " || ",
      LOWER(
        regexp_replace(
          REGEXP_REPLACE(
            LOWER(
              REPLACE(
                REGEXP_EXTRACT(
                  creative_name,
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
      )
    ),
    CONCAT(placement," || ", creative_name)
  ) AS del_key,
FROM
  `giant-spoon-299605.data_model_2025.basis_master2`;

-- select
-- count(distinct placement), count(distinct REGEXP_EXTRACT(placement, r'CP_(\d+)') ) from `repo_stg.basis_delivery`