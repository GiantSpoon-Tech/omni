# Basis UTMs Utilities

Organized workspace for MassMutual Basis UTM extraction, validation, and supporting SQL.

## Folder Structure

- `essential/`: Actively used scripts and SQL for the current workflow.
- `archive/`: Legacy, exploratory, or scratch assets retained for reference.

## Essential Scripts

- `essential/util__basis__utm_pivot_longer_loop.r`
  - Batch pipeline that loads Basis trafficking sheets, pivots UTM creatives to long format, standardizes names, and writes to BigQuery.
- `essential/util_b_utm_validation.r`
  - Validation helper that queries BigQuery delivery data to inspect creative-name coverage and mappings.
- `essential/stg3_b_plus_utms_PnS.sql`
  - Main SQL join workflow that combines delivery keys and parsed UTM data.
- `essential/get_distinct_creative_names.sql`
  - Utility SQL for extracting distinct creative names and null-UTM diagnostics.
- `essential/load_basis_utms_unioned_0929_from_fy26_q1.sql`
  - Idempotent backfill script that inserts missing FY26 Q1 rows from `landing.basis_utms_pivoted_fy26_q1` into `landing.basis_utms_unioned-0929`.

## Pipeline-Critical SQL Dependencies

- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_union.sql`
  - Unions all `landing.basis_utms_pivoted_*` tables (including FY26 Q1) into `landing.basis_utms_unioned`.
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/stg__basis__utms.sql`
  - Parses UTM parameters from `landing.basis_utms_unioned` into the staging view used by downstream Basis+UTM joins.

## Archive Contents

Archive includes older R script variants, ad hoc SQL scratchpads, and legacy notebook/diagram artifacts:

- `archive/util__basis__utm_pivot_longer.r`
- `archive/util__basis__utm_pivot_longer_clean.r`
- `archive/scrap.sql`
- `archive/testsAndScrap.sql`
- `archive/utm_validation_scrap.sql`
- `archive/union_basis_utms.ipynb`
- `archive/b_utms_diagram.md`

## Quick Run Commands

```bash
Rscript util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r
Rscript util/basis_utms/essential/util_b_utm_validation.r
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql
```

Use project auth/environment defaults for BigQuery access before running these scripts.
