# Changelog

All notable changes to this repository are documented in this file.

## 2026-02-12

### Added

- **Basis UTMs Workspace README**
  What: Added a dedicated utilities README describing how Basis UTM assets are organized into active and archived subfolders.
  Why: Creates a clear entrypoint for running current scripts and locating legacy references.
  <details><summary>Paths — Basis UTMs Workspace README</summary>

  [util/basis_utms/README.md](util/basis_utms/README.md)

  </details>

- **FY26-to-0929 Basis UTM Backfill Script**
  What: Added an idempotent SQL script to insert missing rows from `landing.basis_utms_pivoted_fy26_q1` into `landing.basis_utms_unioned-0929`, with before/after row-count output.
  Why: Provides a repeatable way to update the pipeline’s `-0929` unioned table from newly loaded FY26 Q1 pivoted data.
  <details><summary>Paths — FY26-to-0929 Basis UTM Backfill Script</summary>

  [util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql](util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql)
  [util/basis_utms/README.md](util/basis_utms/README.md)

  </details>

### Changed

- **Basis UTMs Script Reorganization**
  What: Reorganized Basis UTM R/SQL assets into `util/basis_utms/essential` for active workflows and `util/basis_utms/archive` for legacy/scratch artifacts.
  Why: Reduces root-level utility clutter and separates operational scripts from historical/debug materials.
  <details><summary>Paths — Basis UTMs Script Reorganization</summary>

  [util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r](util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r)
  [util/basis_utms/essential/util_b_utm_validation.r](util/basis_utms/essential/util_b_utm_validation.r)
  [util/basis_utms/essential/stg3_b_plus_utms_PnS.sql](util/basis_utms/essential/stg3_b_plus_utms_PnS.sql)
  [util/basis_utms/essential/get_distinct_creative_names.sql](util/basis_utms/essential/get_distinct_creative_names.sql)
  [util/basis_utms/archive/util__basis__utm_pivot_longer.r](util/basis_utms/archive/util__basis__utm_pivot_longer.r)
  [util/basis_utms/archive/util__basis__utm_pivot_longer_clean.r](util/basis_utms/archive/util__basis__utm_pivot_longer_clean.r)
  [util/basis_utms/archive/scrap.sql](util/basis_utms/archive/scrap.sql)
  [util/basis_utms/archive/testsAndScrap.sql](util/basis_utms/archive/testsAndScrap.sql)
  [util/basis_utms/archive/utm_validation_scrap.sql](util/basis_utms/archive/utm_validation_scrap.sql)
  [util/basis_utms/archive/union_basis_utms.ipynb](util/basis_utms/archive/union_basis_utms.ipynb)
  [util/basis_utms/archive/b_utms_diagram.md](util/basis_utms/archive/b_utms_diagram.md)

  </details>

- **FY26-to-0929 Backfill Size Mapping**
  What: Updated the FY26 backfill script to derive `size` from creative fields (`name`, `tag_placement`, `line_item`) instead of inserting `NULL`.
  Why: Preserves size information when loading `basis_utms_unioned-0929` and keeps idempotent dedupe matching aligned with inserted size values.
  <details><summary>Paths — FY26-to-0929 Backfill Size Mapping</summary>

  [util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql](util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Basis UTMs Documentation Path Updates**
  What: Updated MFT and root README references to point at the new Basis UTMs folder structure.
  Why: Prevents stale links and keeps project navigation accurate after the script move.
  <details><summary>Paths — Basis UTMs Documentation Path Updates</summary>

  [mft/README.md](mft/README.md)
  [README.md](README.md)

  </details>

- **FY26 Q1 Traffic Sheet Source Configuration**
  What: Added a new `fy26_q1` source in the Basis UTM loop script for `/Users/eugenetsenter/Downloads/MassMutual_FY26_Q1_Traffic Sheet.xlsx` using tab `MASSMUTUAL004_updated 1.14.26`, plus included it in `sources_to_process`.
  Why: Ensures the FY26 Q1 MASSMUTUAL004 trafficking sheet is processed in the existing batch ingestion flow.
  <details><summary>Paths — FY26 Q1 Traffic Sheet Source Configuration</summary>

  [util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r](util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Basis UTM Union and Staging Critical SQL Coverage**
  What: Updated the Basis UTM union SQL to include `landing.basis_utms_pivoted_fy26_q1` and documented `load_basis_utms_union.sql` plus `stg__basis__utms.sql` as pipeline-critical dependencies in project docs.
  Why: Keeps the downstream staging view in sync with the new FY26 Q1 source and makes core SQL dependencies explicit for ongoing operations.
  <details><summary>Paths — Basis UTM Union and Staging Critical SQL Coverage</summary>

  [util/basis_utms/essential/load_basis_utms_union.sql](util/basis_utms/essential/load_basis_utms_union.sql)
  [util/basis_utms/essential/stg__basis__utms.sql](util/basis_utms/essential/stg__basis__utms.sql)
  [util/basis_utms/README.md](util/basis_utms/README.md)
  [mft/README.md](mft/README.md)

  </details>

- **Basis UTM Core SQL Relocation to Essential Folder**
  What: Moved `load_basis_utms_union.sql` and `stg__basis__utms.sql` into `util/basis_utms/essential` and updated cross-doc references.
  Why: Keeps all operational Basis UTM extraction and staging assets co-located in one maintained essential workspace.
  <details><summary>Paths — Basis UTM Core SQL Relocation to Essential Folder</summary>

  [util/basis_utms/essential/load_basis_utms_union.sql](util/basis_utms/essential/load_basis_utms_union.sql)
  [util/basis_utms/essential/stg__basis__utms.sql](util/basis_utms/essential/stg__basis__utms.sql)
  [util/basis_utms/README.md](util/basis_utms/README.md)
  [mft/README.md](mft/README.md)
  [CLAUDE.md](CLAUDE.md)

  </details>

- **Basis UTM Rebuild and Cleanup TODOs**
  What: Added explicit project TODOs to rebuild the Basis UTM pipeline with one canonical runbook and clean up confusing remnant local/warehouse assets.
  Why: Reduces operational ambiguity and prepares a safer long-term maintenance path for the Basis-to-MFT endpoint workflow.
  <details><summary>Paths — Basis UTM Rebuild and Cleanup TODOs</summary>

  [AGENTS.md](AGENTS.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

- **Systemwide SQL QA Safety Gate**
  What: Added a systemwide SQL QA protocol requiring isolated `_qa` validation, proof output review, and explicit approval before live SQL patches.
  Why: Prevents accidental production-impacting SQL changes and makes QA evidence-driven by default.
  <details><summary>Paths — Systemwide SQL QA Safety Gate</summary>

  [AGENTS.md](AGENTS.md)
  [README.md](README.md)
  [CHANGELOG.md](CHANGELOG.md)

  </details>

## [Unreleased]

### Added - 2026-02-06

- Added FPD loader documentation TODO to create a reusable Codex skill for Excel-to-standard-input conversion in `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/FPD_loader/README.md`.
- Added ADIF social layering SQL at `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/stg__adif__social_crossplatform.sql`.
- Added cross-brand data flow diagram documentation for OLI, MassMutual, and ADIF at `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/DATA_FLOW_DIAGRAMS.md` covering ingestion -> BigQuery -> dbt -> dashboards.

### Changed - 2026-02-06

- Updated ADIF social staging filter in `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/stg__adif__social_crossplatform.sql` to require both an `account_name` allowlist (`ADIF USA`, `A Diamond is Forever - US`, `A Diamond is Forever`, `De Beers Group`) and literal `WP_` presence in `campaign_name`.

### Changed - 2026-02-09

- Updated `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/FPD_loader/util_collect_fpd_v3.r` so final Phase 7 outputs round `impressions` and `clicks` to whole-number integers before validation, CSV output, and BigQuery upload.
- Enhanced `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/FPD_loader/util_collect_fpd_v3.r` to add Phase 6 filter-audit output (`phase6_filter_audit.csv`), save a full Phase 5-vs-Phase 7 validation table (`phase7_validation_table.csv`), add aggregated `filter_reason`, and print mismatches at end-of-run output.
- Updated `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl.sql` to clone base schema/data from `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view` instead of `...view_v3_test`, preserving updated-FPD fields in the social-layered table.
- Updated `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/query__adif__prisma_expanded_plus_dcm_with_social_tbl_sched.sql` to use `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view` for both schema cloning and final base-row union output.
- Updated `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/AGENTS.md` with sectioned workflow formatting and the ADIF social layer + updated FPD run commands.
