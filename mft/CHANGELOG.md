# MFT Data Pipeline Changelog (Base Path: /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft)

Concise daily essentials are documented in this file.
Detailed session-level changes are documented in [CHANGELOG_EXTENDED.md](CHANGELOG_EXTENDED.md).

## 2026-02-12

### Changed
#### **Global changelog content standards**  
What: codified changelog content rules in repo guidance and docs, including preserving unrelated history, outcome-first `What`/`Why`, and dedicated collapsible path sections.  
Why: keep future updates consistent and reduce formatting churn across sessions.

<details>
<summary>Paths — Global changelog content standards</summary>

- [AGENTS.md](AGENTS.md)
- [README.md](README.md)

</details>

#### **DCM UTM join hardening (campaign-scoped fallback)**  
What: added a deployable SQL definition for `repo_stg.dcm_plus_utms` with exact-key UTM matching plus a normalized fallback scoped to `MassMutual20252026Media` and `MassMutualLVGP2025`; updated README lineage notes to match.  
Why: fix known creative-key mismatches for targeted MassMutual campaigns while preventing unintended backfill changes in other campaigns.

<details>
<summary>Paths — DCM UTM join hardening (campaign-scoped fallback)</summary>

- [scripts/sql/repo_stg__dcm_plus_utms.sql](scripts/sql/repo_stg__dcm_plus_utms.sql)
- [README.md](README.md)

</details>

### Fixed
#### **MassMutual DCM UTM enrichment gaps (2026 null `utm_content`)**  
Issue: targeted MassMutual campaign rows in 2026 were flowing to the endpoint with null UTM fields.  
Cause: exact join on `placement_id + creative_assignment` in `repo_stg.dcm_plus_utms` failed on creative naming mismatches (case differences and `px` suffix variants).  
Resolution: deployed constrained normalized fallback join logic for `MassMutual20252026Media` and `MassMutualLVGP2025`, and verified post-deploy that null `utm_content` dropped to `0` for those campaigns in both `repo_stg.dcm_plus_utms` and `repo_mart.mft_view`.

<details>
<summary>Paths — MassMutual DCM UTM enrichment gaps (2026 null utm_content)</summary>

- [scripts/sql/repo_stg__dcm_plus_utms.sql](scripts/sql/repo_stg__dcm_plus_utms.sql)
- [README.md](README.md)
- [CHANGELOG.md](CHANGELOG.md)

</details>

## 2026-02-11

### Added
#### **BigQuery guardrails and fallback guidance**  
What: added safe-query defaults, schema-only support, summary-first fallback guidance, and explicit bypass instructions.  
Why: reduce expensive/token-heavy pulls while keeping analyst workflows unblocked.

<details>
<summary>Paths — BigQuery guardrails and fallback guidance</summary>

- [scripts/bq-safe-query.sh](scripts/bq-safe-query.sh)
- [README.md](README.md)
- [AGENTS.md](AGENTS.md)

</details>

#### **MM MMM EXTERNAL Offline DATA PIPELINE**  
##### What:   created a gsheets connected staging table  and created an update script that updates external table daily
<details><summary>Paths</summary>
- created a gsheets connected staging table  
   gsheet path: [Google Sheet](https://docs.google.com/spreadsheets/d/15DddW291w_O7WWv8F0AcOcumEMYJSdm9hWKU9vE5WPQ/edit?gid=1999096908#gid=1999096908) (`[NEW] INTERNAL | COMBINED DATA`, `A:U`)  
   bq staging table (full path): `looker-studio-pro-452620.repo_stg.stg__mm__mft_offline_connected_gsheet`  
- created an update script that updates external table daily (full overwrite)  
   bq ext table (full path): `looker-studio-pro-452620.mass_mutual_mft_ext.mft_offline`  
   BQ scheadule: [Transfer Config](https://console.cloud.google.com/bigquery/transfers/location/us/configs/699421ab-0000-2129-a27e-883d24f0f1b8?project=looker-studio-pro-452620)  
</details>
 Why: enable reliable refresh of `looker-studio-pro-452620.mass_mutual_mft_ext.mft_offline` from connected sheet sources.

<details>
<summary>Paths — MM MMM EXTERNAL Offline DATA PIPELINE</summary>

- [scripts/sql/mft_offline_daily_sheet_sync.sql](scripts/sql/mft_offline_daily_sheet_sync.sql)
- [scripts/sql/stg__mm__mft_offline_connected_gsheet.sql](scripts/sql/stg__mm__mft_offline_connected_gsheet.sql)
- [scripts/sql/mft_offline_update_manual.sql](scripts/sql/mft_offline_update_manual.sql)
- [README.md](README.md)

</details>

### Changed
#### **Documentation and changelog standards**  
What: aligned endpoint/pipeline docs and split changelog into concise daily essentials plus extended details.  
Why: improve readability while preserving full implementation history.

<details>
<summary>Paths — Documentation and changelog standards</summary>

- [README.md](README.md)
- [CHANGELOG.md](CHANGELOG.md)
- [CHANGELOG_EXTENDED.md](CHANGELOG_EXTENDED.md)
- [AGENTS.md](AGENTS.md)

</details>
