# Current Work - MFT (Resume Fast)

Updated: `2026-02-12 18:51:17 EST`
Target repo: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft`

## What You Were Working On

### 1) DCM + UTM join hardening was the main focus
You were fixing missing UTM enrichment in 2026 rows by keeping exact key matching first, then using a constrained fallback for two campaigns only.

<details><summary>Paths - DCM + UTM hardening</summary>

- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/repo_stg__dcm_plus_utms.sql`
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/README.md`
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/CHANGELOG.md`

</details>

<details><summary>Tables - DCM + UTM hardening</summary>

- `looker-studio-pro-452620.repo_stg.dcm_plus_utms`
- `looker-studio-pro-452620.final_views.utms_view`
- `looker-studio-pro-452620.final_views.dcm`
- `looker-studio-pro-452620.repo_mart.mft_view`

</details>

### 2) Offline sheet to BigQuery pipeline was also updated
You set up a daily sync flow that pulls the connected sheet into staging, then writes native output for reporting.

<details><summary>Paths - Offline pipeline</summary>

- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/setup-mft-offline-daily-sheet-sync.sh`
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_daily_sheet_sync.sql`
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/stg__mm__mft_offline_connected_gsheet.sql`
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_update_manual.sql`

</details>

<details><summary>Tables - Offline pipeline</summary>

- `looker-studio-pro-452620.repo_stg.stg__mm__mft_offline_connected_gsheet`
- `looker-studio-pro-452620.mass_mutual_mft_ext.mft_offline`

</details>

### 3) Basis UTM backfill work was active in sibling util files
Your currently open files indicate active work on FY26 Q1 union/backfill logic and a patched UTM update query in `/tmp`.

<details><summary>Paths - Basis UTM backfill work</summary>

- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_union.sql`
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql`
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r`
- `/tmp/utm_updates_patched.sql`
- `/private/tmp/basis_utms_workbook.ipynb`

</details>

<details><summary>Tables - Basis UTM backfill work</summary>

- `giant-spoon-299605.data_model_2025.mm_utms_raw_string`
- `giant-spoon-299605.data_model_2025.mm_utms_snapshot`
- `looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab`
- `looker-studio-pro-452620.utm_scrap.master_utms_raw`
- `looker-studio-pro-452620.landing.basis_utms_unioned-0929`
- `looker-studio-pro-452620.landing.basis_utms_pivoted_fy26_q1`

</details>

## What You Should Do Next (In Order)

1. Verify DCM enrichment is still healthy for the two scoped campaigns.

```bash
cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft
./scripts/bq-safe-query.sh --max-rows 25 --sql "SELECT campaign, COUNT(*) AS rows, COUNTIF(utm_content IS NULL) AS null_utm_content FROM `looker-studio-pro-452620.repo_stg.dcm_plus_utms` WHERE campaign IN ('MassMutual20252026Media','MassMutualLVGP2025') AND date >= '2026-01-01' GROUP BY 1 ORDER BY rows DESC"
```

2. Confirm offline daily sync schedule and output table are healthy.

```bash
bq ls --transfer_config --transfer_location=US --project_id=looker-studio-pro-452620
bq show --transfer_config --transfer_location=US projects/671028410185/locations/us/transferConfigs/699421ab-0000-2129-a27e-883d24f0f1b8
bq head -n 5 looker-studio-pro-452620:mass_mutual_mft_ext.mft_offline
```

3. Decide whether `/tmp/utm_updates_patched.sql` should be promoted into repo SQL, then run once to validate if needed.

```bash
bq query --use_legacy_sql=false < /tmp/utm_updates_patched.sql
```

4. Create a checkpoint commit so this work is easy to resume later.[^5]

```bash
cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft
git add .
git commit -m "Checkpoint: MFT + Basis UTM work status"
```

## Current Risks

- This repo has no commits yet, so there is no clean checkpoint for rollback or diff review.[^5]
- Basis UTM files are outside the `mft` repo, so context is split across locations.
- `/tmp/utm_updates_patched.sql` is easy to lose because `/tmp` is temporary.

## Quick Timeline (Latest Signals)

- 2026-02-12 12:45:24 EST: `/tmp/utm_updates_patched.sql`
- 2026-02-12 12:21:20 EST: `/private/tmp/basis_utms_workbook.ipynb`
- 2026-02-11 20:44:11 EST: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql`
- 2026-02-11 20:13:21 EST: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_union.sql`
- 2026-02-11 19:54:20 EST: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r`
- 2026-02-11 19:40:09 EST: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/repo_stg__dcm_plus_utms.sql`

## Footnotes

[^1]: BigQuery is Google's cloud SQL warehouse.
[^2]: View means a saved SQL query result definition, not a copied table.
[^3]: UTM means URL tracking fields like source/medium/campaign.
[^4]: Idempotent means safe to run again without creating duplicate outcomes.
[^5]: Working tree means your local file state before commit.
