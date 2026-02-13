# Current Work - Workspace (Resume Fast)

Updated: `2026-02-12 20:08:10 EST`
Workspace root: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal`
Directories scanned this run: `2`

## Headlines

1. **Basis UTM merge and FY26 Q1 backfill** - You were working on Basis UTM union/backfill logic and a patched SQL flow in `/tmp`. Confidence: high. Next: Validate patched SQL once, then decide whether to move it into repo (about 10 minutes).
2. **Offline sheet daily sync pipeline** - You were maintaining the sheet-to-BigQuery sync used for `mft_offline` updates. Confidence: high. Next: Verify scheduled-query status and latest rows (about 7 minutes).
3. **DCM + UTM enrichment hardening** - You were tightening how DCM rows get UTM fields with a constrained fallback for specific campaigns. Confidence: medium. Next: Run a null-UTM health check for scoped campaigns (about 8 minutes).

## Details

### Do First (5 Minutes)
Validate patched SQL once, then decide whether to move it into repo (estimated 10 minutes)

```bash
bq query --use_legacy_sql=false < /tmp/utm_updates_patched.sql
```

### 1) Basis UTM merge and FY26 Q1 backfill
What this means: You were working on Basis UTM union/backfill logic and a patched SQL flow in `/tmp`.

Confidence: high

Why this is prioritized now: Open tabs and recent util file edits both point to Basis UTM backfill tasks right now.

Why it matters: This controls whether Basis delivery rows join into UTM layers without gaps or duplicate logic.

Next step now: Validate patched SQL once, then decide whether to move it into repo (estimated 10 minutes)

How to do this: Run the command one time and confirm the output looks sane; then choose whether to keep temporary logic or copy it into the repo file. Start with [stg__basis__utms.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/stg__basis__utms.sql), [load_basis_utms_unioned_0929_from_fy26_q1.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql), [README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/README.md).

```bash
bq query --use_legacy_sql=false < /tmp/utm_updates_patched.sql
```

<details><summary>Signals - Basis UTM merge and FY26 Q1 backfill</summary>

- `open-tab hit`
- `path hits=8`
- `table hits=11`

</details>

<details><summary>Paths - Basis UTM merge and FY26 Q1 backfill</summary>

- 2026-02-11 20:57:50 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/stg__basis__utms.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/stg__basis__utms.sql)
- 2026-02-11 20:44:11 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql)
- 2026-02-11 20:43:23 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/README.md](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/README.md)
- 2026-02-11 20:13:21 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_union.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_union.sql)
- 2026-02-11 19:54:20 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r)
- 2026-02-11 19:45:48 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/archive/union_basis_utms.ipynb](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/archive/union_basis_utms.ipynb)
- [/private/tmp/basis_utms_workbook.ipynb](/private/tmp/basis_utms_workbook.ipynb)
- [/tmp/utm_updates_patched.sql](/tmp/utm_updates_patched.sql)

</details>

<details><summary>Tables - Basis UTM merge and FY26 Q1 backfill</summary>

- `looker-studio-pro-452620.landing.basis_utms_unioned`
- `looker-studio-pro-452620.landing.basis_utms_unioned-0929`
- `looker-studio-pro-452620.landing.basis_utms_pivoted_fy26_q1`
- `looker-studio-pro-452620.landing.basis_utms_pivoted_flight1_2`
- `looker-studio-pro-452620.landing.basis_utms_pivoted_flight2_2`
- `looker-studio-pro-452620.landing.basis_utms_pivoted_flight3_2`

</details>

### 2) Offline sheet daily sync pipeline
What this means: You were maintaining the sheet-to-BigQuery sync used for `mft_offline` updates.

Confidence: high

Why this is prioritized now: Recent scheduler and sync SQL files are concentrated in this workflow.

Why it matters: If this schedule breaks, offline reporting can drift from source-sheet truth.

Next step now: Verify scheduled-query status and latest rows (estimated 7 minutes)

How to do this: Run the check, confirm expected status and recent rows, then capture what you saw. Start with [mft_offline_update_manual.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_update_manual.sql), [stg__mm__mft_offline_connected_gsheet.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/stg__mm__mft_offline_connected_gsheet.sql), [mft_offline_daily_sheet_sync.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_daily_sheet_sync.sql).

```bash
bq ls --transfer_config --transfer_location=US --project_id=looker-studio-pro-452620
bq show --transfer_config --transfer_location=US projects/671028410185/locations/us/transferConfigs/699421ab-0000-2129-a27e-883d24f0f1b8
bq head -n 5 looker-studio-pro-452620:mass_mutual_mft_ext.mft_offline
```

<details><summary>Signals - Offline sheet daily sync pipeline</summary>

- `path hits=4`
- `table hits=2`

</details>

<details><summary>Paths - Offline sheet daily sync pipeline</summary>

- 2026-02-11 17:52:56 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_update_manual.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_update_manual.sql)
- 2026-02-11 17:52:46 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/stg__mm__mft_offline_connected_gsheet.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/stg__mm__mft_offline_connected_gsheet.sql)
- 2026-02-11 17:52:35 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_daily_sheet_sync.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_daily_sheet_sync.sql)
- 2026-02-11 17:48:08 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/setup-mft-offline-daily-sheet-sync.sh](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/setup-mft-offline-daily-sheet-sync.sh)

</details>

<details><summary>Tables - Offline sheet daily sync pipeline</summary>

- `looker-studio-pro-452620.mass_mutual_mft_ext.mft_offline`
- `looker-studio-pro-452620.repo_stg.stg__mm__mft_offline_connected_gsheet`

</details>

### 3) DCM + UTM enrichment hardening
What this means: You were tightening how DCM rows get UTM fields with a constrained fallback for specific campaigns.

Confidence: medium

Why this is prioritized now: Recent SQL edits and key table hits point to active DCM-UTM alignment work.

Why it matters: This prevents null UTM rows and keeps attribution reporting stable.

Next step now: Run a null-UTM health check for scoped campaigns (estimated 8 minutes)

How to do this: Run the command and confirm the key metric changed in the expected direction. Start with [repo_stg__dcm_plus_utms.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/repo_stg__dcm_plus_utms.sql).

```bash
cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft
./scripts/bq-safe-query.sh --max-rows 25 --sql "SELECT campaign, COUNT(*) AS rows, COUNTIF(utm_content IS NULL) AS null_utm_content FROM `looker-studio-pro-452620.repo_stg.dcm_plus_utms` WHERE campaign IN ('MassMutual20252026Media','MassMutualLVGP2025') AND date >= '2026-01-01' GROUP BY 1 ORDER BY rows DESC"
```

<details><summary>Signals - DCM + UTM enrichment hardening</summary>

- `path hits=1`
- `table hits=3`

</details>

<details><summary>Paths - DCM + UTM enrichment hardening</summary>

- 2026-02-11 19:40:09 EST - [/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/repo_stg__dcm_plus_utms.sql](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/repo_stg__dcm_plus_utms.sql)

</details>

<details><summary>Tables - DCM + UTM enrichment hardening</summary>

- `looker-studio-pro-452620.repo_stg.dcm_plus_utms`
- `looker-studio-pro-452620.final_views.dcm`
- `looker-studio-pro-452620.final_views.utms_view`

</details>

## Repo Status

### `mft`
- Branch[^3]: `No commits yet on main`
- Working tree[^1]: staged=1, modified=0, untracked=7
- Remote activity[^2]:
  - `origin` (unknown) => local repository has no commits; url=`https://github.com/genetsen/mft.git`

### `util`
- Branch[^3]: `ADIF...origin/ADIF`
- Working tree[^1]: staged=0, modified=21, untracked=11
- Remote activity[^2]:
  - `Omni_remote` (main) => local HEAD differs from remote HEAD; url=`https://github.com/GiantSpoon-Tech/omni.git`
  - `origin` (main) => local HEAD differs from remote HEAD; url=`https://github.com/genetsen/looker_personal`

## Open Files Seen From VS Code Context[^4]

- [/private/tmp/basis_utms_workbook.ipynb](/private/tmp/basis_utms_workbook.ipynb)
- [/tmp/utm_updates_patched.sql](/tmp/utm_updates_patched.sql)
- [/Users/eugenetsenter/.cline/skills/know/bq_via_jupyter_example.ipynb](/Users/eugenetsenter/.cline/skills/know/bq_via_jupyter_example.ipynb)
- [/Users/eugenetsenter/.codex/current-work.md](/Users/eugenetsenter/.codex/current-work.md)
- [/Users/eugenetsenter/.cline/skills/know/_test_viz_cell.py](/Users/eugenetsenter/.cline/skills/know/_test_viz_cell.py)
- [/Users/eugenetsenter/.codex/AGENTS.md](/Users/eugenetsenter/.codex/AGENTS.md)

## Current Risks

- `mft` has no local commit history.
- `util` has a high untracked count (11).
- At least one active file is under `/tmp`, which is temporary storage.

## Sample Scope Note

- This run used a limited directory set, so some workspace areas were intentionally skipped.
- Skipped top-level directories in this sample run:
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/dim_model`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/Explorations`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/Prisma`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/FPD`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/apollo`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/scrap`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/omni`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/olipop`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/sql`

## Footnotes

[^1]: Working tree means your local file state before commit.
[^2]: Remote means the server-side git copy (for example GitHub).
[^3]: Branch means a named commit line in git.
[^4]: VS Code context here comes from local session log messages that include open tabs.
[^5]: PKM means personal knowledge management notes.
