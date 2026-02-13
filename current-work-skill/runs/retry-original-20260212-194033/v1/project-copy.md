# Current Work - Workspace (Resume Fast)

Updated: `2026-02-12 19:40:48 EST`
Workspace root: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal`
Directories scanned this run: `2`

## What You Were Working On

### 1) DCM + UTM join hardening
Based on local files and logs, this appears to be an active workstream right now.

<details><summary>Paths - DCM + UTM join hardening</summary>

- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/repo_stg__dcm_plus_utms.sql`

</details>

<details><summary>Tables - DCM + UTM join hardening</summary>

- `looker-studio-pro-452620.final_views.dcm`
- `looker-studio-pro-452620.final_views.utms_view`
- `looker-studio-pro-452620.repo_stg.dcm_plus_utms`

</details>

### 2) Offline sheet sync pipeline
Based on local files and logs, this appears to be an active workstream right now.

<details><summary>Paths - Offline sheet sync pipeline</summary>

- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/setup-mft-offline-daily-sheet-sync.sh`
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_daily_sheet_sync.sql`
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/mft_offline_update_manual.sql`
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/sql/stg__mm__mft_offline_connected_gsheet.sql`

</details>

<details><summary>Tables - Offline sheet sync pipeline</summary>

- `looker-studio-pro-452620.mass_mutual_mft_ext.mft_offline`
- `looker-studio-pro-452620.repo_stg.stg__mm__mft_offline_connected_gsheet`

</details>

### 3) Basis UTM merge/backfill work
Based on local files and logs, this appears to be an active workstream right now.

<details><summary>Paths - Basis UTM merge/backfill work</summary>

- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/load_basis_utms_unioned_0929_from_fy26_q1.sql`
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms/essential/stg__basis__utms.sql`
- `/private/tmp/basis_utms_workbook.ipynb`
- `/tmp/utm_updates_patched.sql`

</details>

<details><summary>Tables - Basis UTM merge/backfill work</summary>

- `looker-studio-pro-452620.20250327_data_model.basis_utms_pivoted`
- `looker-studio-pro-452620.20250327_data_model.basis_utms_pivoted_unioned`
- `looker-studio-pro-452620.final_views.utms_view`
- `looker-studio-pro-452620.landing.basis_utms_pivoted_fy26_q1`
- `looker-studio-pro-452620.landing.basis_utms_unioned`
- `looker-studio-pro-452620.landing.basis_utms_unioned-0929`
- `looker-studio-pro-452620.repo_stg.basis_utms`
- `looker-studio-pro-452620.repo_stg.basis_utms_stg_view`
- `looker-studio-pro-452620.repo_stg.basis_utms_stg_view_2507`
- `looker-studio-pro-452620.repo_stg.dcm_plus_utms`
- `repo_stg.basis_delivery.del_key`

</details>

## What You Should Do Next (In Order)

1. Verify DCM enrichment for scoped campaigns

```bash
cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft
./scripts/bq-safe-query.sh --max-rows 25 --sql "SELECT campaign, COUNT(*) AS rows, COUNTIF(utm_content IS NULL) AS null_utm_content FROM `looker-studio-pro-452620.repo_stg.dcm_plus_utms` WHERE campaign IN ('MassMutual20252026Media','MassMutualLVGP2025') AND date >= '2026-01-01' GROUP BY 1 ORDER BY rows DESC"
```

2. Check offline scheduled query health

```bash
bq ls --transfer_config --transfer_location=US --project_id=looker-studio-pro-452620
bq show --transfer_config --transfer_location=US projects/671028410185/locations/us/transferConfigs/699421ab-0000-2129-a27e-883d24f0f1b8
bq head -n 5 looker-studio-pro-452620:mass_mutual_mft_ext.mft_offline
```

3. Decide if temporary UTM patch should move into repo

```bash
bq query --use_legacy_sql=false < /tmp/utm_updates_patched.sql
```

4. Create a checkpoint commit in the workspace repo

```bash
cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal
git status --short
git add .
git commit -m "Checkpoint: current-work snapshot"
```

## Repo Status Snapshot

### `mft`
- Branch: `No commits yet on main`
- Working tree[^1]: staged=1, modified=0, untracked=7
- Remote activity[^2]:
  - `origin` (unknown) => local repository has no commits; url=`https://github.com/genetsen/mft.git`

### `util`
- Branch: `ADIF...origin/ADIF`
- Working tree[^1]: staged=0, modified=21, untracked=11
- Remote activity[^2]:
  - `Omni_remote` (main) => local HEAD differs from remote HEAD; url=`https://github.com/GiantSpoon-Tech/omni.git`
  - `origin` (main) => local HEAD differs from remote HEAD; url=`https://github.com/genetsen/looker_personal`

## Open Files Seen From VS Code Context[^4]

- `/private/tmp/basis_utms_workbook.ipynb`
- `/tmp/utm_updates_patched.sql`
- `/Users/eugenetsenter/.cline/skills/know/bq_via_jupyter_example.ipynb`
- `/Users/eugenetsenter/.codex/AGENTS.md`

## Current Risks

- `mft` has no local commit history yet.
- `util` has high untracked count (11).
- One or more active files are under `/tmp`, which is temporary storage.
- Active files are spread across multiple directories, so context can drift.

## Where I Looked This Run

- Workspace root: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal`
- Scanned directories:
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft`
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util`
- Codex data paths:
  - `/Users/eugenetsenter/.codex/sessions`
  - `/Users/eugenetsenter/.codex/shell_snapshots`
- Latest session files inspected:
  - `/Users/eugenetsenter/.codex/sessions/2026/02/12/rollout-2026-02-12T16-04-19-019c53ab-46dc-7fe3-92fd-10d7138bacb9.jsonl`
  - `/Users/eugenetsenter/.codex/sessions/2026/02/12/rollout-2026-02-12T16-01-53-019c53a9-0beb-7da0-afd0-3d459b226e16.jsonl`
  - `/Users/eugenetsenter/.codex/sessions/2026/02/12/rollout-2026-02-12T11-24-36-019c52ab-2efc-7433-a4ff-a21ad604d37b.jsonl`
  - `/Users/eugenetsenter/.codex/sessions/2026/02/11/rollout-2026-02-11T19-39-23-019c4f49-d18c-79a0-a31f-8b0663b8ccce.jsonl`
  - `/Users/eugenetsenter/.codex/sessions/2026/02/11/rollout-2026-02-11T19-16-15-019c4f34-a206-7c92-b42b-dd1ce9b6e9c5.jsonl`
  - `/Users/eugenetsenter/.codex/sessions/2026/02/11/rollout-2026-02-11T15-36-28-019c4e6b-6afc-7680-ad33-e025000f97b4.jsonl`
- Remote git checks executed:
  - `git -C /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft ls-remote --symref origin HEAD`
  - `git -C /Users/eugenetsenter/Looker_clonedRepo/looker_personal/util ls-remote --symref Omni_remote HEAD`
  - `git -C /Users/eugenetsenter/Looker_clonedRepo/looker_personal/util ls-remote --symref origin HEAD`
- Open-tab sources:
  - `/Users/eugenetsenter/.codex/sessions/2026/02/12/rollout-2026-02-12T16-04-19-019c53ab-46dc-7fe3-92fd-10d7138bacb9.jsonl`
  - `/Users/eugenetsenter/.codex/sessions/2026/02/12/rollout-2026-02-12T16-01-53-019c53a9-0beb-7da0-afd0-3d459b226e16.jsonl`
  - `/Users/eugenetsenter/.codex/sessions/2026/02/12/rollout-2026-02-12T11-24-36-019c52ab-2efc-7433-a4ff-a21ad604d37b.jsonl`
  - `/Users/eugenetsenter/.codex/sessions/2026/02/11/rollout-2026-02-11T19-39-23-019c4f49-d18c-79a0-a31f-8b0663b8ccce.jsonl`
  - `/Users/eugenetsenter/.codex/sessions/2026/02/11/rollout-2026-02-11T15-36-28-019c4e6b-6afc-7680-ad33-e025000f97b4.jsonl`
  - `/Users/eugenetsenter/.codex/sessions/2026/02/11/rollout-2026-02-11T15-44-32-019c4e72-cde8-7730-a7fb-88e4fe2fd81d.jsonl`
  - `/Users/eugenetsenter/.codex/sessions/2026/02/11/rollout-2026-02-11T15-11-30-019c4e54-8fb9-74a1-8933-0020b1a791a5.jsonl`

## Footnotes

[^1]: Working tree means your current local file state before commit.
[^2]: Remote means the git server copy of a repository (for example GitHub).[1]
[^3]: Branch means a named line of commits in git.[2]
[^4]: VS Code context here comes from local Codex session logs that include open-tab lists.

[1] Remote: the network location that stores shared git history.
[2] Branch: an isolated commit path used to organize changes.
