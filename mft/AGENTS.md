# MFT Agent Rules

## BigQuery Guardrails
- For BigQuery reads, use `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/scripts/bq-safe-query.sh` by default.
- Do not run raw `bq query` for data pulls unless the user explicitly asks to bypass guardrails.
- Apply the same guardrails when using MCP (`bigquery` server, `run_query` tool):
  - no `SELECT *` by default
  - default row ceiling `LIMIT <= 50`
  - prefer summary-first queries before detailed row pulls
- Prefer `--schema-only` when the user asks for table columns.
- Keep result outputs small by default:
  - `--max-rows 50`
  - `--max-bytes 176000000000` (about `$1` at `$6.25/TiB`)
- When larger output is requested, confirm and increase limits intentionally.
- If a query is blocked by guardrails, provide summary alternatives instead of returning raw rows:
  - row count and date span
  - top categories by volume
  - narrow explicit-column sample (`LIMIT 50`)
  - schema exploration via `INFORMATION_SCHEMA.COLUMNS`

## Changelog Consolidation
- Keep `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/CHANGELOG.md` concise and daily:
  - include only essential high-level outcomes for each day
  - keep formatting simple and preview-stable
  - avoid session-by-session and prompt by prompt implementation noise
- Keep detailed implementation/session history in `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/CHANGELOG_EXTENDED.md`.
- When adding new entries:
  - update both files in the same turn
  - do not remove or rewrite unrelated historical entries
  - update only the thread-related entry unless explicitly asked to refactor older entries
  - in `CHANGELOG.md`, use concise `What` + `Why` for `Added`/`Changed` and focus on outcomes, not process churn
  - in `CHANGELOG_EXTENDED.md`, keep full context and detailed breakdown
  - bold each main item title (for example `**Documentation and changelog standards**`)
  - keep path references in dedicated collapsible blocks:
    - use `<details><summary>Paths — [Title]</summary>` blocks
    - put each path on its own line as a clickable markdown link
    - keep long URLs on their own lines
- For `Fixed` entries in `CHANGELOG.md`:
  - include `Issue` and `Cause`
  - only include `Resolution` after verification
  - if unresolved/unverified, use `Verification: pending` and add/update `## Verification TODOs` below

## Verification TODOs
- None currently.
- Offline daily scheduled query fix was verified with successful transfer updates and successful runs for active schedule `ext_mm_mft_offlineData_sched`.
