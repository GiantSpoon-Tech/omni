# Current Work Skill

Generate resume-first `current-work.md` documents that answer two restart questions quickly:

1. What was I working on?
2. What should I do next?

Project location:
- `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill`

## What each run scans

- Selected workspace subdirectories (or all top-level directories if none are specified).
- Global Codex logs under `~/.codex/sessions` and `~/.codex/shell_snapshots`.
- Git status and remote-head checks for scanned git repositories.
- VS Code open-tab context extracted from recent local session logs.
- Run metadata that records where each scan looked.

## Output style

- `Headlines` first, then `Details`.
- `Do First` action near the top.
- One direct runnable command per workstream.
- A `How to do this` sentence with links under each next step.
- Clickable absolute paths in path and open-file sections.
- Table names in collapsible sections.
- Basic footnotes for terms that may be unfamiliar.

## Run (sample limited to 2 directories)

```bash
python /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scripts/generate_current_work.py \
  --workspace-root /Users/eugenetsenter/Looker_clonedRepo/looker_personal \
  --scan-dirs mft,util \
  --workspace-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work.md \
  --global-out /Users/eugenetsenter/.codex/current-work.md \
  --project-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/output/current-work-workspace.md \
  --run-dir /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scrap/retry-original-20260213-194905/rerun-best-v4
```

## Completed retry artifacts (baseline + 3 improvement rounds)

- Root: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scrap/retry-original-20260213-194905`
- Versions: `v1`, `v2`, `v3`, `v4`
- Script snapshots: `versions/generate_current_work.v1.py` through `.v4.py`
- Comparison docs: `compare/01-round-1-improvements.md` through `compare/04-version-decision.md`
- Best rerun output: `rerun-best-v4/`

## Primary outputs

- Workspace-level resume file:
  - `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work.md`
- Global resume file:
  - `/Users/eugenetsenter/.codex/current-work.md`
- PKM capture note (non-interactive):
  - `/Users/eugenetsenter/Library/Mobile Documents/iCloud~md~obsidian/Documents/2026-ob-vault/2026-ob-vault/00 Inbox/2026-02-12 Current Work Snapshot - looker_personal.md`
