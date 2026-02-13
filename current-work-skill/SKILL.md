---
name: current-work-skill
description: Generate resume-first current-work markdown snapshots for a workspace by scanning selected subdirectories, global Codex logs, remote git activity, and VS Code open-tab context from local session logs. Use when you need to quickly know what you were doing, what to do next, and have actionable next-step guidance with clickable paths.
---

# Current Work Skill

## Run

```bash
python /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scripts/generate_current_work.py \
  --workspace-root /Users/eugenetsenter/Looker_clonedRepo/looker_personal \
  --workspace-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work.md \
  --global-out /Users/eugenetsenter/.codex/current-work.md \
  --project-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/output/current-work-workspace.md
```

## Sample (2 directories)

```bash
python /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/scripts/generate_current_work.py \
  --workspace-root /Users/eugenetsenter/Looker_clonedRepo/looker_personal \
  --scan-dirs mft,util \
  --workspace-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work.md \
  --global-out /Users/eugenetsenter/.codex/current-work.md \
  --project-out /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/output/current-work-workspace.md \
  --run-dir /Users/eugenetsenter/Looker_clonedRepo/looker_personal/current-work-skill/runs/sample-2dirs
```

## Output guarantees

- `What You Were Working On`
- `What You Should Do Next (In Order)`
- `How to do this` guidance under each next step
- clickable local path links in path/open-file sections
- scan-location details saved in run metadata
- concise footnotes for jargon
