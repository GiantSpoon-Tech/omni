#!/usr/bin/env python3
"""Generate resume-first current-work docs with headline -> detail structure."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

DEFAULT_WORKSPACE_ROOT = Path("/Users/eugenetsenter/Looker_clonedRepo/looker_personal")
DEFAULT_CODEX_HOME = Path("/Users/eugenetsenter/.codex")
DEFAULT_GLOBAL_OUT = Path("/Users/eugenetsenter/.codex/current-work.md")
DEFAULT_WORKSPACE_OUT = DEFAULT_WORKSPACE_ROOT / "current-work.md"
DEFAULT_PKM_INBOX = Path(
    "/Users/eugenetsenter/Library/Mobile Documents/"
    "iCloud~md~obsidian/Documents/2026-ob-vault/2026-ob-vault/00 Inbox"
)

SKIP_DIRS = {
    ".git",
    ".history",
    ".vscode",
    ".cursor",
    "node_modules",
    "renv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".Rproj.user",
}

TABLE_TOKEN_RE = re.compile(r"`?([A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)`?")


@dataclass
class RemoteSnapshot:
    name: str
    url: str
    default_branch: str
    remote_head: str
    compare_to_local: str


@dataclass
class RepoSnapshot:
    name: str
    path: Path
    is_git: bool
    branch: str
    local_head: str
    staged: int
    modified: int
    untracked: int
    recent_files: List[Tuple[float, Path]]
    tables: List[str]
    remotes: List[RemoteSnapshot]


@dataclass
class CodexSignals:
    sessions_dir: Path
    shell_snapshots_dir: Path
    latest_session_files: List[Path]
    open_tabs: List[str]
    open_tab_sources: List[str]


@dataclass
class Workstream:
    title: str
    summary: str
    why_it_matters: str
    next_step_label: str
    next_step_cmd: str
    paths: List[str]
    tables: List[str]


@dataclass
class RunLookups:
    workspace_root: Path
    scanned_dirs: List[Path]
    codex_paths: List[Path]
    latest_session_files: List[Path]
    git_remote_checks: List[str]
    open_tab_sources: List[str]


def run_cmd(args: Sequence[str], cwd: Path | None = None) -> Tuple[int, str, str]:
    proc = subprocess.run(
        list(args),
        cwd=str(cwd) if cwd else None,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate resume-first current-work docs")
    parser.add_argument("--workspace-root", default=str(DEFAULT_WORKSPACE_ROOT))
    parser.add_argument("--scan-dirs", default="", help="Comma-separated directory names or absolute paths")
    parser.add_argument("--max-dirs", type=int, default=0)
    parser.add_argument("--codex-home", default=str(DEFAULT_CODEX_HOME))
    parser.add_argument("--workspace-out", default=str(DEFAULT_WORKSPACE_OUT))
    parser.add_argument("--global-out", default=str(DEFAULT_GLOBAL_OUT))
    parser.add_argument("--project-out", default="")
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--pkm-inbox", default=str(DEFAULT_PKM_INBOX))
    return parser.parse_args()


def resolve_scan_dirs(workspace_root: Path, scan_dirs_arg: str, max_dirs: int) -> List[Path]:
    dirs: List[Path] = []

    if scan_dirs_arg.strip():
        tokens = [x.strip() for x in scan_dirs_arg.split(",") if x.strip()]
        for token in tokens:
            p = Path(token)
            if not p.is_absolute():
                p = workspace_root / token
            p = p.resolve()
            if p.exists() and p.is_dir():
                dirs.append(p)
    else:
        for child in sorted(workspace_root.iterdir()):
            if not child.is_dir() or child.name.startswith("."):
                continue
            if child.name in {"current-work-skill", "archive", "~"}:
                continue
            dirs.append(child.resolve())

    if max_dirs > 0:
        dirs = dirs[:max_dirs]

    return dirs


def is_git_repo(path: Path) -> bool:
    rc, out, _ = run_cmd(["git", "-C", str(path), "rev-parse", "--is-inside-work-tree"])
    return rc == 0 and out.strip() == "true"


def parse_git_status(path: Path) -> Tuple[str, str, int, int, int]:
    rc, out, _ = run_cmd(["git", "-C", str(path), "status", "--short", "--branch"])
    if rc != 0:
        return "", "", 0, 0, 0

    lines = [line for line in out.splitlines() if line.strip()]
    branch = ""
    staged = 0
    modified = 0
    untracked = 0

    if lines and lines[0].startswith("##"):
        branch = lines[0].replace("##", "", 1).strip()

    for line in lines[1:]:
        if line.startswith("??"):
            untracked += 1
            continue
        if len(line) >= 2:
            x = line[0]
            y = line[1]
            if x not in (" ", "?"):
                staged += 1
            if y not in (" ", "?"):
                modified += 1

    rc_head, head, _ = run_cmd(["git", "-C", str(path), "rev-parse", "HEAD"])
    local_head = head if rc_head == 0 else ""

    return branch, local_head, staged, modified, untracked


def parse_remote_default_branch(text: str) -> str:
    for line in text.splitlines():
        if line.startswith("ref:") and line.endswith("\tHEAD"):
            ref = line.split()[1]
            return ref.replace("refs/heads/", "")
    return ""


def parse_remote_head(text: str) -> str:
    for line in text.splitlines():
        if line.endswith("\tHEAD"):
            return line.split("\t", 1)[0]
    return ""


def remote_activity(path: Path, local_head: str, lookup_log: List[str]) -> List[RemoteSnapshot]:
    rc, out, _ = run_cmd(["git", "-C", str(path), "remote"])
    if rc != 0:
        return []

    remotes = [line.strip() for line in out.splitlines() if line.strip()]
    snapshots: List[RemoteSnapshot] = []

    for remote in remotes[:3]:
        rc_url, url, _ = run_cmd(["git", "-C", str(path), "remote", "get-url", remote])
        if rc_url != 0:
            continue

        lookup_log.append(f"git -C {shlex.quote(str(path))} ls-remote --symref {remote} HEAD")

        rc_sym, sym_out, _ = run_cmd(["git", "-C", str(path), "ls-remote", "--symref", remote, "HEAD"])
        rc_head, head_out, _ = run_cmd(["git", "-C", str(path), "ls-remote", remote, "HEAD"])

        default_branch = parse_remote_default_branch(sym_out) if rc_sym == 0 else ""
        remote_head = parse_remote_head(head_out) if rc_head == 0 else ""

        if not local_head:
            compare = "local repository has no commits"
        elif not remote_head:
            compare = "remote HEAD not available"
        elif local_head == remote_head:
            compare = "local HEAD matches remote HEAD"
        else:
            compare = "local HEAD differs from remote HEAD"

        snapshots.append(
            RemoteSnapshot(
                name=remote,
                url=url,
                default_branch=default_branch or "unknown",
                remote_head=remote_head or "unknown",
                compare_to_local=compare,
            )
        )

    return snapshots


def collect_recent_files(path: Path, limit: int = 20) -> List[Tuple[float, Path]]:
    rows: List[Tuple[float, Path]] = []

    for root, dirs, files in os.walk(path):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        root_path = Path(root)
        if "scrap" in root_path.parts and "basis_utms" not in root_path.parts:
            continue
        for name in files:
            if name in {"current-work.md", ".DS_Store"}:
                continue
            full = root_path / name
            try:
                mtime = full.stat().st_mtime
            except OSError:
                continue
            rows.append((mtime, full))

    rows.sort(key=lambda x: x[0], reverse=True)
    return rows[:limit]


def extract_tables(paths: Iterable[Path], limit: int = 20) -> List[str]:
    seen = set()
    out: List[str] = []

    for path in paths:
        if path.suffix.lower() != ".sql":
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        for match in TABLE_TOKEN_RE.finditer(text):
            token = match.group(1)
            if token not in seen:
                seen.add(token)
                out.append(token)
                if len(out) >= limit:
                    return out
    return out


def parse_open_tabs_from_user_text(text: str) -> List[str]:
    tabs: List[str] = []
    lines = text.splitlines()
    in_block = False

    for line in lines:
        raw = line.rstrip()
        if raw.strip().lower() == "## open tabs:":
            in_block = True
            continue
        if in_block:
            if not raw.strip().startswith("-"):
                if raw.strip() == "":
                    continue
                break
            item = raw.strip()[1:].strip()
            if ":" in item:
                candidate = item.split(":", 1)[1].strip()
            else:
                candidate = item
            if candidate.startswith("/"):
                tabs.append(candidate)

    return tabs


def load_codex_signals(codex_home: Path) -> CodexSignals:
    sessions_dir = codex_home / "sessions"
    shell_dir = codex_home / "shell_snapshots"

    latest_jsonl = sorted(sessions_dir.rglob("*.jsonl"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)[:8]

    tabs: List[str] = []
    tab_sources: List[str] = []

    for jsonl in latest_jsonl:
        try:
            with jsonl.open("r", encoding="utf-8", errors="ignore") as handle:
                for raw in handle:
                    try:
                        obj = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if obj.get("type") != "response_item":
                        continue
                    payload = obj.get("payload", {})
                    if not isinstance(payload, dict):
                        continue
                    if payload.get("type") != "message" or payload.get("role") != "user":
                        continue
                    content = payload.get("content", [])
                    if not isinstance(content, list):
                        continue
                    for item in content:
                        if not isinstance(item, dict):
                            continue
                        txt = item.get("text") or item.get("input_text")
                        if isinstance(txt, str) and "## Open tabs:" in txt:
                            found = parse_open_tabs_from_user_text(txt)
                            if found:
                                tabs.extend(found)
                                tab_sources.append(str(jsonl))
        except OSError:
            continue

    dedup_tabs: List[str] = []
    seen_tabs = set()
    for tab in tabs:
        if tab not in seen_tabs:
            dedup_tabs.append(tab)
            seen_tabs.add(tab)

    dedup_sources: List[str] = []
    seen_sources = set()
    for src in tab_sources:
        if src not in seen_sources:
            dedup_sources.append(src)
            seen_sources.add(src)

    return CodexSignals(
        sessions_dir=sessions_dir,
        shell_snapshots_dir=shell_dir,
        latest_session_files=latest_jsonl,
        open_tabs=dedup_tabs,
        open_tab_sources=dedup_sources,
    )


def ts_local(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def dedup_keep_order(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def path_matches(path: str, keywords: Sequence[str]) -> bool:
    low = path.lower()
    return any(k in low for k in keywords)


def table_matches(table: str, keywords: Sequence[str]) -> bool:
    low = table.lower()
    return any(k in low for k in keywords)


def build_workstreams(repo_snapshots: List[RepoSnapshot], open_tabs: List[str]) -> List[Workstream]:
    # Define candidate streams with plain-language summaries.
    defs = [
        {
            "id": "dcm_utm",
            "title": "DCM + UTM enrichment hardening",
            "summary": "You were tightening how DCM rows get UTM fields, with a constrained fallback for specific campaigns.",
            "why": "This prevents null UTM fields and keeps reporting rows attributed correctly.",
            "next_label": "Run a null-UTM health check for scoped campaigns",
            "next_cmd": (
                "cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft\n"
                "./scripts/bq-safe-query.sh --max-rows 25 --sql \"SELECT campaign, COUNT(*) AS rows, "
                "COUNTIF(utm_content IS NULL) AS null_utm_content FROM `looker-studio-pro-452620.repo_stg.dcm_plus_utms` "
                "WHERE campaign IN ('MassMutual20252026Media','MassMutualLVGP2025') AND date >= '2026-01-01' "
                "GROUP BY 1 ORDER BY rows DESC\""
            ),
            "keywords": ["dcm", "utm", "repo_stg__dcm_plus_utms", "utms_view"],
        },
        {
            "id": "offline",
            "title": "Offline sheet daily sync pipeline",
            "summary": "You were maintaining the sheet-to-BigQuery sync used for `mft_offline` updates.",
            "why": "If this schedule fails, offline metrics drift from the source sheet and dashboards go stale.",
            "next_label": "Verify scheduled-query status and latest rows",
            "next_cmd": (
                "bq ls --transfer_config --transfer_location=US --project_id=looker-studio-pro-452620\n"
                "bq show --transfer_config --transfer_location=US "
                "projects/671028410185/locations/us/transferConfigs/699421ab-0000-2129-a27e-883d24f0f1b8\n"
                "bq head -n 5 looker-studio-pro-452620:mass_mutual_mft_ext.mft_offline"
            ),
            "keywords": ["mft_offline", "offline", "sheet_sync", "setup-mft-offline"],
        },
        {
            "id": "basis",
            "title": "Basis UTM merge and FY26 Q1 backfill",
            "summary": "You were working on Basis UTM union/backfill logic and a patched SQL flow in `/tmp`.",
            "why": "This controls whether Basis rows join cleanly into the shared UTM layer without gaps or duplicates.",
            "next_label": "Validate patched SQL once, then decide whether to move it into repo",
            "next_cmd": "bq query --use_legacy_sql=false < /tmp/utm_updates_patched.sql",
            "keywords": ["basis", "utm_updates_patched", "fy26_q1", "pivoted", "unioned"],
        },
    ]

    all_paths: List[Tuple[float, str]] = []
    all_tables: List[str] = []

    for repo in repo_snapshots:
        for ts, p in repo.recent_files:
            all_paths.append((ts, str(p)))
        all_tables.extend(repo.tables)

    for tab in open_tabs:
        all_paths.append((0.0, tab))

    all_tables = dedup_keep_order(all_tables)

    streams: List[Tuple[int, Workstream]] = []

    for d in defs:
        matched_paths: List[str] = []
        score = 0

        for ts, p in all_paths:
            if path_matches(p, d["keywords"]):
                if ts > 0:
                    matched_paths.append(f"{ts_local(ts)} - {p}")
                    score += 2
                else:
                    matched_paths.append(p)
                    score += 3

        matched_tables = [t for t in all_tables if table_matches(t, d["keywords"])]
        score += len(matched_tables)

        matched_paths = dedup_keep_order(matched_paths)
        matched_tables = dedup_keep_order(matched_tables)

        if matched_paths or matched_tables:
            ws = Workstream(
                title=d["title"],
                summary=d["summary"],
                why_it_matters=d["why"],
                next_step_label=d["next_label"],
                next_step_cmd=d["next_cmd"],
                paths=matched_paths[:15],
                tables=matched_tables[:15],
            )
            streams.append((score, ws))

    streams.sort(key=lambda x: x[0], reverse=True)

    selected = [ws for _, ws in streams[:3]]

    if not selected:
        selected = [
            Workstream(
                title="General workspace maintenance",
                summary="Recent file and git activity was detected, but no strong thematic cluster was found.",
                why_it_matters="A quick checkpoint commit can prevent context loss while you reorient.",
                next_step_label="Create checkpoint commit",
                next_step_cmd=(
                    "cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal\n"
                    "git status --short\n"
                    "git add .\n"
                    "git commit -m \"Checkpoint: current-work snapshot\""
                ),
                paths=dedup_keep_order([p for _, p in all_paths])[:10],
                tables=all_tables[:10],
            )
        ]

    return selected


def render_list(items: List[str], empty_text: str) -> str:
    if not items:
        return f"- {empty_text}\n"
    return "".join(f"- `{item}`\n" for item in items)


def render_headlines(workstreams: List[Workstream]) -> str:
    lines = ["## Headlines\n\n"]
    for i, ws in enumerate(workstreams, start=1):
        lines.append(f"{i}. **{ws.title}** - {ws.summary} Next: {ws.next_step_label}.\n")
    lines.append("\n")
    return "".join(lines)


def render_details(workstreams: List[Workstream]) -> str:
    lines = ["## Details\n\n"]
    for i, ws in enumerate(workstreams, start=1):
        lines.append(f"### {i}) {ws.title}\n")
        lines.append(f"What this means: {ws.summary}\n\n")
        lines.append(f"Why it matters: {ws.why_it_matters}\n\n")
        lines.append(f"Next step now: {ws.next_step_label}\n\n")
        lines.append("```bash\n")
        lines.append(ws.next_step_cmd + "\n")
        lines.append("```\n\n")

        lines.append(f"<details><summary>Paths - {ws.title}</summary>\n\n")
        lines.append(render_list(ws.paths, "None detected"))
        lines.append("\n</details>\n\n")

        lines.append(f"<details><summary>Tables - {ws.title}</summary>\n\n")
        lines.append(render_list(ws.tables, "None detected"))
        lines.append("\n</details>\n\n")

    return "".join(lines)


def render_repo_status(repo_snapshots: List[RepoSnapshot]) -> str:
    lines = ["## Repo Status\n\n"]
    for repo in repo_snapshots:
        lines.append(f"### `{repo.name}`\n")
        if not repo.is_git:
            lines.append("- Not a git repository.\n\n")
            continue
        lines.append(f"- Branch[^3]: `{repo.branch or 'unknown'}`\n")
        lines.append(f"- Working tree[^1]: staged={repo.staged}, modified={repo.modified}, untracked={repo.untracked}\n")
        lines.append("- Remote activity[^2]:\n")
        if repo.remotes:
            for rm in repo.remotes:
                lines.append(
                    f"  - `{rm.name}` ({rm.default_branch}) => {rm.compare_to_local}; url=`{rm.url}`\n"
                )
        else:
            lines.append("  - none detected\n")
        lines.append("\n")
    return "".join(lines)


def render_open_tabs(open_tabs: List[str]) -> str:
    lines = ["## Open Files Seen From VS Code Context[^4]\n\n"]
    if not open_tabs:
        lines.append("- No open-file list found in recent local session logs.\n\n")
        return "".join(lines)
    for tab in open_tabs[:25]:
        lines.append(f"- `{tab}`\n")
    lines.append("\n")
    return "".join(lines)


def render_risks(repo_snapshots: List[RepoSnapshot], open_tabs: List[str]) -> str:
    risks: List[str] = []

    for repo in repo_snapshots:
        if repo.is_git and not repo.local_head:
            risks.append(f"`{repo.name}` has no local commit history.")
        if repo.untracked >= 10:
            risks.append(f"`{repo.name}` has a high untracked count ({repo.untracked}).")

    if any(tab.startswith("/tmp/") for tab in open_tabs):
        risks.append("At least one active file is under `/tmp`, which is temporary storage.")

    if not risks:
        risks.append("No high-risk signal found in this scan.")

    lines = ["## Current Risks\n\n"]
    for risk in dedup_keep_order(risks):
        lines.append(f"- {risk}\n")
    lines.append("\n")
    return "".join(lines)


def render_where_looked(lookups: RunLookups) -> str:
    lines = ["## Where I Looked This Run\n\n"]
    lines.append(f"- Workspace root: `{lookups.workspace_root}`\n")

    lines.append("- Scanned directories:\n")
    for path in lookups.scanned_dirs:
        lines.append(f"  - `{path}`\n")

    lines.append("- Codex data paths:\n")
    for path in lookups.codex_paths:
        lines.append(f"  - `{path}`\n")

    lines.append("- Latest session files inspected:\n")
    for p in lookups.latest_session_files[:8]:
        lines.append(f"  - `{p}`\n")

    lines.append("- Remote git checks executed:\n")
    if lookups.git_remote_checks:
        for cmd in lookups.git_remote_checks:
            lines.append(f"  - `{cmd}`\n")
    else:
        lines.append("  - `none`\n")

    lines.append("- Open-tab sources:\n")
    if lookups.open_tab_sources:
        for src in lookups.open_tab_sources:
            lines.append(f"  - `{src}`\n")
    else:
        lines.append("  - `No open-tab block found in inspected session files`\n")

    lines.append("\n")
    return "".join(lines)


def render_footnotes() -> str:
    return (
        "## Footnotes\n\n"
        "[^1]: Working tree means your local file state before commit.\n"
        "[^2]: Remote means the server-side git copy (for example GitHub).\n"
        "[^3]: Branch means a named commit line in git.\n"
        "[^4]: VS Code context here comes from local session log messages that include open tabs.\n"
        "[^5]: PKM means personal knowledge management notes.\n"
    )


def build_workspace_doc(
    now_text: str,
    workspace_root: Path,
    scanned_dirs: List[Path],
    workstreams: List[Workstream],
    repo_snapshots: List[RepoSnapshot],
    codex_signals: CodexSignals,
    lookups: RunLookups,
) -> str:
    parts: List[str] = []
    parts.append("# Current Work - Workspace (Resume Fast)\n\n")
    parts.append(f"Updated: `{now_text}`\n")
    parts.append(f"Workspace root: `{workspace_root}`\n")
    parts.append(f"Directories scanned this run: `{len(scanned_dirs)}`\n\n")

    parts.append(render_headlines(workstreams))
    parts.append(render_details(workstreams))
    parts.append(render_repo_status(repo_snapshots))
    parts.append(render_open_tabs(codex_signals.open_tabs))
    parts.append(render_risks(repo_snapshots, codex_signals.open_tabs))
    parts.append(render_where_looked(lookups))
    parts.append(render_footnotes())
    return "".join(parts)


def build_global_doc(
    now_text: str,
    workspace_root: Path,
    workstreams: List[Workstream],
    lookups: RunLookups,
) -> str:
    parts: List[str] = []
    parts.append("# Current Work - Global (Resume Fast)\n\n")
    parts.append(f"Updated: `{now_text}`\n")
    parts.append(f"Primary workspace: `{workspace_root}`\n\n")

    parts.append(render_headlines(workstreams))
    parts.append(render_details(workstreams))
    parts.append(render_where_looked(lookups))
    parts.append(render_footnotes())
    return "".join(parts)


def build_pkm_note(
    now_text: str,
    workspace_root: Path,
    workstreams: List[Workstream],
    workspace_out: Path,
    global_out: Path,
    lookups: RunLookups,
) -> str:
    date_str = datetime.now().astimezone().strftime("%Y-%m-%d")
    lines: List[str] = []
    lines.append("---\n")
    lines.append(f"title: Current Work Snapshot - {workspace_root.name}\n")
    lines.append(f"date: {date_str}\n")
    lines.append("tags: [codex, handoff, current-work, non-interactive]\n")
    lines.append("status: captured\n")
    lines.append("---\n\n")

    lines.append("# Numbered Summary\n\n")
    lines.append(f"1. Captured workspace snapshot at `{now_text}` in non-interactive mode.\n")
    lines.append(f"2. Workspace root: `{workspace_root}`.\n")
    for i, ws in enumerate(workstreams, start=3):
        lines.append(f"{i}. {ws.title}: {ws.summary}\n")

    lines.append("\n# Captured Files\n\n")
    lines.append(f"- Workspace doc: `{workspace_out}`\n")
    lines.append(f"- Global doc: `{global_out}`\n")

    lines.append("\n# Where Looked\n\n")
    for p in lookups.scanned_dirs:
        lines.append(f"- `{p}`\n")
    for p in lookups.codex_paths:
        lines.append(f"- `{p}`\n")

    lines.append("\n# What To Do Next\n\n")
    for ws in workstreams:
        lines.append(f"- {ws.next_step_label}\n")

    return "".join(lines)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> int:
    args = parse_args()

    workspace_root = Path(args.workspace_root).expanduser().resolve()
    codex_home = Path(args.codex_home).expanduser().resolve()
    workspace_out = Path(args.workspace_out).expanduser().resolve()
    global_out = Path(args.global_out).expanduser().resolve()
    project_out = Path(args.project_out).expanduser().resolve() if args.project_out else None
    run_dir = Path(args.run_dir).expanduser().resolve() if args.run_dir else None
    pkm_inbox = Path(args.pkm_inbox).expanduser().resolve()

    scan_dirs = resolve_scan_dirs(workspace_root, args.scan_dirs, args.max_dirs)

    remote_checks: List[str] = []
    repo_snapshots: List[RepoSnapshot] = []

    for directory in scan_dirs:
        git_repo = is_git_repo(directory)

        branch = ""
        local_head = ""
        staged = modified = untracked = 0
        remotes: List[RemoteSnapshot] = []

        if git_repo:
            branch, local_head, staged, modified, untracked = parse_git_status(directory)
            remotes = remote_activity(directory, local_head, remote_checks)

        recent_files = collect_recent_files(directory)
        tables = extract_tables([p for _, p in recent_files])

        repo_snapshots.append(
            RepoSnapshot(
                name=directory.name,
                path=directory,
                is_git=git_repo,
                branch=branch,
                local_head=local_head,
                staged=staged,
                modified=modified,
                untracked=untracked,
                recent_files=recent_files,
                tables=tables,
                remotes=remotes,
            )
        )

    codex_signals = load_codex_signals(codex_home)
    workstreams = build_workstreams(repo_snapshots, codex_signals.open_tabs)

    lookups = RunLookups(
        workspace_root=workspace_root,
        scanned_dirs=scan_dirs,
        codex_paths=[codex_signals.sessions_dir, codex_signals.shell_snapshots_dir],
        latest_session_files=codex_signals.latest_session_files,
        git_remote_checks=remote_checks,
        open_tab_sources=codex_signals.open_tab_sources,
    )

    now_text = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    workspace_doc = build_workspace_doc(
        now_text=now_text,
        workspace_root=workspace_root,
        scanned_dirs=scan_dirs,
        workstreams=workstreams,
        repo_snapshots=repo_snapshots,
        codex_signals=codex_signals,
        lookups=lookups,
    )

    global_doc = build_global_doc(
        now_text=now_text,
        workspace_root=workspace_root,
        workstreams=workstreams,
        lookups=lookups,
    )

    pkm_note_path = pkm_inbox / f"{datetime.now().astimezone().strftime('%Y-%m-%d')} Current Work Snapshot - {workspace_root.name}.md"
    pkm_doc = build_pkm_note(now_text, workspace_root, workstreams, workspace_out, global_out, lookups)

    write_text(workspace_out, workspace_doc)
    write_text(global_out, global_doc)
    write_text(pkm_note_path, pkm_doc)

    if project_out:
        write_text(project_out, workspace_doc)

    if run_dir:
        run_dir.mkdir(parents=True, exist_ok=True)
        write_text(run_dir / "workspace-current-work.md", workspace_doc)
        write_text(run_dir / "global-current-work.md", global_doc)
        write_text(run_dir / "pkm-note.md", pkm_doc)
        metadata = {
            "generated_at": now_text,
            "workspace_root": str(workspace_root),
            "scan_dirs": [str(p) for p in scan_dirs],
            "workspace_out": str(workspace_out),
            "global_out": str(global_out),
            "project_out": str(project_out) if project_out else "",
            "pkm_note": str(pkm_note_path),
            "workstreams": [
                {
                    "title": ws.title,
                    "summary": ws.summary,
                    "next_step_label": ws.next_step_label,
                    "paths": ws.paths,
                    "tables": ws.tables,
                }
                for ws in workstreams
            ],
            "open_tabs_detected": codex_signals.open_tabs,
            "session_files_used": [str(p) for p in codex_signals.latest_session_files],
            "remote_checks": remote_checks,
            "open_tab_sources": codex_signals.open_tab_sources,
        }
        write_text(run_dir / "metadata.json", json.dumps(metadata, indent=2) + "\n")

    print(f"Wrote workspace doc: {workspace_out}")
    print(f"Wrote global doc: {global_out}")
    print(f"Wrote PKM note: {pkm_note_path}")
    if project_out:
        print(f"Wrote project copy: {project_out}")
    if run_dir:
        print(f"Saved run artifacts: {run_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
