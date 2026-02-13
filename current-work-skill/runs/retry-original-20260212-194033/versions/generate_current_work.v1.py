#!/usr/bin/env python3
"""Generate quick resume-oriented current-work docs for workspace + global scope."""

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
    status_lines: List[str]
    recent_files: List[str]
    tables: List[str]
    remotes: List[RemoteSnapshot]


@dataclass
class CodexSignals:
    sessions_dir: Path
    shell_snapshots_dir: Path
    latest_session_files: List[Path]
    latest_shell_snapshot: Path | None
    open_tabs: List[str]


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
    parser.add_argument("--max-dirs", type=int, default=0, help="Limit directories scanned when --scan-dirs is empty")
    parser.add_argument("--codex-home", default=str(DEFAULT_CODEX_HOME))
    parser.add_argument("--global-out", default=str(DEFAULT_GLOBAL_OUT))
    parser.add_argument("--workspace-out", default=str(DEFAULT_WORKSPACE_OUT))
    parser.add_argument("--project-out", default="", help="Optional output copy inside this project")
    parser.add_argument("--run-dir", default="", help="Optional run artifact folder")
    return parser.parse_args()


def resolve_scan_dirs(workspace_root: Path, scan_dirs_arg: str, max_dirs: int) -> List[Path]:
    dirs: List[Path] = []

    if scan_dirs_arg.strip():
        for token in [p.strip() for p in scan_dirs_arg.split(",") if p.strip()]:
            path = Path(token)
            if not path.is_absolute():
                path = workspace_root / token
            path = path.resolve()
            if path.exists() and path.is_dir():
                dirs.append(path)
    else:
        for child in sorted(workspace_root.iterdir()):
            if not child.is_dir():
                continue
            if child.name.startswith("."):
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


def parse_git_status(path: Path) -> Tuple[str, int, int, int, List[str], str]:
    rc, out, err = run_cmd(["git", "-C", str(path), "status", "--short", "--branch"])
    if rc != 0:
        return ("not-a-repo", 0, 0, 0, [err or "git status failed"], "")

    lines = [line for line in out.splitlines() if line.strip()]
    branch = "unknown"
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

    rc2, head, _ = run_cmd(["git", "-C", str(path), "rev-parse", "HEAD"])
    local_head = head if rc2 == 0 else ""

    return (branch, staged, modified, untracked, lines, local_head)


def parse_remote_default_branch(ls_remote_symref_output: str) -> str:
    for line in ls_remote_symref_output.splitlines():
        if line.startswith("ref:") and line.endswith("\tHEAD"):
            # format: ref: refs/heads/main	HEAD
            ref = line.split()[1]
            return ref.replace("refs/heads/", "")
    return ""


def parse_remote_head(ls_remote_output: str) -> str:
    for line in ls_remote_output.splitlines():
        if line.endswith("\tHEAD"):
            return line.split("\t", 1)[0]
    return ""


def remote_activity(path: Path, local_head: str, lookup_log: List[str]) -> List[RemoteSnapshot]:
    rc, out, _ = run_cmd(["git", "-C", str(path), "remote"])
    if rc != 0:
        return []

    remotes = [line.strip() for line in out.splitlines() if line.strip()]
    results: List[RemoteSnapshot] = []

    for remote in remotes[:3]:
        rc_url, url, _ = run_cmd(["git", "-C", str(path), "remote", "get-url", remote])
        if rc_url != 0:
            continue

        rc_sym, sym_out, _ = run_cmd(["git", "-C", str(path), "ls-remote", "--symref", remote, "HEAD"])
        rc_head, head_out, _ = run_cmd(["git", "-C", str(path), "ls-remote", remote, "HEAD"])

        default_branch = parse_remote_default_branch(sym_out) if rc_sym == 0 else ""
        remote_head = parse_remote_head(head_out) if rc_head == 0 else ""

        if not local_head:
            compare = "local repository has no commits"
        elif not remote_head:
            compare = "remote HEAD not available"
        elif remote_head == local_head:
            compare = "local HEAD matches remote HEAD"
        else:
            compare = "local HEAD differs from remote HEAD"

        lookup_log.append(f"git -C {shlex.quote(str(path))} ls-remote --symref {remote} HEAD")

        results.append(
            RemoteSnapshot(
                name=remote,
                url=url,
                default_branch=default_branch or "unknown",
                remote_head=remote_head or "unknown",
                compare_to_local=compare,
            )
        )

    return results


def collect_recent_files(path: Path, max_files: int = 12) -> List[Path]:
    rows: List[Tuple[float, Path]] = []

    for root, dirs, files in os.walk(path):
        root_path = Path(root)
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]

        # Skip internal noise and large generated folders.
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
    return [p for _, p in rows[:max_files]]


def extract_tables(paths: Iterable[Path], max_tables: int = 15) -> List[str]:
    seen = set()
    tables: List[str] = []

    for path in paths:
        if path.suffix.lower() != ".sql":
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        for match in TABLE_TOKEN_RE.finditer(text):
            token = match.group(1).strip()
            parts = token.split(".")
            if len(parts) != 3:
                continue
            if token not in seen:
                seen.add(token)
                tables.append(token)
                if len(tables) >= max_tables:
                    return tables

    return tables


def parse_open_tabs_from_user_text(text: str) -> List[str]:
    tabs: List[str] = []
    lines = text.splitlines()

    in_tabs = False
    for line in lines:
        raw = line.rstrip()
        if raw.strip().lower() == "## open tabs:":
            in_tabs = True
            continue
        if in_tabs:
            if not raw.strip().startswith("-"):
                if raw.strip() == "":
                    continue
                break
            # Expected pattern: - label: /path/to/file
            body = raw.strip()[1:].strip()
            if ":" in body:
                path_part = body.split(":", 1)[1].strip()
            else:
                path_part = body.strip()
            if path_part.startswith("/"):
                tabs.append(path_part)

    return tabs


def load_codex_signals(codex_home: Path) -> Tuple[CodexSignals, List[str], List[str], List[Path]]:
    sessions_dir = codex_home / "sessions"
    shell_dir = codex_home / "shell_snapshots"

    latest_jsonl = sorted(sessions_dir.rglob("*.jsonl"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)[:8]

    open_tabs: List[str] = []
    open_tab_sources: List[str] = []

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
                                open_tabs.extend(found)
                                open_tab_sources.append(str(jsonl))
        except OSError:
            continue

    dedup_tabs: List[str] = []
    seen_tabs = set()
    for tab in open_tabs:
        if tab not in seen_tabs:
            dedup_tabs.append(tab)
            seen_tabs.add(tab)

    dedup_sources: List[str] = []
    seen_sources = set()
    for src in open_tab_sources:
        if src not in seen_sources:
            dedup_sources.append(src)
            seen_sources.add(src)

    latest_shell = None
    shell_files = sorted(shell_dir.glob("*.sh"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    if shell_files:
        latest_shell = shell_files[0]

    signals = CodexSignals(
        sessions_dir=sessions_dir,
        shell_snapshots_dir=shell_dir,
        latest_session_files=latest_jsonl,
        latest_shell_snapshot=latest_shell,
        open_tabs=dedup_tabs,
    )

    codex_paths = [sessions_dir, shell_dir]
    return signals, dedup_sources, [str(p) for p in latest_jsonl], codex_paths


def summarize_focus(repo_snapshots: List[RepoSnapshot], open_tabs: List[str]) -> List[Tuple[str, List[str], List[str]]]:
    open_set = set(open_tabs)
    focus: List[Tuple[str, List[str], List[str]]] = []

    # Signal 1: DCM + UTM hardening
    dcm_paths = []
    dcm_tables = []
    for repo in repo_snapshots:
        for rf in repo.recent_files:
            if "repo_stg__dcm_plus_utms.sql" in rf:
                dcm_paths.append(rf)
        for t in repo.tables:
            if "dcm_plus_utms" in t or t.endswith("final_views.dcm") or t.endswith("final_views.utms_view"):
                dcm_tables.append(t)
    if dcm_paths or dcm_tables:
        focus.append((
            "DCM + UTM join hardening",
            sorted(set(dcm_paths)),
            sorted(set(dcm_tables)),
        ))

    # Signal 2: Offline sync pipeline
    off_paths = []
    off_tables = []
    for repo in repo_snapshots:
        for rf in repo.recent_files:
            if "mft_offline" in rf or "setup-mft-offline" in rf:
                off_paths.append(rf)
        for t in repo.tables:
            if "mft_offline" in t or "stg__mm__mft_offline_connected_gsheet" in t:
                off_tables.append(t)
    if off_paths or off_tables:
        focus.append((
            "Offline sheet sync pipeline",
            sorted(set(off_paths)),
            sorted(set(off_tables)),
        ))

    # Signal 3: Basis UTM work from open tabs and recent files
    basis_paths = []
    basis_tables = []
    for tab in open_set:
        if "basis_utms" in tab or "utm_updates_patched.sql" in tab:
            basis_paths.append(tab)
    for repo in repo_snapshots:
        for rf in repo.recent_files:
            if "basis_utms" in rf:
                basis_paths.append(rf)
        for t in repo.tables:
            if "utm" in t.lower() or "basis" in t.lower():
                basis_tables.append(t)
    if basis_paths or basis_tables:
        focus.append((
            "Basis UTM merge/backfill work",
            sorted(set(basis_paths)),
            sorted(set(basis_tables)),
        ))

    if not focus:
        generic_paths = []
        generic_tables = []
        for repo in repo_snapshots:
            generic_paths.extend(repo.recent_files[:4])
            generic_tables.extend(repo.tables[:4])
        focus.append(("General repository maintenance", sorted(set(generic_paths)), sorted(set(generic_tables))))

    return focus


def make_next_steps(repo_snapshots: List[RepoSnapshot], workspace_root: Path) -> List[Tuple[str, str]]:
    steps: List[Tuple[str, str]] = []

    repo_names = {r.name for r in repo_snapshots}
    if "mft" in repo_names:
        steps.append((
            "Verify DCM enrichment for scoped campaigns",
            "cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft\n"
            "./scripts/bq-safe-query.sh --max-rows 25 --sql \"SELECT campaign, COUNT(*) AS rows, COUNTIF(utm_content IS NULL) AS null_utm_content FROM `looker-studio-pro-452620.repo_stg.dcm_plus_utms` WHERE campaign IN ('MassMutual20252026Media','MassMutualLVGP2025') AND date >= '2026-01-01' GROUP BY 1 ORDER BY rows DESC\"",
        ))
        steps.append((
            "Check offline scheduled query health",
            "bq ls --transfer_config --transfer_location=US --project_id=looker-studio-pro-452620\n"
            "bq show --transfer_config --transfer_location=US projects/671028410185/locations/us/transferConfigs/699421ab-0000-2129-a27e-883d24f0f1b8\n"
            "bq head -n 5 looker-studio-pro-452620:mass_mutual_mft_ext.mft_offline",
        ))

    if "util" in repo_names:
        steps.append((
            "Decide if temporary UTM patch should move into repo",
            "bq query --use_legacy_sql=false < /tmp/utm_updates_patched.sql",
        ))

    steps.append((
        "Create a checkpoint commit in the workspace repo",
        f"cd {workspace_root}\n"
        "git status --short\n"
        "git add .\n"
        "git commit -m \"Checkpoint: current-work snapshot\"",
    ))

    return steps[:5]


def build_where_looked(runlookups: RunLookups) -> str:
    lines = ["## Where I Looked This Run\n\n"]
    lines.append(f"- Workspace root: `{runlookups.workspace_root}`\n")

    lines.append("- Scanned directories:\n")
    for path in runlookups.scanned_dirs:
        lines.append(f"  - `{path}`\n")

    lines.append("- Codex data paths:\n")
    for path in runlookups.codex_paths:
        lines.append(f"  - `{path}`\n")

    lines.append("- Latest session files inspected:\n")
    for path in runlookups.latest_session_files[:6]:
        lines.append(f"  - `{path}`\n")

    lines.append("- Remote git checks executed:\n")
    if runlookups.git_remote_checks:
        for cmd in runlookups.git_remote_checks:
            lines.append(f"  - `{cmd}`\n")
    else:
        lines.append("  - `none`\n")

    lines.append("- Open-tab sources:\n")
    if runlookups.open_tab_sources:
        for src in runlookups.open_tab_sources:
            lines.append(f"  - `{src}`\n")
    else:
        lines.append("  - `No open-tab block found in recent local session logs`\n")

    lines.append("\n")
    return "".join(lines)


def render_focus(focus: List[Tuple[str, List[str], List[str]]]) -> str:
    lines = ["## What You Were Working On\n\n"]
    for idx, (title, paths, tables) in enumerate(focus, start=1):
        lines.append(f"### {idx}) {title}\n")
        lines.append("Based on local files and logs, this appears to be an active workstream right now.\n\n")

        lines.append(f"<details><summary>Paths - {title}</summary>\n\n")
        if paths:
            for p in paths[:15]:
                lines.append(f"- `{p}`\n")
        else:
            lines.append("- None detected\n")
        lines.append("\n</details>\n\n")

        lines.append(f"<details><summary>Tables - {title}</summary>\n\n")
        if tables:
            for t in tables[:15]:
                lines.append(f"- `{t}`\n")
        else:
            lines.append("- None detected\n")
        lines.append("\n</details>\n\n")
    return "".join(lines)


def render_next_steps(steps: List[Tuple[str, str]]) -> str:
    lines = ["## What You Should Do Next (In Order)\n\n"]
    for idx, (label, cmd) in enumerate(steps, start=1):
        lines.append(f"{idx}. {label}\n\n")
        lines.append("```bash\n")
        lines.append(cmd + "\n")
        lines.append("```\n\n")
    return "".join(lines)


def render_repo_status(repo_snapshots: List[RepoSnapshot]) -> str:
    lines = ["## Repo Status Snapshot\n\n"]
    for repo in repo_snapshots:
        lines.append(f"### `{repo.name}`\n")
        if not repo.is_git:
            lines.append("- Not a git repository.\n\n")
            continue

        lines.append(f"- Branch: `{repo.branch or 'unknown'}`\n")
        lines.append(f"- Working tree[^1]: staged={repo.staged}, modified={repo.modified}, untracked={repo.untracked}\n")

        lines.append("- Remote activity[^2]:\n")
        if repo.remotes:
            for remote in repo.remotes:
                lines.append(
                    f"  - `{remote.name}` ({remote.default_branch}) => {remote.compare_to_local}; url=`{remote.url}`\n"
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

    for tab in open_tabs[:30]:
        lines.append(f"- `{tab}`\n")
    lines.append("\n")
    return "".join(lines)


def render_risks(repo_snapshots: List[RepoSnapshot], open_tabs: List[str]) -> str:
    lines = ["## Current Risks\n\n"]
    risks: List[str] = []

    for repo in repo_snapshots:
        if repo.is_git and not repo.local_head:
            risks.append(f"`{repo.name}` has no local commit history yet.")
        if repo.untracked >= 8:
            risks.append(f"`{repo.name}` has high untracked count ({repo.untracked}).")

    if any(path.startswith("/tmp/") for path in open_tabs):
        risks.append("One or more active files are under `/tmp`, which is temporary storage.")

    if len({p.split("/")[1] if p.startswith("/") else p for p in open_tabs}) > 1:
        risks.append("Active files are spread across multiple directories, so context can drift.")

    if not risks:
        risks.append("No high-risk signals detected from local scan.")

    for item in risks:
        lines.append(f"- {item}\n")
    lines.append("\n")
    return "".join(lines)


def render_footnotes() -> str:
    return (
        "## Footnotes\n\n"
        "[^1]: Working tree means your current local file state before commit.\n"
        "[^2]: Remote means the git server copy of a repository (for example GitHub).[1]\n"
        "[^3]: Branch means a named line of commits in git.[2]\n"
        "[^4]: VS Code context here comes from local Codex session logs that include open-tab lists.\n\n"
        "[1] Remote: the network location that stores shared git history.\n"
        "[2] Branch: an isolated commit path used to organize changes.\n"
    )


def build_workspace_doc(
    now_text: str,
    workspace_root: Path,
    repo_snapshots: List[RepoSnapshot],
    focus: List[Tuple[str, List[str], List[str]]],
    next_steps: List[Tuple[str, str]],
    codex: CodexSignals,
    runlookups: RunLookups,
) -> str:
    lines: List[str] = []
    lines.append("# Current Work - Workspace (Resume Fast)\n\n")
    lines.append(f"Updated: `{now_text}`\n")
    lines.append(f"Workspace root: `{workspace_root}`\n")
    lines.append(f"Directories scanned this run: `{len(repo_snapshots)}`\n\n")

    lines.append(render_focus(focus))
    lines.append(render_next_steps(next_steps))
    lines.append(render_repo_status(repo_snapshots))
    lines.append(render_open_tabs(codex.open_tabs))
    lines.append(render_risks(repo_snapshots, codex.open_tabs))
    lines.append(build_where_looked(runlookups))
    lines.append(render_footnotes())

    return "".join(lines)


def build_global_doc(
    now_text: str,
    workspace_root: Path,
    focus: List[Tuple[str, List[str], List[str]]],
    next_steps: List[Tuple[str, str]],
    runlookups: RunLookups,
) -> str:
    lines: List[str] = []
    lines.append("# Current Work - Global (Resume Fast)\n\n")
    lines.append(f"Updated: `{now_text}`\n")
    lines.append(f"Primary workspace: `{workspace_root}`\n\n")

    lines.append("## Quick Summary\n\n")
    for idx, (title, _, _) in enumerate(focus[:4], start=1):
        lines.append(f"{idx}. {title}\n")
    lines.append("\n")

    lines.append("## Do Next Now\n\n")
    for idx, (label, _) in enumerate(next_steps[:4], start=1):
        lines.append(f"{idx}. {label}\n")
    lines.append("\n")

    lines.append(build_where_looked(runlookups))
    lines.append(render_footnotes())
    return "".join(lines)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> int:
    args = parse_args()

    workspace_root = Path(args.workspace_root).expanduser().resolve()
    codex_home = Path(args.codex_home).expanduser().resolve()
    global_out = Path(args.global_out).expanduser().resolve()
    workspace_out = Path(args.workspace_out).expanduser().resolve()
    project_out = Path(args.project_out).expanduser().resolve() if args.project_out else None
    run_dir = Path(args.run_dir).expanduser().resolve() if args.run_dir else None

    scan_dirs = resolve_scan_dirs(workspace_root, args.scan_dirs, args.max_dirs)

    remote_checks: List[str] = []
    repo_snapshots: List[RepoSnapshot] = []

    for directory in scan_dirs:
        git_repo = is_git_repo(directory)

        branch = ""
        local_head = ""
        staged = modified = untracked = 0
        status_lines: List[str] = []
        remotes: List[RemoteSnapshot] = []

        if git_repo:
            branch, staged, modified, untracked, status_lines, local_head = parse_git_status(directory)
            remotes = remote_activity(directory, local_head, remote_checks)

        recent_files = [str(p) for p in collect_recent_files(directory)]
        tables = extract_tables(Path(p) for p in recent_files)

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
                status_lines=status_lines,
                recent_files=recent_files,
                tables=tables,
                remotes=remotes,
            )
        )

    codex_signals, open_tab_sources, latest_session_files, codex_paths = load_codex_signals(codex_home)

    focus = summarize_focus(repo_snapshots, codex_signals.open_tabs)
    next_steps = make_next_steps(repo_snapshots, workspace_root)

    runlookups = RunLookups(
        workspace_root=workspace_root,
        scanned_dirs=scan_dirs,
        codex_paths=codex_paths,
        latest_session_files=[Path(p) for p in latest_session_files],
        git_remote_checks=remote_checks,
        open_tab_sources=open_tab_sources,
    )

    now_text = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    workspace_doc = build_workspace_doc(
        now_text=now_text,
        workspace_root=workspace_root,
        repo_snapshots=repo_snapshots,
        focus=focus,
        next_steps=next_steps,
        codex=codex_signals,
        runlookups=runlookups,
    )

    global_doc = build_global_doc(
        now_text=now_text,
        workspace_root=workspace_root,
        focus=focus,
        next_steps=next_steps,
        runlookups=runlookups,
    )

    write_text(workspace_out, workspace_doc)
    write_text(global_out, global_doc)

    if project_out:
        write_text(project_out, workspace_doc)

    if run_dir:
        run_dir.mkdir(parents=True, exist_ok=True)
        write_text(run_dir / "workspace-current-work.md", workspace_doc)
        write_text(run_dir / "global-current-work.md", global_doc)
        metadata = {
            "generated_at": now_text,
            "workspace_root": str(workspace_root),
            "scan_dirs": [str(p) for p in scan_dirs],
            "workspace_out": str(workspace_out),
            "global_out": str(global_out),
            "project_out": str(project_out) if project_out else "",
            "open_tabs_detected": codex_signals.open_tabs,
            "session_files_used": latest_session_files,
            "remote_checks": remote_checks,
            "repos": [
                {
                    "name": r.name,
                    "path": str(r.path),
                    "is_git": r.is_git,
                    "branch": r.branch,
                    "staged": r.staged,
                    "modified": r.modified,
                    "untracked": r.untracked,
                    "remotes": [
                        {
                            "name": rm.name,
                            "url": rm.url,
                            "default_branch": rm.default_branch,
                            "remote_head": rm.remote_head,
                            "compare_to_local": rm.compare_to_local,
                        }
                        for rm in r.remotes
                    ],
                }
                for r in repo_snapshots
            ],
        }
        write_text(run_dir / "metadata.json", json.dumps(metadata, indent=2) + "\n")

    print(f"Wrote workspace doc: {workspace_out}")
    print(f"Wrote global doc: {global_out}")
    if project_out:
        print(f"Wrote project copy: {project_out}")
    if run_dir:
        print(f"Saved run artifacts: {run_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
