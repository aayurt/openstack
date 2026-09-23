#!/usr/bin/env python3
"""openstack-orchestrator — distributed task pipeline coordinator.

Replaces the Hermes task-worker cron + workdbctl/slotmon machinery. The
orchestrator owns the task queue, slot claims/leases, git worktrees, merges,
the PIPELINE event log, and a live SSE dashboard. Executors are thin workers:

    poll POST /api/claim  (slot, model) -> task or NONE
    per task:              create/enter worktree -> opencode run -> verify
    report POST /api/status (slot, task_id, outcome, verification) -> next action

Task files under /workspace/projects/*/TASKS/*.md remain the source of truth;
the orchestrator mirrors their frontmatter into SQLite and (re)writes status.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import subprocess
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
import uvicorn

from laya_client import TaskState, classify_task, triage_result, is_available as laya_available
from pipeline_routes import router as pipeline_router
from pipeline_runtime import init_pipeline_tables

# ── paths / config ─────────────────────────────────────────────────────────
WORKSPACE = Path(os.environ.get("ORCH_WS", "/workspace"))
PROJECTS = "/workspace/projects"
REPOS = Path(os.environ.get("ORCH_REPOS", "/projects/supreme"))
DATA_DIR = Path(os.environ.get("ORCH_DATA", "/data"))
DB = DATA_DIR / "orchestrator.db"
PIPELINE_LOG = WORKSPACE / "PIPELINE.log"
NOTIFY_DIR = WORKSPACE / "notify"
POOL_TTL_SECONDS = int(os.environ.get("ORCH_POOL_TTL", "24"))  # hours in pool
LAYA_ENABLED = os.environ.get("LAYA_ENABLED", "true").lower() in ("true", "1", "yes")

STATUSES = {"new", "ready", "in_progress", "verifying", "review", "done", "blocked"}
EVENT_PREFIXES = ("SELECT", "SESSION", "ASSIGN", "VERIFY", "REVIEW", "NOTIFY", "MERGE", "WATCH")

app = FastAPI(title="openstack-orchestrator")
app.include_router(pipeline_router)
_lock = threading.Lock()
_db_lock = threading.Lock()
sse_clients: set[asyncio.Queue] = set()
# executor liveness: slot -> {ts, last_seen, model, task, phase}
_beats: dict[str, dict] = {}
BEAT_TTL_SECONDS = 75  # executors heartbeat every 15s; 5 missed beats = offline

# ── Laya warmer ─────────────────────────────────────────────────────────────
LAYA_WARMER_INTERVAL = int(os.environ.get("LAYA_WARMER_INTERVAL", "120"))  # seconds
_laya_warmer_stop = threading.Event()


def _laya_warmer_loop() -> None:
    """Background thread that pings Laya to keep the model warm."""
    while not _laya_warmer_stop.is_set():
        try:
            if LAYA_ENABLED and laya_available():
                # Simple health check keeps the model loaded
                from laya_client import health
                health()
        except Exception:
            pass
        _laya_warmer_stop.wait(LAYA_WARMER_INTERVAL)


def start_laya_warmer() -> None:
    """Start the Laya warmer background thread."""
    if LAYA_ENABLED:
        threading.Thread(target=_laya_warmer_loop, daemon=True).start()


def stop_laya_warmer() -> None:
    """Stop the Laya warmer background thread."""
    _laya_warmer_stop.set()


# ── db ─────────────────────────────────────────────────────────────────────
def db_conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c


def db_init() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with db_conn() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                project TEXT NOT NULL,
                type TEXT,
                priority TEXT DEFAULT 'P2',
                status TEXT NOT NULL DEFAULT 'new',
                created TEXT,
                files_scope TEXT DEFAULT '[]',
                depends_on TEXT DEFAULT '[]',
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                retry_delay_seconds INTEGER DEFAULT 0,
                block_reason TEXT,
                verification TEXT DEFAULT '{}',
                slot TEXT,
                claimed_at TEXT,
                mtime REAL,
                complexity TEXT DEFAULT 'medium',
                laya_pre TEXT DEFAULT '{}',
                laya_triage TEXT DEFAULT '{}',
                laya_pre_version TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at TEXT NOT NULL,
                event TEXT NOT NULL,
                task TEXT,
                detail TEXT
            );
            CREATE TABLE IF NOT EXISTS audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at TEXT NOT NULL,
                slot TEXT,
                task TEXT,
                action TEXT,
                detail TEXT
            );
            """
        )
        # Ensure newer columns exist on existing databases
        cols = {row[1] for row in c.execute("PRAGMA table_info(tasks)").fetchall()}
        for col, col_def in [
            ("retry_delay_seconds", "INTEGER DEFAULT 0"),
            ("complexity", "TEXT DEFAULT 'medium'"),
            ("laya_pre", "TEXT DEFAULT '{}'"),
            ("laya_triage", "TEXT DEFAULT '{}'"),
            ("laya_pre_version", "TEXT DEFAULT ''"),
        ]:
            if col not in cols:
                c.execute(f"ALTER TABLE tasks ADD COLUMN {col} {col_def}")
        init_pipeline_tables(c)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def add_event(event: str, task: str | None, detail: str = "") -> None:
    try:
        with db_conn() as c:
            c.execute(
                "INSERT INTO events(at, event, task, detail) VALUES(?,?,?,?)",
                (_now(), event, task, detail),
            )
    except Exception:
        pass


def audit(slot: str, task: str | None, action: str, detail: str = "") -> None:
    try:
        with db_conn() as c:
            c.execute(
                "INSERT INTO audit(at, slot, task, action, detail) VALUES(?,?,?,?,?)",
                (_now(), slot, task, action, detail),
            )
    except Exception:
        pass


# ── frontmatter parsing (mirrors workdbctl.parse_frontmatter) ─────────────
def parse_frontmatter(text: str) -> dict:
    fm: dict = {}
    if not text.startswith("---"):
        return fm
    lines = text.split("\n")[1:]
    last_key = None
    for line in lines:
        if line == "---":
            break
        if line.startswith("  ") and last_key is not None:
            item = line.strip()
            if item.startswith("- "):
                lst = fm.setdefault(last_key, [])
                if isinstance(lst, list):
                    lst.append(item[2:].strip())
            continue
        key, sep, val = line.partition(":")
        if not sep:
            continue
        key = key.strip()
        val = val.strip()
        if val.startswith("[") or val.startswith('"'):
            try:
                val = json.loads(val)
            except Exception:
                pass
        if val == "":
            fm[key] = []
            last_key = key
        else:
            fm[key] = val
            last_key = None
    return fm


def _as_list(v) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        s = v.strip()
        if s.startswith("["):
            try:
                return [str(x) for x in json.loads(s)]
            except Exception:
                pass
        if s:
            return [x.strip() for x in s.split(",")]
    return []


def replace_frontmatter(text: str, updates: dict) -> str:
    """Rewrite YAML frontmatter fields in-place, preserving the rest."""
    lines = text.split("\n")
    if not lines or lines[0] != "---":
        return text
    end = 1
    for i in range(1, len(lines)):
        if lines[i] == "---":
            end = i
            break
    fm_lines = lines[1:end]
    out: list[str] = []
    for k, v in updates.items():
        if isinstance(v, (dict, list)):
            v = json.dumps(v)
        elif isinstance(v, bool):
            v = "true" if v else "false"
        out.append(f"{k}: {v}")
    # remove any existing keys we're replacing
    keys_done: set[str] = set()
    kept: list[str] = []
    for ln in fm_lines:
        key = ln.split(":", 1)[0].strip()
        if key in updates:
            if key in keys_done:
                continue
            keys_done.add(key)
            continue
        kept.append(ln)
    new_fm = kept + out
    return lines[0] + "\n" + "\n".join(new_fm) + "\n" + lines[end] + "\n" + "\n".join(lines[end + 1 :])


def task_files(projects_root: Path = Path(PROJECTS)) -> list[tuple[Path, str, dict]]:
    out = []
    if not projects_root.is_dir():
        return out
    for pdir in sorted(projects_root.iterdir()):
        tasks_dir = pdir / "TASKS"
        if not tasks_dir.is_dir():
            continue
        for f in sorted(tasks_dir.glob("*.md")):
            try:
                fm = parse_frontmatter(f.read_text())
            except Exception:
                continue
            if fm.get("id"):
                out.append((f, str(fm["id"]), fm))
    return out


def _task_state_from_file(f: Path, fm: dict) -> TaskState:
    """Build a TaskState from a task file and its frontmatter."""
    text = f.read_text() if f.exists() else ""
    return TaskState(
        id=str(fm.get("id", "")),
        project=str(fm.get("project", "")),
        type=str(fm.get("type", "task")),
        priority=str(fm.get("priority", "P2")),
        files_scope=_as_list(fm.get("files_scope")),
        depends_on=_as_list(fm.get("depends_on")),
        plan=_section(text, "Plan"),
        acceptance=_section(text, "Acceptance criteria"),
        frontmatter=fm,
    )


def _section(text: str, name: str) -> str:
    """Extract a markdown section by ## header name."""
    m = re.search(rf"## {name}[^\n]*\n(.*?)(?=\n## |\Z)", text, re.S)
    return m.group(1).strip() if m else ""


def _significant_change(old_laya: dict, new_fm: dict) -> bool:
    """Check if task materially changed to warrant reclassification.
    
    Reclassify when:
    - objective/plan changes
    - acceptance criteria change
    - relevant files/dependencies change
    - Hermes re-plans the task
    
    Don't reclassify merely because status changes.
    """
    old_files = set(old_laya.get("files_scope", []))
    new_files = set(new_fm.get("files_scope", []))
    
    old_deps = set(old_laya.get("depends_on", []))
    new_deps = set(new_fm.get("depends_on", []))
    
    return (
        old_files != new_files or
        old_deps != new_deps or
        old_laya.get("plan") != new_fm.get("plan") or
        old_laya.get("acceptance") != new_fm.get("acceptance")
    )


# ── sync task files -> db ──────────────────────────────────────────────────
def sync_tasks() -> None:
    with _db_lock:
        with db_conn() as c:
            rows = {r["id"]: r for r in c.execute("SELECT * FROM tasks")}
        seen = set()
        for f, tid, fm in task_files():
            seen.add(tid)
            mtime = f.stat().st_mtime
            st = str(fm.get("status", "new")).strip()
            if st not in STATUSES:
                st = "new"
            if tid in rows:
                row = rows[tid]
                changed = (
                    row["status"] != st
                    or row["project"] != fm.get("project")
                    or row["mtime"] != mtime
                )
                # status/fields changed on the FILE → respect it unless the
                # row is mid-flight owned by a slot.
                if changed:
                    with db_conn() as c:
                        c.execute(
                            """UPDATE tasks SET status=?, project=?, type=?,
                               priority=?, created=?, files_scope=?,
                               depends_on=?, retry_count=?, max_retries=?,
                               block_reason=?, mtime=?
                               WHERE id=?""",
                            (
                                st,
                                fm.get("project", ""),
                                fm.get("type", "task"),
                                str(fm.get("priority", "P2")),
                                str(fm.get("created", "")),
                                json.dumps(_as_list(fm.get("files_scope"))),
                                json.dumps(_as_list(fm.get("depends_on"))),
                                int(fm.get("retry_count", 0) or 0),
                                int(fm.get("max_retries", 3) or 3),
                                fm.get("block_reason"),
                                mtime,
                                tid,
                            ),
                        )
                        # If status changed from in_progress → ready/blocked,
                        # release the slot so the task can be re-claimed.
                        if row["status"] == "in_progress" and st in ("ready", "blocked"):
                            c.execute(
                                "UPDATE tasks SET slot=NULL, claimed_at=NULL WHERE id=?",
                                (tid,),
                            )
                if LAYA_ENABLED and laya_available() and row["mtime"] != mtime:
                    # Task changed → reclassify if significant
                    old_laya = json.loads(row.get("laya_pre") or "{}")
                    text = f.read_text() if f.exists() else ""
                    old_fm_compare = {
                        "files_scope": _as_list(old_laya.get("files_scope", [])),
                        "depends_on": _as_list(old_laya.get("depends_on", [])),
                        "plan": old_laya.get("plan_summary", ""),
                        "acceptance": old_laya.get("acceptance_summary", ""),
                    }
                    new_fm_compare = {
                        "files_scope": _as_list(fm.get("files_scope", [])),
                        "depends_on": _as_list(fm.get("depends_on", [])),
                        "plan": _section(text, "Plan"),
                        "acceptance": _section(text, "Acceptance criteria"),
                    }
                    if _significant_change(old_fm_compare, new_fm_compare):
                        try:
                            task_state = _task_state_from_file(f, fm)
                            laya_result = classify_task(task_state)
                            with db_conn() as c:
                                c.execute(
                                    "UPDATE tasks SET laya_pre=?, laya_pre_version=?, complexity=? WHERE id=?",
                                    (
                                        json.dumps(laya_result),
                                        _now(),
                                        laya_result.get("complexity", row["complexity"]),
                                        tid,
                                    ),
                                )
                        except Exception:
                            pass
            else:
                # New task → classify and insert
                laya_pre = "{}"
                laya_pre_version = ""
                complexity = "medium"

                if LAYA_ENABLED and laya_available():
                    try:
                        task_state = _task_state_from_file(f, fm)
                        laya_result = classify_task(task_state)
                        laya_pre = json.dumps(laya_result)
                        laya_pre_version = _now()
                        complexity = laya_result.get("complexity", "medium")
                        # Adjust priority if Laya recommends
                        if laya_result.get("priority_adjust") == "escalate":
                            current_p = str(fm.get("priority", "P2"))
                            if current_p.startswith("P"):
                                num = int(current_p[1:])
                                if num > 1:
                                    fm["priority"] = f"P{num - 1}"
                                    st = str(fm.get("status", "new")).strip()
                                    if st not in STATUSES:
                                        st = "new"
                    except Exception:
                        pass  # Laya fallback: use defaults

                with db_conn() as c:
                    c.execute(
                        """INSERT OR IGNORE INTO tasks(id, project, type, priority, status,
                           created, files_scope, depends_on, retry_count,
                           max_retries, block_reason, mtime, complexity, laya_pre, laya_pre_version)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            tid,
                            fm.get("project", ""),
                            fm.get("type", "task"),
                            str(fm.get("priority", "P2")),
                            st,
                            str(fm.get("created", "")),
                            json.dumps(_as_list(fm.get("files_scope"))),
                            json.dumps(_as_list(fm.get("depends_on"))),
                            int(fm.get("retry_count", 0) or 0),
                            int(fm.get("max_retries", 3) or 3),
                            fm.get("block_reason"),
                            mtime,
                            complexity,
                            laya_pre,
                            laya_pre_version,
                        ),
                    )
        # remove tasks whose files vanished and which nothing owns
        with db_conn() as c:
            for tid, row in rows.items():
                if tid in seen or row["status"] in ("in_progress", "verifying"):
                    continue
                if row["slot"]:
                    continue
                c.execute("DELETE FROM tasks WHERE id=?", (tid,))


# ── eligibility (mirrors workdbctl) ────────────────────────────────────────
def scope_overlap(a: list[str], b: list[str]) -> bool:
    for pa in a:
        pa = pa.rstrip("/")
        for pb in b:
            pb = pb.rstrip("/")
            if "*" in (pa, pb) or pa == pb:
                return True
            if pa.startswith(pb + "/") or pb.startswith(pa + "/"):
                return True
    return False


def _file_status(conn, tid: str) -> str:
    r = conn.execute("SELECT status FROM tasks WHERE id=?", (tid,)).fetchone()
    return r["status"] if r else "unknown"


def eligible_claim() -> str | None:
    """Oldest eligible task id, honouring deps + scope + Laya readiness."""
    with _db_lock:
        with db_conn() as c:
            rows = [dict(r) for r in c.execute("SELECT * FROM tasks")]
    # occupied scopes by other in-flight tasks
    other_occ: dict[str, list[list[str]]] = {}
    for r in rows:
        if r["status"] in ("in_progress", "verifying") and r["slot"]:
            other_occ.setdefault(r["project"], []).append(_as_list(r["files_scope"]))
    # check if any large task is already in progress (concurrency constraint)
    large_in_progress = any(
        r["complexity"] == "large" and r["status"] == "in_progress"
        for r in rows
    ) if LAYA_ENABLED else False
    cands = []
    for r in rows:
        if r["status"] not in ("new", "ready"):
            continue
        scope = _as_list(r["files_scope"]) or ["*"]
        project = r["project"]
        if any(scope_overlap(scope, s) for s in other_occ.get(project, [])):
            continue
        if r["slot"]:
            continue
        # Concurrency constraint: skip large tasks if one is already running
        if LAYA_ENABLED and r["complexity"] == "large" and large_in_progress:
            continue
        deps_ok = True
        for d in _as_list(r["depends_on"]):
            with db_conn() as c:
                if _file_status(c, d) not in ("done", "blocked"):
                    deps_ok = False
                    break
        if not deps_ok:
            continue
        # Laya readiness check: skip tasks Laya marks as not ready
        if LAYA_ENABLED:
            laya_pre = json.loads(r.get("laya_pre") or "{}")
            if laya_pre.get("ready") == "blocked_deps":
                # Double-check dependencies are actually met
                continue
            if laya_pre.get("ready") == "blocked_missing_info":
                # Skip tasks needing clarification
                continue
        cands.append((r["created"] or "", r["id"], project, scope))
    if not cands:
        return None
    cands.sort(key=lambda t: (t[0], t[1]))
    return cands[0][1]


def unanswered_questions(task_file: Path) -> list[str]:
    text = task_file.read_text()
    in_q = False
    qs: list[str] = []
    for line in text.split("\n"):
        s = line.strip()
        if s.startswith("## Questions"):
            in_q = True
            continue
        if in_q and s.startswith("## "):
            break
        if in_q and s.startswith("Q:") and "A:" not in s:
            qs.append(s[2:].strip())
    return qs


# ── git helpers (worktree create / merge) ─────────────────────────────────
def git(repo: Path, *args) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    for k, v in (("GIT_AUTHOR_NAME", "openstack-orchestrator"),
                 ("GIT_AUTHOR_EMAIL", "orchestrator@openstack.local"),
                 ("GIT_COMMITTER_NAME", "openstack-orchestrator"),
                 ("GIT_COMMITTER_EMAIL", "orchestrator@openstack.local")):
        env.setdefault(k, v)
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True, text=True, env=env, timeout=300,
    )


def worktree_path(project: str, tid: str) -> Path:
    return REPOS / f"{project}-{tid}"


def ensure_worktree(project: str, tid: str) -> tuple[Path, bool]:
    repo = REPOS / project
    wt = worktree_path(project, tid)
    if not (repo / ".git").is_dir() and not (repo / ".git").is_file():
        raise RuntimeError(f"not a git repo: {repo}")
    if wt.exists():
        return wt, True
    r = git(repo, "worktree", "add", str(wt), "-b", f"task/{tid}", "main")
    if r.returncode != 0:
        raise RuntimeError(f"worktree add failed: {r.stderr.strip()}")
    return wt, False


def node_modules_symlinks(main_repo: Path, wt: Path) -> list[Path]:
    """Symlink node_modules trees from the main checkout into a bare worktree
    so verification builds resolve deps without a full pnpm install."""
    links = []
    candidates = [main_repo / "node_modules"] + list((main_repo / "apps").glob("*/node_modules")) \
        if (main_repo / "apps").is_dir() else [main_repo / "node_modules"]
    for nm in candidates:
        if not nm.exists():
            continue
        rel = nm.relative_to(main_repo)
        target = wt / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() or target.is_symlink():
            continue
        try:
            target.symlink_to(nm, target_is_directory=True)
            links.append(target)
        except OSError:
            continue
    return links


def merge_and_clean(project: str, tid: str) -> dict:
    repo = REPOS / project
    wt = worktree_path(project, tid)
    branch = f"task/{tid}"
    # Stash uncommitted changes before merging into main.
    stashed = False
    stash_r = git(repo, "stash", "--include-untracked")
    if stash_r.returncode == 0 and "No local changes" not in stash_r.stdout:
        stashed = True
    # Merge task branch into main.
    r = git(repo, "merge", "--no-ff", branch, "-m", f"task {tid}")
    merge_rc = r.returncode
    merge_err = r.stderr.strip()
    # Restore stashed changes if we stashed.
    if stashed:
        git(repo, "stash", "pop")
    if merge_rc == 0:
        git(repo, "worktree", "remove", str(wt), "--force")
    return {"merged": merge_rc == 0, "error": merge_err or r.stdout[-500:]}


# ── task file writers ──────────────────────────────────────────────────────
def task_file_for(tid: str) -> Path | None:
    for f, _, _ in task_files():
        if f.stem == tid:
            return f
    return None


def write_task_status(tid: str, status: str, extra: dict | None = None) -> None:
    f = task_file_for(tid)
    if not f:
        return
    updates = {"status": status}
    if extra:
        updates.update(extra)
    f.write_text(replace_frontmatter(f.read_text(), updates))
    sync_tasks()


def pipeline_log(prefix: str, tid: str, detail: str = "") -> None:
    line = f"[{datetime.now().astimezone().isoformat(timespec='seconds')}] {prefix} {tid} {detail}".strip()
    with open(PIPELINE_LOG, "a") as fh:
        fh.write(line + "\n")
    audit("pipeline", tid, prefix, detail)


def ws_git_commit(message: str) -> None:
    git(WORKSPACE, "add", "-A")
    git(WORKSPACE, "commit", "-m", message, "--quiet")


# ── API models ────────────────────────────────────────────────────────────
class ClaimReq(BaseModel):
    slot: str
    model: str = "opencode/big-pickle"
    host: str = ""


class PlanReq(BaseModel):
    slot: str
    task_id: str
    plan_summary: str = ""


@app.post("/api/plan")
def plan_done(req: PlanReq):
    """Planning finished: mark the task ready for implementation."""
    sync_tasks()
    with db_conn() as c:
        row = c.execute("SELECT * FROM tasks WHERE id=?", (req.task_id,)).fetchone()
    if not row:
        return {"ok": False, "error": "unknown task"}
    write_task_status(req.task_id, "ready")
    with db_conn() as c:
        c.execute("UPDATE tasks SET status='ready', slot=NULL, claimed_at=NULL WHERE id=?",
                  (req.task_id,))
    pipeline_log("PLAN", req.task_id, f"ready (slot={req.slot}) {req.plan_summary[:80]}")
    audit(req.slot, req.task_id, "plan", req.plan_summary[:200])
    return {"ok": True, "next": "ready"}


class StatusReport(BaseModel):
    slot: str
    task_id: str
    outcome: str                    # done | review | blocked | skipped
    verification: dict = {}
    block_reason: str | None = None
    review_feedback: str | None = None


class NotifyReq(BaseModel):
    target: str = "telegram"
    message: str


# ── SSE ────────────────────────────────────────────────────────────────────
async def sse_broadcast(event: str, data: dict) -> None:
    msg = f"event: {event}\ndata: {json.dumps(data)}\n\n"
    for q in list(sse_clients):
        try:
            q.put_nowait(msg)
        except Exception:
            sse_clients.discard(q)


async def sse_stream(request: Request):
    q: asyncio.Queue = asyncio.Queue()
    sse_clients.add(q)
    try:
        yield "event: connected\ndata: {}\n\n"
        while True:
            if await request.is_disconnected():
                break
            try:
                msg = await asyncio.wait_for(q.get(), timeout=15)
                yield msg
            except asyncio.TimeoutError:
                yield "event: ping\ndata: {}\n\n"
    finally:
        sse_clients.discard(q)


# ── routes ─────────────────────────────────────────────────────────────────
@app.get("/events")
async def events(request: Request):
    return StreamingResponse(sse_stream(request), media_type="text/event-stream")


@app.get("/api/snapshot")
def snapshot(include_events: bool = True, include_audit: bool = False, limit: int = 50):
    sync_tasks()
    with db_conn() as c:
        tasks = [dict(r) for r in c.execute(
            "SELECT * FROM tasks ORDER BY created, id LIMIT ?", (limit,))]
        events = [dict(r) for r in c.execute(
            "SELECT * FROM events ORDER BY id DESC LIMIT 100")] if include_events else []
        audit_rows = [dict(r) for r in c.execute(
            "SELECT * FROM audit ORDER BY id DESC LIMIT 50")] if include_audit else []
    return {"tasks": tasks, "events": events, "audit": audit_rows,
            "slots": slots_state(), "now": _now()}


# ── executor heartbeats ────────────────────────────────────────────────────
def slots_state() -> dict:
    """Liveness map for the dashboard: slot -> heartbeat info (or offline)."""
    now = time.time()
    out = {}
    for slot, b in _beats.items():
        age = now - b["ts"]
        out[slot] = {
            "online": age < BEAT_TTL_SECONDS,
            "age_s": round(age),
            "model": b.get("model", ""),
            "task": b.get("task"),
            "phase": b.get("phase", "idle"),
            "last_seen": b.get("last_seen"),
        }
    return out


class BeatReq(BaseModel):
    slot: str
    task: str | None = None
    phase: str = "idle"          # idle | planning | implementing | verifying
    model: str = ""


@app.post("/api/heartbeat")
def heartbeat(req: BeatReq):
    _beats[req.slot] = {
        "ts": time.time(),
        "last_seen": _now(),
        "model": req.model,
        "task": req.task,
        "phase": req.phase,
    }
    return {"ok": True}


@app.get("/api/slots")
def slots():
    return {"slots": slots_state(), "now": _now()}


@app.get("/api/config")
def get_config():
    """Get current orchestrator config including feature flags."""
    return {
        "laya_enabled": LAYA_ENABLED,
        "laya_available": laya_available(),
        "laya_warmer_interval": LAYA_WARMER_INTERVAL,
        "laya_warmer_active": not _laya_warmer_stop.is_set(),
    }


class ConfigUpdate(BaseModel):
    laya_enabled: bool | None = None


@app.post("/api/config")
def update_config(req: ConfigUpdate):
    """Update orchestrator config (runtime toggle for feature flags)."""
    global LAYA_ENABLED
    if req.laya_enabled is not None:
        LAYA_ENABLED = req.laya_enabled
        pipeline_log("CONFIG", None, f"LAYA_ENABLED={LAYA_ENABLED}")
    return {"ok": True, "laya_enabled": LAYA_ENABLED}


# ── task detail (modal payload) ────────────────────────────────────────────
def _section(text: str, name: str) -> str:
    m = re.search(rf"## {name}[^\n]*\n(.*?)(?=\n## |\Z)", text, re.S)
    return m.group(1).strip() if m else ""


@app.get("/api/task/{task_id}")
def task_detail(task_id: str):
    sync_tasks()
    with db_conn() as c:
        row = c.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"unknown task {task_id}")
    t = dict(row)
    tf = task_file_for(task_id)
    body = tf.read_text(errors="replace") if tf and tf.exists() else ""
    sections = {}
    for key, name in (("plan", "Plan"), ("acceptance", "Acceptance criteria"),
                      ("worklog", "Work log"), ("questions", "Questions"),
                      ("user_report", "User report"), ("analysis", "Analysis")):
        sections[key] = _section(body, name)
    with db_conn() as c:
        events = [dict(r) for r in c.execute(
            "SELECT * FROM events WHERE task=? ORDER BY id DESC LIMIT 50", (task_id,))]
        audit_rows = [dict(r) for r in c.execute(
            "SELECT * FROM audit WHERE task=? ORDER BY id DESC LIMIT 50", (task_id,))]
    fstat = None
    if tf and tf.exists():
        st = tf.stat()
        fstat = {"mtime": st.st_mtime, "size": st.st_size, "path": str(tf)}
    return {"task": t, "sections": sections, "events": events,
            "audit": audit_rows, "file": fstat}


@app.get("/api/state")
def state():
    with db_conn() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM tasks")]
    return {"tasks": [r for r in rows if r["status"] in ("in_progress", "verifying")], "now": _now()}


@app.post("/api/claim")
def claim(req: ClaimReq):
    sync_tasks()
    with _lock:
        tid = eligible_claim()
        if not tid:
            return {"task": None}
        f = task_file_for(tid)
        if f and f.exists():
            qs = unanswered_questions(f)
            if qs:
                write_task_status(tid, "ready")
                pipeline_log("ASK", tid, f"⏸️ waiting for answers: {qs[0]}")
                audit(req.slot, tid, "skip", "unanswered questions")
                return {"task": None, "unanswered_questions": qs}
        row = None
        with db_conn() as c:
            row = c.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
        if not row:
            return {"task": None}
        stage = row["status"]  # new | ready
        if stage == "ready":
            try:
                wt, existed = ensure_worktree(row["project"], tid)
                node_modules_symlinks(REPOS / row["project"], wt) if wt else None
            except RuntimeError as e:
                pipeline_log("REVIEW", tid, f"blocked (worktree): {e}")
                write_task_status(tid, "blocked", {"block_reason": str(e)})
                return {"task": None, "error": str(e)}
        else:
            wt = worktree_path(row["project"], tid)
        now = _now()
        with db_conn() as c:
            c.execute(
                "UPDATE tasks SET slot=?, claimed_at=?, status=? WHERE id=?",
                (req.slot, now, "in_progress" if stage == "ready" else "new", tid),
            )
        if stage == "ready":
            write_task_status(tid, "in_progress")
            pipeline_log("EMPLOY", tid, f"slot={req.slot}")
            audit(req.slot, tid, "claim", f"worktree={wt} model={req.model}")
        else:
            audit(req.slot, tid, "claim", "planning (new)")
        return {
            "task": {
                "id": tid,
                "project": row["project"],
                "status": stage,
                "priority": row["priority"],
                "created": row["created"],
                "retry_count": row["retry_count"],
                "max_retries": row["max_retries"],
            },
            "worktree": str(wt),
            "repo": str(REPOS / row["project"]),
            "stage": stage,
        }


@app.post("/api/status")
def status_report(req: StatusReport):
    sync_tasks()
    with db_conn() as c:
        row = c.execute("SELECT * FROM tasks WHERE id=?", (req.task_id,)).fetchone()
    if not row:
        return {"ok": False, "error": "unknown task"}
    f = task_file_for(req.task_id)
    retry = int(row["retry_count"] or 0)
    max_r = int(row["max_retries"] or 3)
    out = {"ok": True, "slot": req.slot, "task": req.task_id}

    if req.outcome == "done":
        merge = merge_and_clean(row["project"], req.task_id)
        if not merge["merged"]:
            out["ok"] = False
            out["error"] = f"merge failed: {merge['error']}"
            pipeline_log("REVIEW", req.task_id, f"review (merge failed: {merge['error'][:120]})")
            audit(req.slot, req.task_id, "merge-fail", merge["error"][:300])
            # Release slot and requeue for retry on merge failure.
            retry += 1
            status = "review" if retry >= max_r else "ready"
            block_reason = req.block_reason or (f"merge failed; retry {retry}/{max_r} exhausted" if retry >= max_r else None)
            write_task_status(req.task_id, status, {
                "retry_count": retry,
                "block_reason": block_reason,
            })
            with db_conn() as c:
                c.execute("UPDATE tasks SET status=?, slot=NULL, claimed_at=NULL, retry_count=?, block_reason=? WHERE id=?",
                          (status, retry, block_reason, req.task_id))
            pipeline_log("REVIEW", req.task_id, f"{status} (slot={req.slot}, merge failed)")
            audit(req.slot, req.task_id, "merge-fail-retry", f"retry {retry}/{max_r}")
            out["next"] = "ready" if status == "ready" else "blocked"
            out["retry_count"] = retry
            return out
        pipeline_log("MERGE", req.task_id, f"-> main (slot={req.slot})")
        audit(req.slot, req.task_id, "merge", "merged branch to main")
        write_task_status(
            req.task_id,
            "done",
            {"verification": json.dumps(req.verification)},
        )
        pipeline_log("REVIEW", req.task_id, f"done (slot={req.slot})")
        with db_conn() as c:
            c.execute("UPDATE tasks SET status=?, slot=NULL, claimed_at=NULL, verification=? WHERE id=?",
                      ("done", json.dumps(req.verification), req.task_id))
        out["next"] = "done"
    elif req.outcome == "review":
        # Laya triage: classify the failure and decide retry/escalate
        laya_triage = {}
        
        # Check if already triaged (idempotency)
        existing_triage = json.loads(row.get("laya_triage") or "{}")
        if existing_triage.get("verdict"):
            # Already triaged, use existing result
            laya_triage = existing_triage
        elif LAYA_ENABLED and laya_available():
            # New triage
            try:
                tf = task_file_for(req.task_id)
                task_state = _task_state_from_file(tf, dict(row)) if tf else None
                if task_state:
                    raw_triage = triage_result(
                        task_state,
                        req.verification,
                        req.review_feedback or "",
                    )
                    laya_triage = raw_triage  # Already validated in triage_result()
            except Exception:
                pass  # Laya fallback: use default retry logic

        # Store Laya triage result
        if laya_triage:
            with db_conn() as c:
                c.execute(
                    "UPDATE tasks SET laya_triage=? WHERE id=?",
                    (json.dumps(laya_triage), req.task_id),
                )

        # Escalate to Hermes if Laya says so
        if laya_triage.get("verdict") == "escalate":
            block_reason = f"Laya escalation: {laya_triage.get('retry_reason', 'unknown')}"
            write_task_status(req.task_id, "blocked", {
                "block_reason": block_reason,
                "retry_count": retry,
            })
            with db_conn() as c:
                c.execute(
                    "UPDATE tasks SET status='blocked', slot=NULL, claimed_at=NULL, "
                    "retry_count=?, block_reason=? WHERE id=?",
                    (retry, block_reason, req.task_id),
                )
            pipeline_log("REVIEW", req.task_id, f"escalated (slot={req.slot}): {block_reason}")
            audit(req.slot, req.task_id, "escalate", block_reason)
            # Write structured escalation for Hermes
            NOTIFY_DIR.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            notify_file = NOTIFY_DIR / f"{ts}-escalate-{req.task_id}.json"
            escalation = {
                "type": "hermes_replan",
                "task_id": req.task_id,
                "reason": laya_triage.get("retry_reason", "unknown"),
                "context": {
                    "verification": req.verification,
                    "laya_triage": laya_triage,
                    "review_feedback": req.review_feedback,
                },
                "timestamp": _now(),
            }
            notify_file.write_text(json.dumps(escalation, indent=2))
            out["next"] = "blocked"
            out["retry_count"] = retry
            return out

        # Retry with exponential backoff (or old-style simple retry if Laya disabled)
        retry += 1
        if retry >= max_r:
            status = "review"  # blocked after max retries
            block_reason = req.block_reason or f"retry {retry}/{max_r} exhausted"
        else:
            status = "ready"
            if LAYA_ENABLED:
                # Exponential backoff: 60s, 120s, 240s, max 480s
                delay = min(60 * (2 ** (retry - 1)), 480)
                with db_conn() as c:
                    c.execute(
                        "UPDATE tasks SET retry_delay_seconds=? WHERE id=?",
                        (delay, req.task_id),
                    )
                block_reason = req.block_reason or f"retry {retry}/{max_r} (delay {delay}s)"
            else:
                block_reason = req.block_reason or f"retry {retry}/{max_r}"

        # append review feedback into the task file
        if f and req.review_feedback and retry < max_r:
            retry_reason = laya_triage.get("retry_reason", "") if laya_triage else ""
            f.write_text(
                f.read_text().rstrip()
                + f"\n\n## Review feedback (round {retry})\n\n"
                + f"Laya verdict: retry ({retry_reason})\n\n"
                + req.review_feedback
                + "\n"
            )
        write_task_status(req.task_id, status, {
            "retry_count": retry,
            "block_reason": block_reason,
        })
        with db_conn() as c:
            c.execute(
                "UPDATE tasks SET status=?, slot=NULL, claimed_at=NULL, "
                "retry_count=?, block_reason=? WHERE id=?",
                (status, retry, block_reason, req.task_id),
            )
        pipeline_log("REVIEW", req.task_id, f"{status} (slot={req.slot})")
        audit(req.slot, req.task_id, "review", f"round {retry} ({laya_triage.get('verdict', 'retry')})")
        out["next"] = "ready" if status == "ready" else "blocked"
        out["retry_count"] = retry
    elif req.outcome == "blocked":
        write_task_status(req.task_id, "blocked", {"block_reason": req.block_reason})
        with db_conn() as c:
            c.execute("UPDATE tasks SET status='blocked', slot=NULL, claimed_at=NULL, block_reason=? WHERE id=?",
                      (req.block_reason, req.task_id))
        pipeline_log("REVIEW", req.task_id, f"blocked (slot={req.slot}): {req.block_reason}")
        audit(req.slot, req.task_id, "blocked", req.block_reason or "")
        out["next"] = "blocked"
    elif req.outcome == "skipped":
        # executor could not run (engine unreachable, deps etc.) — requeue
        write_task_status(req.task_id, "ready")
        with db_conn() as c:
            c.execute("UPDATE tasks SET status='ready', slot=NULL, claimed_at=NULL WHERE id=?",
                      (req.task_id,))
        pipeline_log("REVIEW", req.task_id, "ready (skipped by executor)")
        audit(req.slot, req.task_id, "skip", req.block_reason or "")
        out["next"] = "ready"
    else:
        out["ok"] = False
        out["error"] = f"unknown outcome {req.outcome}"

    # persist a fresh PIPELINE log + task-file commit
    try:
        ws_git_commit(f"chore(pipeline): {req.task_id} {req.outcome}")
    except Exception:
        pass
    return out


@app.post("/api/notify")
def notify(req: NotifyReq):
    NOTIFY_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    safe = re.sub(r"[^A-Za-z0-9_-]", "", req.message[:40]).strip() or "notify"
    f = NOTIFY_DIR / f"{ts}-{safe}.{req.target}"
    f.write_text(req.message + "\n")
    pipeline_log("NOTIFY", "", f"{req.target} \"{req.message[:120]}\"")
    add_event("notify", None, req.message[:200])
    return {"ok": True, "file": str(f)}


@app.get("/api/notifications")
def notifications_drain():
    """Host-side Hermes no-agent cron drains this once per minute."""
    NOTIFY_DIR.mkdir(parents=True, exist_ok=True)
    pending = []
    for f in sorted(NOTIFY_DIR.iterdir()):
        if f.suffix.lstrip(".") != "telegram":
            continue
        pending.append({"file": f.name, "message": f.read_text().strip(), "target": f.suffix.lstrip(".")})
    return {"pending": pending}


@app.post("/api/notifications/ack")
def notifications_ack(files: list[str]):
    for name in files:
        f = NOTIFY_DIR / name
        if f.exists():
            f.unlink()
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML


@app.get("/pipeline", response_class=HTMLResponse)
def pipeline_editor():
    """Visual pipeline editor — serves the SPA."""
    import pathlib
    html_path = pathlib.Path(__file__).parent / "pipeline.html"
    if html_path.exists():
        return html_path.read_text()
    return "<h1>Pipeline editor not found</h1><p>Place pipeline.html in the orchestrator directory.</p>"


DASHBOARD_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>openstack-orchestrator · pipeline</title>
<style>
:root{--bg:#0f1115;--panel:#171a21;--line:#242836;--fg:#dfe3ec;--mut:#8b93a7;
--ok:#3fb950;--warn:#d29922;--bad:#f85149;--blue:#58a6ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);
font:13px/1.45 system-ui,Segoe UI,Roboto,sans-serif}
header{padding:14px 20px;border-bottom:1px solid var(--line);display:flex;
gap:18px;align-items:baseline;position:sticky;top:0;background:var(--bg);z-index:5}
header h1{margin:0;font-size:16px;color:var(--blue)}
header .live{color:var(--ok);font-size:12px}
header .hdr-meta{color:var(--mut);font-size:11px;margin-left:auto}
.graph{position:relative;padding:16px 20px 6px;min-width:900px}
svg.wires{position:absolute;inset:0;width:100%;height:100%;pointer-events:none}
svg.wires path{fill:none;stroke:var(--line);stroke-width:1.5}
svg.wires path.hot{stroke:var(--blue);stroke-dasharray:6 4;animation:flow 1s linear infinite}
@keyframes flow{to{stroke-dashoffset:-10}}
.stages{display:grid;grid-template-columns:repeat(6,1fr);gap:26px;position:relative}
.stage{position:relative;z-index:1;border:1px solid var(--line);border-radius:10px;
background:var(--panel);padding:8px 9px;min-height:96px}
.stage h4{margin:0 0 7px;font-size:10px;letter-spacing:.07em;text-transform:uppercase;
color:var(--mut);display:flex;justify-content:space-between}
.stage.active{border-color:var(--blue);box-shadow:0 0 0 1px var(--blue) inset}
.stage.done-col{border-color:var(--ok)}
.stage .cnt{color:var(--fg);font-weight:600}
.chip{border:1px solid var(--line);border-radius:7px;padding:4px 7px;margin-bottom:5px;
background:#1c2029;cursor:pointer;transition:border-color .15s}
.chip:hover{border-color:var(--blue)}
.chip b{font-size:11px}
.chip .sub{color:var(--mut);font-size:10px;display:block}
.chip.blocked{border-color:var(--bad)}.chip.blocked b{color:var(--bad)}
.chip.working{border-color:var(--warn)}.chip.working b{color:var(--warn)}
.chip.done{border-color:var(--ok)}.chip.done b{color:var(--ok)}
.chip.new{border-color:var(--blue)}.chip.new b{color:var(--blue)}
.blocked-lane{margin:2px 20px 0;border:1px dashed var(--bad);border-radius:10px;
padding:7px 10px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.blocked-lane:empty{display:none}
.blocked-lane .lbl{color:var(--bad);font-size:10px;letter-spacing:.07em;text-transform:uppercase}
.execs{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;padding:12px 20px 0}
.exec{border:1px solid var(--line);border-radius:9px;padding:6px 9px;background:var(--panel);
display:flex;justify-content:space-between;align-items:center;gap:8px}
.exec .dot{width:7px;height:7px;border-radius:50%;background:var(--line);display:inline-block;margin-right:6px}
.exec.on .dot{background:var(--ok);box-shadow:0 0 6px var(--ok)}
.exec.off .dot{background:var(--bad)}
.exec .m{color:var(--mut);font-size:10px;text-align:right}
.wrap{display:grid;grid-template-columns:2fr 1fr;gap:14px;padding:12px 20px 16px}
@media(max-width:1100px){.wrap{grid-template-columns:1fr}}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;
padding:12px 14px}
.panel h2{margin:0 0 10px;font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--mut)}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);
vertical-align:top;font-size:12px}
th{color:var(--mut);font-weight:500}
tbody tr{cursor:pointer}tbody tr:hover td{background:#1c2029}
td.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px}
.badge{display:inline-block;padding:1px 7px;border-radius:20px;font-size:11px;
background:#1c2029;border:1px solid var(--line)}
.st-new{color:#8b93a7}.st-ready{color:var(--blue)}.st-in_progress,.st-verifying,.st-merge{color:var(--warn)}
.st-done{color:var(--ok)}.st-review{color:var(--warn)}.st-blocked{color:var(--bad)}
pre.ev{font-family:ui-monospace,Menlo,monospace;font-size:11px;margin:2px 0;
white-space:pre-wrap;color:var(--mut)}
/* ── modal ── */
.overlay{position:fixed;inset:0;background:rgba(5,8,14,.72);z-index:50;display:none;
align-items:flex-start;justify-content:center;overflow:auto;padding:36px 16px}
.overlay.open{display:flex}
.modal{background:var(--panel);border:1px solid var(--line);border-radius:14px;
width:100%;max-width:880px;box-shadow:0 18px 60px rgba(0,0,0,.5)}
.modal .mhead{display:flex;align-items:baseline;gap:12px;padding:14px 18px;
border-bottom:1px solid var(--line);position:sticky;top:0;background:var(--panel);
border-radius:14px 14px 0 0;z-index:2}
.modal .mhead h3{margin:0;font-size:15px}
.modal .mhead .close{margin-left:auto;cursor:pointer;color:var(--mut);border:1px solid var(--line);
border-radius:7px;padding:2px 9px;font-size:12px}
.modal .mhead .close:hover{color:var(--fg);border-color:var(--mut)}
.modal .mbody{padding:14px 18px 18px}
.tabs{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}
.tab{font-size:11px;padding:3px 10px;border:1px solid var(--line);border-radius:20px;
cursor:pointer;color:var(--mut)}
.tab.on{color:var(--fg);border-color:var(--blue);background:#1c2029}
.meta-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
gap:8px;margin-bottom:14px}
.meta-grid .kv{border:1px solid var(--line);border-radius:8px;padding:6px 9px}
.meta-grid .kv .k{font-size:10px;color:var(--mut);text-transform:uppercase;letter-spacing:.06em}
.meta-grid .kv .v{font-size:12px;margin-top:2px}
.sect{border:1px solid var(--line);border-radius:9px;padding:9px 11px;margin-bottom:10px}
.sect h5{margin:0 0 6px;font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--mut)}
.sect pre{margin:0;font-family:ui-monospace,Menlo,monospace;font-size:11px;line-height:1.5;
white-space:pre-wrap;word-break:break-word;color:var(--fg);max-height:300px;overflow:auto}
.tl{font-family:ui-monospace,Menlo,monospace;font-size:11px}
.tl div{padding:3px 0;border-bottom:1px dashed #20242f;white-space:pre-wrap}
.tl .t{color:var(--blue)}
.tl .a{color:var(--warn)}
.empty{color:var(--mut);font-style:italic}
.vbadge{display:inline-block;margin-right:6px;padding:0 7px;border-radius:20px;font-size:10px;
border:1px solid var(--line)}
.v-pass{color:var(--ok);border-color:var(--ok)}
.v-fail{color:var(--bad);border-color:var(--bad)}
</style></head><body>
<header><h1>openstack-orchestrator</h1>
<span class="live" id="live">connecting…</span>
<span class="hdr-meta" id="hdr-meta"></span></header>
<div class="graph">
<svg class="wires" id="wires"></svg>
<div class="stages" id="stages">
  <div class="stage" data-stage="new"><h4>New <span class="cnt"></span></h4><div class="body"></div></div>
  <div class="stage" data-stage="ready"><h4>Ready <span class="cnt"></span></h4><div class="body"></div></div>
  <div class="stage" data-stage="in_progress"><h4>Implement <span class="cnt"></span></h4><div class="body"></div></div>
  <div class="stage" data-stage="verifying"><h4>Verify <span class="cnt"></span></h4><div class="body"></div></div>
  <div class="stage" data-stage="merge"><h4>Merge <span class="cnt"></span></h4><div class="body"></div></div>
  <div class="stage done-col" data-stage="done"><h4>Done <span class="cnt"></span></h4><div class="body"></div></div>
</div>
</div>
<div class="blocked-lane" id="blocked"></div>
<div class="execs" id="execs"></div>
<div class="wrap">
  <section class="panel">
    <h2>Tasks</h2>
    <table id="tasks"><thead><tr><th>ID</th><th>Proj</th><th>Status</th>
    <th>P</th><th>Slot</th><th>Retry</th></tr></thead><tbody></tbody></table>
  </section>
  <section class="panel">
    <h2>Event log</h2>
    <div id="events"></div>
  </section>
</div>

<div class="overlay" id="overlay">
  <div class="modal">
    <div class="mhead">
      <h3 id="m-title">—</h3>
      <span id="m-badge"></span>
      <span class="close" onclick="closeModal()">✕ esc</span>
    </div>
    <div class="mbody">
      <div class="meta-grid" id="m-meta"></div>
      <div class="tabs" id="m-tabs"></div>
      <div id="m-content"></div>
    </div>
  </div>
</div>

<script>
const $=s=>document.querySelector(s);
function esc(s){return String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}
const badge=s=>`<span class="badge st-${esc(s)}">${esc(s)}</span>`;
const cls=s=>({new:'new',ready:'new',in_progress:'working',verifying:'working',merge:'working',
  done:'done',blocked:'blocked'})[s]||'new';
const EGRESS={'01':'warp IPv6','02':'warp IPv6','03':'ProtonVPN'};
/* ── timezone-aware display: every server timestamp (UTC ISO) → viewer local ── */
const TZ=Intl.DateTimeFormat().resolvedOptions().timeZone;
function fmtT(s){if(!s)return'';const d=new Date(s);if(isNaN(d))return String(s);
  return d.toLocaleString(undefined,{year:'numeric',month:'short',day:'2-digit',
  hour:'2-digit',minute:'2-digit',second:'2-digit'});}
function fmtTz(s){return fmtT(s)+''}
let stageEls={};
function render(d){
  const tasks=d.tasks||[];const slots=d.slots||{};
  stageEls={};document.querySelectorAll('#stages .stage').forEach(s=>stageEls[s.dataset.stage]=s);
  Object.values(stageEls).forEach(s=>{s.querySelector('.body').innerHTML='';s.classList.remove('active')});
  const blockedEl=$('#blocked');blockedEl.innerHTML='<span class="lbl">Blocked</span>';
  const counts={};
  tasks.forEach(t=>{
    counts[t.status]=(counts[t.status]||0)+1;
    let chip=`<div class="chip ${cls(t.status)}" onclick="openModal('${esc(t.id)}')"><b>${esc(t.id)}</b>
      <span class="sub">${esc(t.priority||'')} · ${esc(t.project||'')}</span>`;
    if(t.status==='blocked'&&t.block_reason) chip+=`<span class="sub">${esc(String(t.block_reason).slice(0,70))}</span>`;
    if(t.slot) chip+=`<span class="sub">slot ${esc(t.slot)} · claimed ${esc(fmtT(t.claimed_at))}</span>`;
    chip+='</div>';
    if(t.status==='blocked'){blockedEl.insertAdjacentHTML('beforeend',chip);return}
    const st=stageEls[t.status]||stageEls['new'];
    st.querySelector('.body').insertAdjacentHTML('beforeend',chip);
    if(['in_progress','verifying','merge'].includes(t.status)) st.classList.add('active');
  });
  document.querySelectorAll('#stages .stage').forEach(s=>{
    const n=counts[s.dataset.stage]||0;
    s.querySelector('.cnt').textContent=n?`· ${n}`:'';
  });
  const ex=$('#execs');ex.innerHTML='';
  ['01','02','03'].forEach(k=>{
    const b=slots[k];
    const on=b&&b.online;
    const meta=on?(b.task?`${esc(b.task)} · ${esc(b.phase)}`:'idle · '+esc(b.model||''))
                 :(b?`offline ${b.age_s}s`:EGRESS[k]||'idle');
    ex.insertAdjacentHTML('beforeend',`<div class="exec ${on?'on':(b?'off':'')}">
      <span><span class="dot"></span>executor-${k}</span>
      <span class="m">${meta}</span></div>`);
  });
  $('#tasks tbody').innerHTML=tasks.map(t=>`<tr onclick="openModal('${esc(t.id)}')">
    <td class="mono">${esc(t.id)}</td><td>${esc(t.project)}</td>
    <td>${badge(t.status)}</td><td>${esc(t.priority)}</td>
    <td>${esc(t.slot||'')}</td><td>${t.retry_count}/${t.max_retries}</td></tr>`).join('');
  $('#events').innerHTML=(d.events||[]).slice(0,50).map(e=>
    `<pre class="ev">${esc(fmtT(e.at))} ${esc(e.event)} ${esc(e.task||'')} ${esc((e.detail||'').slice(0,120))}</pre>`).join('');
  $('#live').textContent='live';
  $('#hdr-meta').textContent=`${tasks.length} tasks · ${Object.values(slots).filter(b=>b.online).length}/3 executors online · times in ${TZ}`;
  drawWires();
}
function drawWires(){
  const svg=$('#wires');const graph=svg.parentElement,gr=graph.getBoundingClientRect();
  svg.setAttribute('viewBox',`0 0 ${gr.width} ${gr.height}`);
  let out='';
  const stages=[...document.querySelectorAll('#stages .stage')];
  for(let i=0;i<stages.length-1;i++){
    const a=stages[i].getBoundingClientRect(),b=stages[i+1].getBoundingClientRect();
    const x1=a.right-gr.left,x2=b.left-gr.left;
    const y1=a.top-gr.top+a.height/2,y2=b.top-gr.top+b.height/2;
    const mid=(x1+x2)/2;
    const hot=stages[i].querySelector('.chip')&&stages[i+1].querySelector('.chip')
      &&!stages[i+1].classList.contains('done-col');
    out+=`<path class="${hot?'hot':''}" d="M${x1} ${y1} C${mid} ${y1} ${mid} ${y2} ${x2} ${y2}"/>`;
    out+=`<circle cx="${x2}" cy="${y2}" r="2.5" fill="var(--line)"/>`;
  }
  const impl=stageEls['in_progress'];const bl=$('#blocked');
  if(impl&&bl&&bl.getBoundingClientRect().height>0){
    const ir=impl.getBoundingClientRect();
    const sx=ir.left-gr.left+ir.width/2,sy=ir.bottom-gr.top;
    const by=bl.getBoundingClientRect().top-gr.top-6;
    out+=`<path d="M${sx} ${sy} L${sx} ${by}" stroke="var(--bad)" stroke-dasharray="3 4" fill="none" stroke-width="1"/>`;
  }
  svg.innerHTML=out;
}
/* ── task detail modal ── */
let CUR=null;
async function openModal(id){
  CUR=id;
  $('#overlay').classList.add('open');
  $('#m-title').textContent=id+' …';
  $('#m-badge').innerHTML='';$('#m-meta').innerHTML='';$('#m-tabs').innerHTML='';$('#m-content').innerHTML='';
  try{
    const d=await(await fetch('/api/task/'+encodeURIComponent(id))).json();
    if(CUR!==id)return;
    renderModal(d);
  }catch(e){ $('#m-content').innerHTML='<div class="empty">failed to load: '+esc(e)+'</div>'; }
}
function renderModal(d){
  const t=d.task,sec=d.sections||{};
  $('#m-title').textContent=`${t.id} — ${t.project}`;
  $('#m-badge').innerHTML=badge(t.status)+(t.slot?` <span class="badge">slot ${esc(t.slot)}</span>`:'');
  let ver='';
  try{const v=JSON.parse(t.verification||'{}');
    ver=Object.entries(v).filter(([k])=>['lint','build','types'].includes(k))
      .map(([k,vv])=>`<span class="vbadge v-${esc(vv)}">${esc(k)}: ${esc(vv)}</span>`).join('');
  }catch(e){}
  $('#m-meta').innerHTML=[
    ['priority',t.priority],['type',t.type],['created',fmtT(t.created)],
    ['claimed',fmtT(t.claimed_at)],['retry',`${t.retry_count}/${t.max_retries}`],
    ['last file change',d.file?fmtT(new Date(d.file.mtime*1000).toISOString()):'—'],
  ].map(([k,v])=>`<div class="kv"><div class="k">${k}</div><div class="v">${esc(v??'—')}</div></div>`).join('')
   +(ver?`<div class="kv"><div class="k">verification</div><div class="v">${ver}</div></div>`:'');
  const tabs=[];
  if(sec.plan)tabs.push(['Plan','plan']);
  if(sec.acceptance)tabs.push(['Acceptance criteria','acceptance']);
  if(sec.worklog)tabs.push(['Work log','worklog']);
  if(sec.analysis)tabs.push(['Analysis','analysis']);
  if(sec.questions)tabs.push(['Questions','questions']);
  if(sec.user_report)tabs.push(['User report','user_report']);
  tabs.push(['Timeline','timeline']);
  if(t.block_reason&&t.block_reason!=='null')tabs.push(['Block reason','block']);
  $('#m-tabs').innerHTML=tabs.map(([lbl,key],i)=>
    `<span class="tab${i===0?' on':''}" data-tab="${key}" onclick="showTab('${key}')">${lbl}</span>`).join('');
  window._tabdata={};
  tabs.forEach(([lbl,key])=>{
    if(key==='timeline'){
      const ev=(d.events||[]).map(e=>`<div><span class="t">${esc(fmtT(e.at))}</span>  <b>${esc(e.event)}</b>  ${esc((e.detail||'').slice(0,220))}</div>`);
      const au=(d.audit||[]).map(a=>`<div><span class="t">${esc(fmtT(a.at))}</span>  <span class="a">[${esc(a.slot||'?')}] ${esc(a.action)}</span>  ${esc((a.detail||'').slice(0,220))}</div>`);
      window._tabdata[key]=(ev.length||au.length)?`<div class="tl">${au.join('')}${ev.join('')}</div>`
        :'<div class="empty">no timeline entries</div>';
    }else if(key==='block'){
      window._tabdata[key]=`<div class="sect"><pre>${esc(t.block_reason)}</pre></div>`;
    }else{
      window._tabdata[key]=sec[key]?`<div class="sect"><pre>${esc(sec[key])}</pre></div>`
        :'<div class="empty">not present in task file</div>';
    }
  });
  showTab(tabs[0][1]);
}
function showTab(key){
  document.querySelectorAll('#m-tabs .tab').forEach(el=>el.classList.toggle('on',el.dataset.tab===key));
  $('#m-content').innerHTML=window._tabdata[key]||'';
}
function closeModal(){$('#overlay').classList.remove('open');CUR=null}
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeModal()});
$('#overlay').addEventListener('click',e=>{if(e.target.id==='overlay')closeModal()});
async function load(){try{const d=await(await fetch('/api/snapshot')).json();render(d)}catch(e){}}
load();setInterval(load,5000);
const es=new EventSource('/events');
es.onmessage=load;es.onerror=()=>{$('#live').textContent='reconnecting…'};
</script></body></html>"""


@app.on_event("startup")
def _startup():
    db_init()
    sync_tasks()
    NOTIFY_DIR.mkdir(parents=True, exist_ok=True)
    PIPELINE_LOG.parent.mkdir(parents=True, exist_ok=True)
    # Start Laya warmer to keep model loaded
    start_laya_warmer()


@app.on_event("shutdown")
def _shutdown():
    stop_laya_warmer()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "3080")))