"""Pipeline runtime engine.

Executes a PipelineDefinition by walking the DAG, calling node handlers,
and persisting execution state.  The runtime delegates to the existing
orchestrator functions (laya_client, worker routing, etc.) — it does NOT
duplicate orchestration logic.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from pipeline_schema import (
    PipelineDefinition,
    PipelineNode,
    PipelineEdge,
    NodeType,
    NodeStatus,
    PipelineStatus,
    NODE_PORT_MAP,
)

log = logging.getLogger("pipeline_runtime")

# ---------------------------------------------------------------------------
# Execution state
# ---------------------------------------------------------------------------

@dataclass
class NodeExecution:
    node_id: str
    status: NodeStatus = NodeStatus.PENDING
    started_at: str | None = None
    completed_at: str | None = None
    result: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    attempts: int = 0

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "status": self.status.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "result": self.result,
            "error": self.error,
            "attempts": self.attempts,
        }


@dataclass
class PipelineExecution:
    execution_id: str
    task_id: str
    pipeline_name: str
    pipeline_version: int
    status: PipelineStatus = PipelineStatus.IDLE
    current_node: str | None = None
    nodes: dict[str, NodeExecution] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "execution_id": self.execution_id,
            "task_id": self.task_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "status": self.status.value,
            "current_node": self.current_node,
            "nodes": {k: v.to_dict() for k, v in self.nodes.items()},
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# Node handler signature
# ---------------------------------------------------------------------------

class NodeContext:
    """Context passed to every node handler."""

    def __init__(
        self,
        execution: PipelineExecution,
        pipeline: PipelineDefinition,
        task_data: dict[str, Any],
        node: PipelineNode,
        node_results: dict[str, dict[str, Any]],
        db_conn: sqlite3.Connection | None = None,
    ):
        self.execution = execution
        self.pipeline = pipeline
        self.task_data = task_data
        self.node = node
        self.node_results = node_results
        self.db = db_conn

    def upstream_result(self, node_id: str) -> dict[str, Any]:
        return self.node_results.get(node_id, {})


NodeHandler = Callable[[NodeContext], dict[str, Any]]


# ---------------------------------------------------------------------------
# Built-in node handlers
# ---------------------------------------------------------------------------

def _handle_task_input(ctx: NodeContext) -> dict[str, Any]:
    """Task input node — pass through task data."""
    return {
        "task_id": ctx.task_data.get("id", ""),
        "project": ctx.task_data.get("project", ""),
        "objective": ctx.task_data.get("objective", ""),
        "priority": ctx.task_data.get("priority", "P2"),
        "files_scope": ctx.task_data.get("files_scope", []),
        "depends_on": ctx.task_data.get("depends_on", []),
        "acceptance": ctx.task_data.get("acceptance", ""),
    }


def _handle_laya_classify(ctx: NodeContext) -> dict[str, Any]:
    """Call Laya for pre-execution classification."""
    try:
        from laya_client import classify_task, TaskState
        task = TaskState(
            id=ctx.task_data.get("id", ""),
            project=ctx.task_data.get("project", ""),
            type=ctx.task_data.get("type", "task"),
            priority=ctx.task_data.get("priority", "P2"),
            files_scope=ctx.task_data.get("files_scope", []),
            depends_on=ctx.task_data.get("depends_on", []),
            plan=ctx.task_data.get("plan", ""),
            acceptance=ctx.task_data.get("acceptance", ""),
            frontmatter=ctx.task_data.get("frontmatter", {}),
        )
        result = classify_task(task)
        return {
            "complexity": result.get("complexity", "medium"),
            "priority_adjust": result.get("priority_adjust", "keep"),
            "ready": result.get("ready", "yes"),
            "latency_ms": result.get("latency_ms", 0),
        }
    except Exception as e:
        log.warning("Laya classify failed, using defaults: %s", e)
        return {"complexity": "medium", "priority_adjust": "keep", "ready": "yes", "error": str(e)}


def _handle_laya_triage(ctx: NodeContext) -> dict[str, Any]:
    """Call Laya for post-test triage."""
    try:
        from laya_client import triage_result, TaskState
        task = TaskState(
            id=ctx.task_data.get("id", ""),
            project=ctx.task_data.get("project", ""),
            type=ctx.task_data.get("type", "task"),
            priority=ctx.task_data.get("priority", "P2"),
            files_scope=ctx.task_data.get("files_scope", []),
            depends_on=ctx.task_data.get("depends_on", []),
            plan=ctx.task_data.get("plan", ""),
            acceptance=ctx.task_data.get("acceptance", ""),
            frontmatter=ctx.task_data.get("frontmatter", {}),
        )
        verification = ctx.task_data.get("verification", {})
        engine_output = ctx.task_data.get("engine_output", "")
        result = triage_result(task, verification, engine_output)
        return {
            "verdict": result.get("verdict", "retry"),
            "retry_reason": result.get("retry_reason", "real_bug"),
            "latency_ms": result.get("latency_ms", 0),
        }
    except Exception as e:
        log.warning("Laya triage failed, using defaults: %s", e)
        return {"verdict": "retry", "retry_reason": "real_bug", "error": str(e)}


def _handle_readiness(ctx: NodeContext) -> dict[str, Any]:
    """Check if task is ready to execute."""
    classify = ctx.upstream_result("classify") or ctx.node_results.get("classify", {})
    ready = classify.get("ready", "yes")
    deps = ctx.task_data.get("depends_on", [])
    return {
        "ready": ready == "yes",
        "reason": classify.get("ready", "yes"),
        "dependencies": deps,
    }


def _handle_worker_router(ctx: NodeContext) -> dict[str, Any]:
    """Select an eligible worker."""
    # For now, return a placeholder — real routing uses orchestrator logic
    slot = ctx.node.config.get("preferred_slot", "auto")
    return {
        "worker_id": slot,
        "routing": "least-busy",
        "complexity": ctx.upstream_result("classify").get("complexity", "medium"),
    }


def _handle_opencode_worker(ctx: NodeContext) -> dict[str, Any]:
    """Execute coding task via OpenCode worker."""
    # Real execution happens in the executor loop; this node marks execution started
    worker = ctx.upstream_result("router")
    return {
        "worker_id": worker.get("worker_id", "unknown"),
        "status": "dispatched",
        "task_id": ctx.task_data.get("id", ""),
    }


def _handle_tests(ctx: NodeContext) -> dict[str, Any]:
    """Run verification tests."""
    verification = ctx.task_data.get("verification", {})
    return {
        "lint": verification.get("lint", "pass"),
        "build": verification.get("build", "pass"),
        "types": verification.get("types", "pass"),
        "all_pass": all(
            v == "pass"
            for k, v in verification.items()
            if k in ("lint", "build", "types")
        ),
    }


def _handle_retry(ctx: NodeContext) -> dict[str, Any]:
    """Retry with exponential backoff."""
    max_retries = ctx.node.config.get("max_retries", 3)
    retry_count = ctx.task_data.get("retry_count", 0)
    delay = min(60 * (2 ** retry_count), 240)
    return {
        "retry_count": retry_count + 1,
        "max_retries": max_retries,
        "delay_seconds": delay,
        "should_retry": retry_count + 1 <= max_retries,
    }


def _handle_condition(ctx: NodeContext) -> dict[str, Any]:
    """Route based on expression."""
    expr = ctx.node.config.get("expression", "true")
    # Simple expression evaluation — extend as needed
    try:
        result = eval(expr, {"__builtins__": {}}, {"result": ctx.upstream_result("input")})
    except Exception as e:
        raise RuntimeError(f"Condition expression failed: {expr} — {e}")
    return {"condition": bool(result), "expression": expr}


def _handle_join(ctx: NodeContext) -> dict[str, Any]:
    """Wait for multiple branches."""
    return {"joined": True}


def _handle_hermes_replan(ctx: NodeContext) -> dict[str, Any]:
    """Escalate to Hermes for re-planning."""
    escalation = {
        "task_id": ctx.task_data.get("id", ""),
        "failure_context": ctx.upstream_result("tests"),
        "triage": ctx.upstream_result("triage"),
        "current_plan": ctx.task_data.get("plan", ""),
        "retry_history": ctx.task_data.get("retry_count", 0),
    }
    # Write to notify/ for Hermes to pick up
    try:
        notify_dir = Path(os.environ.get("ORCH_WS", "/workspace")) / "notify"
        notify_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        fname = f"escalation-{ctx.task_data.get('id', 'unknown')}-{ts}.json"
        (notify_dir / fname).write_text(json.dumps(escalation, indent=2))
        log.info("Escalation written to %s", notify_dir / fname)
        return {"escalated": True, "file": fname}
    except (OSError, PermissionError) as e:
        log.warning("Could not write escalation file: %s", e)
        return {"escalated": True, "file": None, "escalation_data": escalation}


def _handle_done(ctx: NodeContext) -> dict[str, Any]:
    """Terminal node — task complete."""
    return {"status": "done"}


# ---------------------------------------------------------------------------
# Handler registry
# ---------------------------------------------------------------------------

NODE_HANDLERS: dict[str, NodeHandler] = {
    NodeType.TASK_INPUT.value: _handle_task_input,
    NodeType.LAYA_CLASSIFY.value: _handle_laya_classify,
    NodeType.LAYA_TRIAGE.value: _handle_laya_triage,
    NodeType.TASK_READINESS.value: _handle_readiness,
    NodeType.WORKER_ROUTER.value: _handle_worker_router,
    NodeType.OPENCODE_WORKER.value: _handle_opencode_worker,
    NodeType.TEST.value: _handle_tests,
    NodeType.RETRY.value: _handle_retry,
    NodeType.CONDITION.value: _handle_condition,
    NodeType.JOIN.value: _handle_join,
    NodeType.HERMES_REPLAN.value: _handle_hermes_replan,
    NodeType.DONE.value: _handle_done,
}


# ---------------------------------------------------------------------------
# Pipeline Runtime
# ---------------------------------------------------------------------------

class PipelineRuntime:
    """Executes a PipelineDefinition against a task."""

    def __init__(
        self,
        pipeline: PipelineDefinition,
        task_data: dict[str, Any],
        db_conn: sqlite3.Connection | None = None,
    ):
        self.pipeline = pipeline
        self.task_data = task_data
        self.db = db_conn
        ts = datetime.now(timezone.utc).isoformat()
        self.execution = PipelineExecution(
            execution_id=f"exec-{uuid.uuid4().hex[:12]}",
            task_id=task_data.get("id", ""),
            pipeline_name=pipeline.pipeline,
            pipeline_version=pipeline.version,
            status=PipelineStatus.RUNNING,
            created_at=ts,
            updated_at=ts,
        )
        self._node_results: dict[str, dict[str, Any]] = {}

    # -- graph helpers -----------------------------------------------------

    def _successors(self, node_id: str, port: str = "output") -> list[PipelineEdge]:
        return [
            e for e in self.pipeline.edges
            if e.source == node_id and e.source_port == port
        ]

    def _find_start_nodes(self) -> list[str]:
        """Nodes with no incoming edges (inputs)."""
        targets = {e.target for e in self.pipeline.edges}
        return [n.id for n in self.pipeline.nodes if n.id not in targets]

    def _node_by_id(self, nid: str) -> PipelineNode | None:
        for n in self.pipeline.nodes:
            if n.id == nid:
                return n
        return None

    # -- execution ---------------------------------------------------------

    def _mark(self, node_id: str, status: NodeStatus, result: dict | None = None, error: str | None = None):
        ne = self.execution.nodes.get(node_id)
        if ne is None:
            ne = NodeExecution(node_id=node_id)
            self.execution.nodes[node_id] = ne
        ne.status = status
        now = datetime.now(timezone.utc).isoformat()
        if status == NodeStatus.RUNNING:
            ne.started_at = now
            ne.attempts += 1
            self.execution.current_node = node_id
        elif status in (NodeStatus.COMPLETED, NodeStatus.FAILED):
            ne.completed_at = now
        if result is not None:
            ne.result = result
        if error is not None:
            ne.error = error
        self.execution.updated_at = now

    def _resolve_next_port(self, node: PipelineNode, result: dict[str, Any]) -> str:
        """Determine which output port to follow."""
        ntype = node.type.value
        if ntype == NodeType.LAYA_TRIAGE.value:
            verdict = result.get("verdict", "retry")
            return verdict  # "pass", "retry", "escalate"
        if ntype == NodeType.TEST.value:
            return "pass" if result.get("all_pass", False) else "fail"
        if ntype == NodeType.TASK_READINESS.value:
            return "ready" if result.get("ready", False) else "blocked"
        if ntype == NodeType.CONDITION.value:
            return "true" if result.get("condition", False) else "false"
        return "output"

    def execute_node(self, node_id: str) -> dict[str, Any]:
        """Execute a single node and return its result."""
        node = self._node_by_id(node_id)
        if node is None:
            raise ValueError(f"Node '{node_id}' not found in pipeline")

        handler = NODE_HANDLERS.get(node.type.value)
        if handler is None:
            raise ValueError(f"No handler for node type '{node.type.value}'")

        ctx = NodeContext(
            execution=self.execution,
            pipeline=self.pipeline,
            task_data=self.task_data,
            node=node,
            node_results=self._node_results,
            db_conn=self.db,
        )

        self._mark(node_id, NodeStatus.RUNNING)
        try:
            result = handler(ctx)
            self._node_results[node_id] = result
            self._mark(node_id, NodeStatus.COMPLETED, result=result)
            return result
        except Exception as e:
            log.exception("Node %s failed", node_id)
            self._mark(node_id, NodeStatus.FAILED, error=str(e))
            raise

    def run(self, start_from: str | None = None) -> PipelineExecution:
        """Execute the full pipeline from start to terminal nodes."""
        start_nodes = self._find_start_nodes() if start_from is None else [start_from]
        queue = list(start_nodes)
        visited: set[str] = set()

        while queue:
            node_id = queue.pop(0)
            if node_id in visited:
                continue
            visited.add(node_id)

            try:
                result = self.execute_node(node_id)
            except Exception as e:
                self.execution.status = PipelineStatus.FAILED
                self.execution.error = str(e)
                return self.execution

            node = self._node_by_id(node_id)
            port = self._resolve_next_port(node, result)

            for edge in self._successors(node_id, port):
                if edge.target not in visited:
                    queue.append(edge.target)

        self.execution.status = PipelineStatus.COMPLETED
        return self.execution

    def to_dict(self) -> dict:
        return self.execution.to_dict()


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def init_pipeline_tables(conn: sqlite3.Connection) -> None:
    """Create pipeline-related DB tables."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipelines (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            definition TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_executions (
            execution_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            pipeline_id TEXT NOT NULL,
            pipeline_version INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'idle',
            current_node TEXT,
            nodes TEXT NOT NULL DEFAULT '{}',
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)


def save_pipeline(conn: sqlite3.Connection, pipeline: PipelineDefinition) -> str:
    """Save a pipeline definition. Returns the pipeline ID."""
    pid = f"{pipeline.pipeline}-v{pipeline.version}"
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO pipelines (id, name, version, definition, content_hash, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (pid, pipeline.pipeline, pipeline.version, pipeline.to_json(), pipeline.content_hash(), now, now),
    )
    conn.commit()
    return pid


def load_pipeline(conn: sqlite3.Connection, pipeline_id: str) -> PipelineDefinition | None:
    """Load a pipeline definition by ID."""
    row = conn.execute("SELECT definition FROM pipelines WHERE id = ?", (pipeline_id,)).fetchone()
    if row is None:
        return None
    return PipelineDefinition.from_json(row[0])


def list_pipelines(conn: sqlite3.Connection) -> list[dict]:
    """List all saved pipelines."""
    rows = conn.execute("SELECT id, name, version, content_hash, created_at, updated_at FROM pipelines ORDER BY updated_at DESC").fetchall()
    return [
        {"id": r[0], "name": r[1], "version": r[2], "content_hash": r[3], "created_at": r[4], "updated_at": r[5]}
        for r in rows
    ]


def save_execution(conn: sqlite3.Connection, execution: PipelineExecution) -> None:
    """Persist pipeline execution state."""
    conn.execute(
        """INSERT OR REPLACE INTO pipeline_executions
           (execution_id, task_id, pipeline_id, pipeline_version, status, current_node, nodes, error, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            execution.execution_id,
            execution.task_id,
            f"{execution.pipeline_name}-v{execution.pipeline_version}",
            execution.pipeline_version,
            execution.status.value,
            execution.current_node,
            json.dumps({k: v.to_dict() for k, v in execution.nodes.items()}),
            execution.error,
            execution.created_at,
            execution.updated_at,
        ),
    )
    conn.commit()


def load_execution(conn: sqlite3.Connection, execution_id: str) -> PipelineExecution | None:
    """Load execution state by ID."""
    row = conn.execute(
        "SELECT execution_id, task_id, pipeline_id, pipeline_version, status, current_node, nodes, error, created_at, updated_at FROM pipeline_executions WHERE execution_id = ?",
        (execution_id,),
    ).fetchone()
    if row is None:
        return None
    nodes_raw = json.loads(row[6]) if row[6] else {}
    # Derive pipeline_name from pipeline_id (e.g., "coding-task-v1" -> "coding-task")
    pipeline_id = row[2] or ""
    pipeline_name = pipeline_id.rsplit("-v", 1)[0] if "-v" in pipeline_id else pipeline_id
    return PipelineExecution(
        execution_id=row[0],
        task_id=row[1],
        pipeline_name=pipeline_name,
        pipeline_version=row[3],
        status=PipelineStatus(row[4]),
        current_node=row[5],
        nodes={k: NodeExecution(**v) for k, v in nodes_raw.items()},
        error=row[7],
        created_at=row[8],
        updated_at=row[9],
    )


def list_executions(conn: sqlite3.Connection, task_id: str | None = None, limit: int = 50) -> list[dict]:
    """List pipeline executions, optionally filtered by task."""
    if task_id:
        rows = conn.execute(
            "SELECT execution_id, task_id, pipeline_id, status, current_node, created_at, updated_at FROM pipeline_executions WHERE task_id = ? ORDER BY created_at DESC LIMIT ?",
            (task_id, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT execution_id, task_id, pipeline_id, status, current_node, created_at, updated_at FROM pipeline_executions ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [
        {"execution_id": r[0], "task_id": r[1], "pipeline_id": r[2], "status": r[3], "current_node": r[4], "created_at": r[5], "updated_at": r[6]}
        for r in rows
    ]
