"""Pipeline API routes — FastAPI router for pipeline CRUD and execution."""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from pipeline_schema import (
    PipelineDefinition,
    PipelineNode,
    PipelineEdge,
    NodeType,
    NODE_CATEGORIES,
    NODE_PORT_MAP,
    default_pipeline,
)
from pipeline_runtime import (
    PipelineRuntime,
    PipelineExecution,
    PipelineStatus,
    init_pipeline_tables,
    save_pipeline,
    load_pipeline,
    list_pipelines,
    save_execution,
    load_execution,
    list_executions,
)

log = logging.getLogger("pipeline_routes")

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class PipelineSaveReq(BaseModel):
    pipeline: str = "coding-task"
    version: int = 1
    description: str = ""
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {}


class PipelineRunReq(BaseModel):
    pipeline_id: str | None = None
    task_id: str
    task_data: dict[str, Any] = {}


class NodeUpdateReq(BaseModel):
    config: dict[str, Any] = {}
    position: dict[str, float] = {}
    label: str = ""


class EdgeAddReq(BaseModel):
    source: str
    target: str
    source_port: str = "output"
    target_port: str = "input"
    condition: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_db() -> sqlite3.Connection:
    """Get a DB connection — injected at runtime via closure or module var."""
    import os
    from pathlib import Path
    data_dir = Path(os.environ.get("ORCH_DATA", "/data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(data_dir / "orchestrator.db"), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


# ---------------------------------------------------------------------------
# Pipeline CRUD
# ---------------------------------------------------------------------------

@router.get("/categories")
def get_categories() -> dict:
    """Return node categories and port definitions for the visual editor."""
    return {"categories": NODE_CATEGORIES, "ports": NODE_PORT_MAP}


@router.get("/default")
def get_default() -> dict:
    """Return the default pipeline definition."""
    return default_pipeline().to_dict()


@router.get("/list")
def api_list_pipelines() -> dict:
    """List all saved pipelines."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)
        pipelines = list_pipelines(conn)
        return {"pipelines": pipelines}
    finally:
        conn.close()


@router.post("/save")
def api_save_pipeline(req: PipelineSaveReq) -> dict:
    """Save a pipeline definition."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)
        nodes = [PipelineNode.from_dict(n) for n in req.nodes]
        edges = [PipelineEdge.from_dict(e) for e in req.edges]
        pipeline = PipelineDefinition(
            pipeline=req.pipeline,
            version=req.version,
            description=req.description,
            nodes=nodes,
            edges=edges,
            metadata=req.metadata,
        )
        errors = pipeline.validate()
        if errors:
            raise HTTPException(status_code=400, detail={"errors": errors})
        pid = save_pipeline(conn, pipeline)
        return {"id": pid, "content_hash": pipeline.content_hash()}
    finally:
        conn.close()


@router.get("/get/{pipeline_id}")
def api_get_pipeline(pipeline_id: str) -> dict:
    """Get a pipeline definition by ID."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)
        pipeline = load_pipeline(conn, pipeline_id)
        if pipeline is None:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        return pipeline.to_dict()
    finally:
        conn.close()


@router.delete("/delete/{pipeline_id}")
def api_delete_pipeline(pipeline_id: str) -> dict:
    """Delete a pipeline definition."""
    conn = _get_db()
    try:
        conn.execute("DELETE FROM pipelines WHERE id = ?", (pipeline_id,))
        conn.commit()
        return {"deleted": pipeline_id}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Node operations
# ---------------------------------------------------------------------------

@router.post("/node/add")
def api_add_node(pipeline_id: str, node_type: str, node_id: str, x: float = 0, y: float = 0) -> dict:
    """Add a node to an existing pipeline."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)
        pipeline = load_pipeline(conn, pipeline_id)
        if pipeline is None:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        if node_id in pipeline.node_ids():
            raise HTTPException(status_code=400, detail=f"Node '{node_id}' already exists")
        try:
            nt = NodeType(node_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid node type: {node_type}")
        new_node = PipelineNode(id=node_id, type=nt, position={"x": x, "y": y})
        pipeline.nodes.append(new_node)
        save_pipeline(conn, pipeline)
        return pipeline.to_dict()
    finally:
        conn.close()


@router.post("/node/update")
def api_update_node(pipeline_id: str, node_id: str, req: NodeUpdateReq) -> dict:
    """Update a node's config, position, or label."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)
        pipeline = load_pipeline(conn, pipeline_id)
        if pipeline is None:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        for n in pipeline.nodes:
            if n.id == node_id:
                if req.config:
                    n.config = req.config
                if req.position:
                    n.position = req.position
                if req.label:
                    n.label = req.label
                break
        else:
            raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
        save_pipeline(conn, pipeline)
        return pipeline.to_dict()
    finally:
        conn.close()


@router.post("/node/delete")
def api_delete_node(pipeline_id: str, node_id: str) -> dict:
    """Remove a node and its connected edges."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)
        pipeline = load_pipeline(conn, pipeline_id)
        if pipeline is None:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        pipeline.nodes = [n for n in pipeline.nodes if n.id != node_id]
        pipeline.edges = [e for e in pipeline.edges if e.source != node_id and e.target != node_id]
        save_pipeline(conn, pipeline)
        return pipeline.to_dict()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Edge operations
# ---------------------------------------------------------------------------

@router.post("/edge/add")
def api_add_edge(pipeline_id: str, req: EdgeAddReq) -> dict:
    """Add an edge between two nodes."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)
        pipeline = load_pipeline(conn, pipeline_id)
        if pipeline is None:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        ids = pipeline.node_ids()
        if req.source not in ids:
            raise HTTPException(status_code=400, detail=f"Source node '{req.source}' not found")
        if req.target not in ids:
            raise HTTPException(status_code=400, detail=f"Target node '{req.target}' not found")
        pipeline.edges.append(PipelineEdge(
            source=req.source,
            target=req.target,
            source_port=req.source_port,
            target_port=req.target_port,
            condition=req.condition,
        ))
        save_pipeline(conn, pipeline)
        return pipeline.to_dict()
    finally:
        conn.close()


@router.post("/edge/delete")
def api_delete_edge(pipeline_id: str, source: str, target: str) -> dict:
    """Remove an edge."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)
        pipeline = load_pipeline(conn, pipeline_id)
        if pipeline is None:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        pipeline.edges = [
            e for e in pipeline.edges
            if not (e.source == source and e.target == target)
        ]
        save_pipeline(conn, pipeline)
        return pipeline.to_dict()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Pipeline validation
# ---------------------------------------------------------------------------

@router.post("/validate")
def api_validate_pipeline(req: PipelineSaveReq) -> dict:
    """Validate a pipeline definition without saving."""
    nodes = [PipelineNode.from_dict(n) for n in req.nodes]
    edges = [PipelineEdge.from_dict(e) for e in req.edges]
    pipeline = PipelineDefinition(
        pipeline=req.pipeline,
        version=req.version,
        nodes=nodes,
        edges=edges,
    )
    errors = pipeline.validate()
    return {"valid": len(errors) == 0, "errors": errors}


# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------

@router.post("/run")
def api_run_pipeline(req: PipelineRunReq) -> dict:
    """Execute a pipeline against a task."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)

        # Load pipeline
        if req.pipeline_id:
            pipeline = load_pipeline(conn, req.pipeline_id)
            if pipeline is None:
                raise HTTPException(status_code=404, detail="Pipeline not found")
        else:
            pipeline = default_pipeline()

        task_data = req.task_data
        task_data["id"] = req.task_id

        runtime = PipelineRuntime(pipeline, task_data, db_conn=conn)
        execution = runtime.run()
        save_execution(conn, execution)

        return execution.to_dict()
    finally:
        conn.close()


@router.post("/step")
def api_step_pipeline(req: PipelineRunReq, node_id: str) -> dict:
    """Execute a single node in a pipeline (for debugging/visualization)."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)

        if req.pipeline_id:
            pipeline = load_pipeline(conn, req.pipeline_id)
            if pipeline is None:
                raise HTTPException(status_code=404, detail="Pipeline not found")
        else:
            pipeline = default_pipeline()

        task_data = req.task_data
        task_data["id"] = req.task_id

        runtime = PipelineRuntime(pipeline, task_data, db_conn=conn)
        result = runtime.execute_node(node_id)
        save_execution(conn, runtime.execution)

        return {
            "execution": runtime.execution.to_dict(),
            "node_result": result,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Execution state
# ---------------------------------------------------------------------------

@router.get("/executions")
def api_list_executions(task_id: str | None = None, limit: int = 50) -> dict:
    """List pipeline executions."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)
        executions = list_executions(conn, task_id=task_id, limit=limit)
        return {"executions": executions}
    finally:
        conn.close()


@router.get("/execution/{execution_id}")
def api_get_execution(execution_id: str) -> dict:
    """Get execution state by ID."""
    conn = _get_db()
    try:
        init_pipeline_tables(conn)
        execution = load_execution(conn, execution_id)
        if execution is None:
            raise HTTPException(status_code=404, detail="Execution not found")
        return execution.to_dict()
    finally:
        conn.close()
