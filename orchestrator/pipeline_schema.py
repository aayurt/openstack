"""Pipeline JSON schema and types for the visual pipeline editor."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Node types
# ---------------------------------------------------------------------------

class NodeType(str, Enum):
    TASK_INPUT = "task.input"
    LAYA_CLASSIFY = "laya.classify"
    LAYA_TRIAGE = "laya.triage"
    TASK_READINESS = "task.readiness"
    WORKER_ROUTER = "worker.router"
    OPENCODE_WORKER = "opencode.worker"
    TEST = "test"
    RETRY = "control.retry"
    CONDITION = "control.condition"
    JOIN = "control.join"
    HERMES_REPLAN = "hermes.replan"
    DONE = "sink.done"


# ---------------------------------------------------------------------------
# Execution states
# ---------------------------------------------------------------------------

class NodeStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class PipelineStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PipelineNode:
    id: str
    type: NodeType
    config: dict[str, Any] = field(default_factory=dict)
    position: dict[str, float] = field(default_factory=lambda: {"x": 0, "y": 0})
    label: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["type"] = self.type.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> PipelineNode:
        return cls(
            id=d["id"],
            type=NodeType(d["type"]),
            config=d.get("config", {}),
            position=d.get("position", {"x": 0, "y": 0}),
            label=d.get("label", ""),
        )


@dataclass
class PipelineEdge:
    source: str
    target: str
    source_port: str = "output"
    target_port: str = "input"
    condition: str | None = None

    def to_dict(self) -> dict:
        d = {
            "source": self.source,
            "target": self.target,
            "source_port": self.source_port,
            "target_port": self.target_port,
        }
        if self.condition:
            d["condition"] = self.condition
        return d

    @classmethod
    def from_dict(cls, d: dict) -> PipelineEdge:
        return cls(
            source=d["source"],
            target=d["target"],
            source_port=d.get("source_port", "output"),
            target_port=d.get("target_port", "input"),
            condition=d.get("condition"),
        )


@dataclass
class PipelineDefinition:
    pipeline: str
    version: int
    nodes: list[PipelineNode]
    edges: list[PipelineEdge]
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "version": self.version,
            "description": self.description,
            "metadata": self.metadata,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, d: dict) -> PipelineDefinition:
        return cls(
            pipeline=d["pipeline"],
            version=d.get("version", 1),
            nodes=[PipelineNode.from_dict(n) for n in d.get("nodes", [])],
            edges=[PipelineEdge.from_dict(e) for e in d.get("edges", [])],
            description=d.get("description", ""),
            metadata=d.get("metadata", {}),
        )

    @classmethod
    def from_json(cls, s: str) -> PipelineDefinition:
        return cls.from_dict(json.loads(s))

    def content_hash(self) -> str:
        return hashlib.sha256(self.to_json().encode()).hexdigest()[:16]

    def node_ids(self) -> set[str]:
        return {n.id for n in self.nodes}

    def validate(self) -> list[str]:
        errors = []
        ids = self.node_ids()
        for e in self.edges:
            if e.source not in ids:
                errors.append(f"Edge source '{e.source}' not found in nodes")
            if e.target not in ids:
                errors.append(f"Edge target '{e.target}' not found in nodes")
        if len(ids) != len(self.nodes):
            errors.append("Duplicate node IDs")
        seen = set()
        for n in self.nodes:
            if n.id in seen:
                errors.append(f"Duplicate node id: {n.id}")
            seen.add(n.id)
        return errors


# ---------------------------------------------------------------------------
# Node type metadata (for the visual editor)
# ---------------------------------------------------------------------------

NODE_CATEGORIES: dict[str, list[dict[str, str]]] = {
    "Input": [
        {"type": NodeType.TASK_INPUT.value, "label": "Task Input", "color": "#4CAF50"},
    ],
    "Laya": [
        {"type": NodeType.LAYA_CLASSIFY.value, "label": "Laya Classify", "color": "#FF9800"},
        {"type": NodeType.LAYA_TRIAGE.value, "label": "Laya Triage", "color": "#FF9800"},
    ],
    "Execution": [
        {"type": NodeType.TASK_READINESS.value, "label": "Readiness Check", "color": "#2196F3"},
        {"type": NodeType.WORKER_ROUTER.value, "label": "Worker Router", "color": "#2196F3"},
        {"type": NodeType.OPENCODE_WORKER.value, "label": "OpenCode Worker", "color": "#9C27B0"},
    ],
    "Validation": [
        {"type": NodeType.TEST.value, "label": "Tests", "color": "#00BCD4"},
    ],
    "Control": [
        {"type": NodeType.RETRY.value, "label": "Retry", "color": "#F44336"},
        {"type": NodeType.CONDITION.value, "label": "Condition", "color": "#607D8B"},
        {"type": NodeType.JOIN.value, "label": "Join", "color": "#607D8B"},
    ],
    "Hermes": [
        {"type": NodeType.HERMES_REPLAN.value, "label": "Hermes Re-plan", "color": "#E91E63"},
    ],
    "Output": [
        {"type": NodeType.DONE.value, "label": "Done", "color": "#4CAF50"},
    ],
}

NODE_PORT_MAP: dict[str, dict[str, list[str]]] = {
    NodeType.TASK_INPUT.value: {"inputs": [], "outputs": ["output"]},
    NodeType.LAYA_CLASSIFY.value: {"inputs": ["input"], "outputs": ["output"]},
    NodeType.LAYA_TRIAGE.value: {"inputs": ["input"], "outputs": ["pass", "retry", "escalate"]},
    NodeType.TASK_READINESS.value: {"inputs": ["input"], "outputs": ["ready", "blocked"]},
    NodeType.WORKER_ROUTER.value: {"inputs": ["input"], "outputs": ["output"]},
    NodeType.OPENCODE_WORKER.value: {"inputs": ["input"], "outputs": ["output"]},
    NodeType.TEST.value: {"inputs": ["input"], "outputs": ["pass", "fail"]},
    NodeType.RETRY.value: {"inputs": ["input"], "outputs": ["output"]},
    NodeType.CONDITION.value: {"inputs": ["input"], "outputs": ["true", "false"]},
    NodeType.JOIN.value: {"inputs": ["input_a", "input_b"], "outputs": ["output"]},
    NodeType.HERMES_REPLAN.value: {"inputs": ["input"], "outputs": ["output"]},
    NodeType.DONE.value: {"inputs": ["input"], "outputs": []},
}


# ---------------------------------------------------------------------------
# Default pipeline
# ---------------------------------------------------------------------------

def default_pipeline() -> PipelineDefinition:
    return PipelineDefinition(
        pipeline="coding-task",
        version=1,
        description="Default coding task pipeline: classify → route → execute → test → triage",
        nodes=[
            PipelineNode(id="input", type=NodeType.TASK_INPUT, position={"x": 50, "y": 200}, label="Task Input"),
            PipelineNode(id="classify", type=NodeType.LAYA_CLASSIFY, position={"x": 250, "y": 200}, label="Laya Classify"),
            PipelineNode(id="readiness", type=NodeType.TASK_READINESS, position={"x": 450, "y": 200}, label="Readiness"),
            PipelineNode(id="router", type=NodeType.WORKER_ROUTER, position={"x": 650, "y": 200}, label="Worker Router"),
            PipelineNode(id="execute", type=NodeType.OPENCODE_WORKER, position={"x": 850, "y": 200}, label="OpenCode"),
            PipelineNode(id="tests", type=NodeType.TEST, position={"x": 1050, "y": 200}, label="Tests"),
            PipelineNode(id="triage", type=NodeType.LAYA_TRIAGE, position={"x": 1250, "y": 200}, label="Laya Triage"),
            PipelineNode(id="done", type=NodeType.DONE, position={"x": 1450, "y": 100}, label="Done"),
            PipelineNode(id="retry_backoff", type=NodeType.RETRY, position={"x": 1450, "y": 250}, label="Retry"),
            PipelineNode(id="hermes", type=NodeType.HERMES_REPLAN, position={"x": 1450, "y": 400}, label="Hermes Re-plan"),
        ],
        edges=[
            PipelineEdge(source="input", target="classify"),
            PipelineEdge(source="classify", target="readiness"),
            PipelineEdge(source="readiness", target="router", source_port="ready"),
            PipelineEdge(source="router", target="execute"),
            PipelineEdge(source="execute", target="tests"),
            PipelineEdge(source="tests", target="triage", source_port="pass"),
            PipelineEdge(source="triage", target="done", source_port="pass"),
            PipelineEdge(source="triage", target="retry_backoff", source_port="retry"),
            PipelineEdge(source="triage", target="hermes", source_port="escalate"),
            PipelineEdge(source="retry_backoff", target="execute"),
        ],
    )
