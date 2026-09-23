"""Laya MCP client for tactical task decisions.

HTTP client for Laya MCP server at http://127.0.0.1:8787.
Provides task classification, readiness checks, and result triage.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

LAYA_URL = "http://127.0.0.1:8787"
REQUEST_TIMEOUT = 10

# Valid values for Laya output validation
VALID_COMPLEXITY = {"trivial", "small", "medium", "large"}
VALID_PRIORITY_ADJUST = {"keep", "escalate", "de-escalate"}
VALID_READY = {"yes", "blocked_missing_info", "blocked_deps"}
VALID_VERDICT = {"pass", "retry", "escalate"}
VALID_RETRY_REASON = {"flaky_test", "real_bug", "env_issue"}


@dataclass
class TaskState:
    """Structured task state for Laya context."""
    id: str
    project: str
    type: str
    priority: str
    files_scope: list[str]
    depends_on: list[str]
    plan: str
    acceptance: str
    frontmatter: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        """Serialize to JSON for Laya state."""
        return json.dumps({
            "id": self.id,
            "project": self.project,
            "type": self.type,
            "priority": self.priority,
            "files_scope": self.files_scope,
            "depends_on": self.depends_on,
            "plan_summary": self.plan[:1000],
            "acceptance_summary": self.acceptance[:500],
        })


def _post(path: str, payload: dict) -> dict:
    """POST to Laya MCP."""
    req = urllib.request.Request(
        f"{LAYA_URL}{path}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as r:
        return json.loads(r.read().decode())


def _get(path: str) -> dict:
    """GET from Laya MCP."""
    with urllib.request.urlopen(f"{LAYA_URL}{path}", timeout=5) as r:
        return json.loads(r.read().decode())


def health() -> dict:
    """Check Laya availability and status."""
    return _get("/health")


def is_available() -> bool:
    """Check if Laya is reachable and loaded."""
    try:
        h = health()
        return h.get("ok") and h.get("loaded")
    except Exception:
        return False


def validate_classify_result(result: dict) -> dict:
    """Validate and sanitize Laya classification output.
    
    Log warning + fall back to defaults on invalid output.
    Don't block execution.
    """
    validated = {
        "complexity": "medium",
        "priority_adjust": "keep",
        "ready": "yes",
        "latency_ms": result.get("latency_ms", 0),
    }
    
    if result.get("complexity") in VALID_COMPLEXITY:
        validated["complexity"] = result["complexity"]
    else:
        logger.warning("Invalid complexity '%s', falling back to 'medium'", result.get("complexity"))
    
    if result.get("priority_adjust") in VALID_PRIORITY_ADJUST:
        validated["priority_adjust"] = result["priority_adjust"]
    else:
        logger.warning("Invalid priority_adjust '%s', falling back to 'keep'", result.get("priority_adjust"))
    
    if result.get("ready") in VALID_READY:
        validated["ready"] = result["ready"]
    else:
        logger.warning("Invalid ready '%s', falling back to 'yes'", result.get("ready"))
    
    return validated


def validate_triage_result(result: dict) -> dict:
    """Validate and sanitize Laya triage output.
    
    Log warning + fall back to defaults on invalid output.
    Don't block execution.
    """
    validated = {
        "verdict": "retry",
        "retry_reason": "real_bug",
        "latency_ms": result.get("latency_ms", 0),
    }
    
    if result.get("verdict") in VALID_VERDICT:
        validated["verdict"] = result["verdict"]
    else:
        logger.warning("Invalid verdict '%s', falling back to 'retry'", result.get("verdict"))
    
    if result.get("retry_reason") in VALID_RETRY_REASON:
        validated["retry_reason"] = result["retry_reason"]
    else:
        logger.warning("Invalid retry_reason '%s', falling back to 'real_bug'", result.get("retry_reason"))
    
    return validated


def classify_task(task: TaskState) -> dict:
    """Pre-execution: classify complexity, priority adjustment, readiness.
    
    Returns:
        dict with keys: complexity, priority_adjust, ready, latency_ms
        Falls back to safe defaults if Laya unavailable.
    """
    state = task.to_json()

    questions = {
        "complexity": {
            "type": "choice",
            "instructions": (
                "Classify task complexity based on scope, dependencies, and plan."
            ),
            "criteria": {
                "trivial": "Single file, <50 LOC, no dependencies",
                "small": "1-2 files, <200 LOC, simple changes",
                "medium": "3-5 files, moderate scope, some dependencies",
                "large": (
                    "5+ files, complex scope, multiple dependencies "
                    "or architectural changes"
                ),
            },
        },
        "priority_adjust": {
            "type": "choice",
            "instructions": (
                "Should priority be adjusted based on dependencies or urgency?"
            ),
            "criteria": {
                "keep": "Current priority is appropriate",
                "escalate": "Blocking other tasks or time-sensitive",
                "de-escalate": "Can wait, not blocking anything",
            },
        },
        "ready": {
            "type": "choice",
            "instructions": (
                "Is this task ready to execute? "
                "Check dependencies and scope clarity."
            ),
            "criteria": {
                "yes": "All dependencies met, scope is clear, plan is actionable",
                "blocked_missing_info": "Plan or requirements need clarification",
                "blocked_deps": "Waiting on other tasks to complete",
            },
        },
    }

    defaults = {
        "complexity": "medium",
        "priority_adjust": "keep",
        "ready": "yes",
    }

    try:
        resp = _post("/ask", {"state": state, "questions": questions})
        if not resp.get("ok"):
            logger.warning("Laya classify_task returned ok=false: %s", resp)
            return {**defaults, "latency_ms": 0}

        raw_result = {
            "complexity": resp["answers"]["complexity"]["choice"],
            "priority_adjust": resp["answers"]["priority_adjust"]["choice"],
            "ready": resp["answers"]["ready"]["choice"],
            "latency_ms": resp.get("latency_ms", 0),
        }
        return validate_classify_result(raw_result)
    except Exception as e:
        logger.warning("Laya classify_task failed (using defaults): %s", e)
        return {**defaults, "latency_ms": 0}


def triage_result(
    task: TaskState,
    verification: dict,
    engine_output: str,
) -> dict:
    """Post-test: classify pass/retry/escalate.
    
    Returns:
        dict with keys: verdict, retry_reason, latency_ms
        Falls back to safe defaults if Laya unavailable.
    """
    state = json.dumps({
        "id": task.id,
        "project": task.project,
        "verification": {k: v for k, v in verification.items() if k != "trace"},
        "engine_output_tail": engine_output[-2000:],
        "plan_summary": task.plan[:500],
    })

    questions = {
        "verdict": {
            "type": "choice",
            "instructions": (
                "Classify the verification result. "
                "Is this a pass, retry, or escalate?"
            ),
            "criteria": {
                "pass": "All checks pass or minor issues that don't block",
                "retry": "Transient or fixable issue (flaky test, env issue, simple bug)",
                "escalate": "Fundamental problem, needs human/strategic review",
            },
        },
        "retry_reason": {
            "type": "choice",
            "instructions": "If retrying, what's the primary reason?",
            "criteria": {
                "flaky_test": "Test is unstable, not related to changes",
                "real_bug": "Actual bug in implementation, fixable",
                "env_issue": "Environment or dependency problem",
            },
        },
    }

    defaults = {
        "verdict": "retry",
        "retry_reason": "real_bug",
    }

    try:
        resp = _post("/ask", {"state": state, "questions": questions})
        if not resp.get("ok"):
            logger.warning("Laya triage_result returned ok=false: %s", resp)
            return {**defaults, "latency_ms": 0}

        raw_result = {
            "verdict": resp["answers"]["verdict"]["choice"],
            "retry_reason": resp["answers"]["retry_reason"]["choice"],
            "latency_ms": resp.get("latency_ms", 0),
        }
        return validate_triage_result(raw_result)
    except Exception as e:
        logger.warning("Laya triage_result failed (using defaults): %s", e)
        return {**defaults, "latency_ms": 0}
