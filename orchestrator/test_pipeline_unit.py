"""Unit tests for pipeline schema and runtime (mocked Laya)."""

import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from pipeline_schema import (
    PipelineDefinition,
    PipelineNode,
    PipelineEdge,
    NodeType,
    NodeStatus,
    PipelineStatus,
    NODE_CATEGORIES,
    NODE_PORT_MAP,
    default_pipeline,
)
from pipeline_runtime import (
    PipelineRuntime,
    PipelineExecution,
    NodeExecution,
    init_pipeline_tables,
    save_pipeline,
    load_pipeline,
    list_pipelines,
    save_execution,
    load_execution,
    list_executions,
    _handle_task_input,
    _handle_laya_classify,
    _handle_laya_triage,
    _handle_readiness,
    _handle_worker_router,
    _handle_opencode_worker,
    _handle_tests,
    _handle_retry,
    _handle_condition,
    _handle_join,
    _handle_hermes_replan,
    _handle_done,
    NodeContext,
)


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestPipelineSchema(unittest.TestCase):
    def test_default_pipeline(self):
        p = default_pipeline()
        self.assertEqual(p.pipeline, "coding-task")
        self.assertEqual(p.version, 1)
        self.assertGreater(len(p.nodes), 0)
        self.assertGreater(len(p.edges), 0)

    def test_node_roundtrip(self):
        n = PipelineNode(id="test", type=NodeType.LAYA_CLASSIFY, position={"x": 10, "y": 20})
        d = n.to_dict()
        n2 = PipelineNode.from_dict(d)
        self.assertEqual(n.id, n2.id)
        self.assertEqual(n.type, n2.type)
        self.assertEqual(n.position, n2.position)

    def test_edge_roundtrip(self):
        e = PipelineEdge(source="a", target="b", source_port="output", target_port="input")
        d = e.to_dict()
        e2 = PipelineEdge.from_dict(d)
        self.assertEqual(e.source, e2.source)
        self.assertEqual(e.target, e2.target)
        self.assertEqual(e.source_port, e2.source_port)

    def test_pipeline_roundtrip(self):
        p = default_pipeline()
        d = p.to_dict()
        p2 = PipelineDefinition.from_dict(d)
        self.assertEqual(p.pipeline, p2.pipeline)
        self.assertEqual(len(p.nodes), len(p2.nodes))
        self.assertEqual(len(p.edges), len(p2.edges))

    def test_pipeline_json_roundtrip(self):
        p = default_pipeline()
        j = p.to_json()
        p2 = PipelineDefinition.from_json(j)
        self.assertEqual(p.pipeline, p2.pipeline)

    def test_content_hash(self):
        p = default_pipeline()
        h1 = p.content_hash()
        h2 = p.content_hash()
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 16)

    def test_node_ids(self):
        p = default_pipeline()
        ids = p.node_ids()
        self.assertIn("input", ids)
        self.assertIn("classify", ids)
        self.assertIn("triage", ids)
        self.assertIn("done", ids)

    def test_validate_valid(self):
        p = default_pipeline()
        errors = p.validate()
        self.assertEqual(errors, [])

    def test_validate_missing_source(self):
        p = PipelineDefinition(
            pipeline="test",
            version=1,
            nodes=[PipelineNode(id="a", type=NodeType.TASK_INPUT)],
            edges=[PipelineEdge(source="missing", target="a")],
        )
        errors = p.validate()
        self.assertGreater(len(errors), 0)
        self.assertIn("missing", errors[0])

    def test_validate_missing_target(self):
        p = PipelineDefinition(
            pipeline="test",
            version=1,
            nodes=[PipelineNode(id="a", type=NodeType.TASK_INPUT)],
            edges=[PipelineEdge(source="a", target="missing")],
        )
        errors = p.validate()
        self.assertGreater(len(errors), 0)

    def test_node_categories(self):
        self.assertIn("Input", NODE_CATEGORIES)
        self.assertIn("Laya", NODE_CATEGORIES)
        self.assertIn("Execution", NODE_CATEGORIES)

    def test_node_ports(self):
        ports = NODE_PORT_MAP[NodeType.LAYA_TRIAGE.value]
        self.assertEqual(ports["inputs"], ["input"])
        self.assertEqual(ports["outputs"], ["pass", "retry", "escalate"])


# ---------------------------------------------------------------------------
# Node handler tests
# ---------------------------------------------------------------------------

class TestNodeHandlers(unittest.TestCase):
    def _make_ctx(self, node_type, task_data=None, upstream=None, config=None):
        node = PipelineNode(id="test", type=node_type, config=config or {})
        execution = PipelineExecution(
            execution_id="exec-1",
            task_id="TASK-001",
            pipeline_name="test",
            pipeline_version=1,
        )
        pipeline = default_pipeline()
        task_data = task_data or {"id": "TASK-001", "project": "test"}
        node_results = upstream or {}
        return NodeContext(
            execution=execution,
            pipeline=pipeline,
            task_data=task_data,
            node=node,
            node_results=node_results,
        )

    def test_task_input(self):
        ctx = self._make_ctx(NodeType.TASK_INPUT, task_data={
            "id": "T1", "project": "p", "objective": "o",
            "priority": "P1", "files_scope": ["a.py"], "depends_on": [],
            "acceptance": "works",
        })
        result = _handle_task_input(ctx)
        self.assertEqual(result["task_id"], "T1")
        self.assertEqual(result["project"], "p")

    @patch("pipeline_runtime._handle_laya_classify")
    def test_laya_classify(self, mock_classify):
        mock_classify.return_value = {"complexity": "small", "priority_adjust": "keep", "ready": "yes", "latency_ms": 100}
        ctx = self._make_ctx(NodeType.LAYA_CLASSIFY)
        result = _handle_laya_classify(ctx)
        self.assertIn("complexity", result)

    def test_laya_classify_fallback(self):
        ctx = self._make_ctx(NodeType.LAYA_CLASSIFY)
        # Laya not available, should use defaults
        result = _handle_laya_classify(ctx)
        self.assertIn(result["complexity"], {"trivial", "small", "medium", "large"})

    def test_laya_triage_fallback(self):
        ctx = self._make_ctx(NodeType.LAYA_TRIAGE)
        result = _handle_laya_triage(ctx)
        self.assertIn(result["verdict"], {"pass", "retry", "escalate"})

    def test_readiness_ready(self):
        ctx = self._make_ctx(NodeType.TASK_READINESS, upstream={"classify": {"ready": "yes"}})
        result = _handle_readiness(ctx)
        self.assertTrue(result["ready"])

    def test_readiness_blocked(self):
        ctx = self._make_ctx(NodeType.TASK_READINESS, upstream={"classify": {"ready": "blocked_deps"}})
        result = _handle_readiness(ctx)
        self.assertFalse(result["ready"])

    def test_worker_router(self):
        ctx = self._make_ctx(NodeType.WORKER_ROUTER, upstream={"classify": {"complexity": "small"}})
        result = _handle_worker_router(ctx)
        self.assertIn("worker_id", result)

    def test_opencode_worker(self):
        ctx = self._make_ctx(NodeType.OPENCODE_WORKER, upstream={"router": {"worker_id": "01"}})
        result = _handle_opencode_worker(ctx)
        self.assertEqual(result["worker_id"], "01")

    def test_tests_all_pass(self):
        ctx = self._make_ctx(NodeType.TEST, task_data={
            "id": "T1", "verification": {"lint": "pass", "build": "pass", "types": "pass"}
        })
        result = _handle_tests(ctx)
        self.assertTrue(result["all_pass"])

    def test_tests_fail(self):
        ctx = self._make_ctx(NodeType.TEST, task_data={
            "id": "T1", "verification": {"lint": "fail", "build": "pass"}
        })
        result = _handle_tests(ctx)
        self.assertFalse(result["all_pass"])

    def test_retry_within_limit(self):
        ctx = self._make_ctx(NodeType.RETRY, task_data={"retry_count": 1}, config={"max_retries": 3})
        result = _handle_retry(ctx)
        self.assertTrue(result["should_retry"])
        self.assertEqual(result["retry_count"], 2)

    def test_retry_at_limit(self):
        ctx = self._make_ctx(NodeType.RETRY, task_data={"retry_count": 3}, config={"max_retries": 3})
        result = _handle_retry(ctx)
        self.assertFalse(result["should_retry"])

    def test_condition_true(self):
        ctx = self._make_ctx(NodeType.CONDITION, config={"expression": "True"})
        result = _handle_condition(ctx)
        self.assertTrue(result["condition"])

    def test_condition_false(self):
        ctx = self._make_ctx(NodeType.CONDITION, config={"expression": "False"})
        result = _handle_condition(ctx)
        self.assertFalse(result["condition"])

    def test_join(self):
        ctx = self._make_ctx(NodeType.JOIN)
        result = _handle_join(ctx)
        self.assertTrue(result["joined"])

    def test_hermes_replan(self):
        ctx = self._make_ctx(NodeType.HERMES_REPLAN, task_data={"id": "T1", "plan": "do stuff"})
        result = _handle_hermes_replan(ctx)
        self.assertTrue(result["escalated"])

    def test_done(self):
        ctx = self._make_ctx(NodeType.DONE)
        result = _handle_done(ctx)
        self.assertEqual(result["status"], "done")


# ---------------------------------------------------------------------------
# Runtime tests
# ---------------------------------------------------------------------------

class TestPipelineRuntime(unittest.TestCase):
    def test_run_default_pipeline(self):
        p = default_pipeline()
        task_data = {
            "id": "TASK-TEST",
            "project": "test",
            "type": "task",
            "priority": "P2",
            "files_scope": [],
            "depends_on": [],
            "plan": "test plan",
            "acceptance": "test acceptance",
            "verification": {"lint": "pass", "build": "pass", "types": "pass"},
            "retry_count": 0,
        }
        runtime = PipelineRuntime(p, task_data)
        execution = runtime.run()
        self.assertEqual(execution.status, PipelineStatus.COMPLETED)
        self.assertEqual(execution.task_id, "TASK-TEST")

    def test_run_single_node(self):
        p = PipelineDefinition(
            pipeline="test",
            version=1,
            nodes=[PipelineNode(id="input", type=NodeType.TASK_INPUT)],
            edges=[],
        )
        runtime = PipelineRuntime(p, {"id": "T1"})
        execution = runtime.run()
        self.assertEqual(execution.status, PipelineStatus.COMPLETED)
        self.assertIn("input", execution.nodes)
        self.assertEqual(execution.nodes["input"].status, NodeStatus.COMPLETED)

    def test_run_with_condition(self):
        p = PipelineDefinition(
            pipeline="test",
            version=1,
            nodes=[
                PipelineNode(id="input", type=NodeType.TASK_INPUT),
                PipelineNode(id="cond", type=NodeType.CONDITION, config={"expression": "True"}),
                PipelineNode(id="done", type=NodeType.DONE),
            ],
            edges=[
                PipelineEdge(source="input", target="cond"),
                PipelineEdge(source="cond", target="done", source_port="true"),
            ],
        )
        runtime = PipelineRuntime(p, {"id": "T1"})
        execution = runtime.run()
        self.assertEqual(execution.status, PipelineStatus.COMPLETED)

    def test_node_failure_stops_execution(self):
        p = PipelineDefinition(
            pipeline="test",
            version=1,
            nodes=[
                PipelineNode(id="fail_node", type=NodeType.CONDITION, config={"expression": "1/0"}),
                PipelineNode(id="after", type=NodeType.DONE),
            ],
            edges=[
                PipelineEdge(source="fail_node", target="after", source_port="true"),
            ],
        )
        runtime = PipelineRuntime(p, {"id": "T1"})
        execution = runtime.run()
        self.assertEqual(execution.status, PipelineStatus.FAILED)

    def test_execution_to_dict(self):
        p = default_pipeline()
        runtime = PipelineRuntime(p, {"id": "T1"})
        execution = runtime.run()
        d = execution.to_dict()
        self.assertIn("execution_id", d)
        self.assertIn("nodes", d)
        self.assertIsInstance(d["nodes"], dict)

    def test_port_resolution_triage(self):
        p = default_pipeline()
        runtime = PipelineRuntime(p, {"id": "T1"})
        # Test port resolution for triage node
        node = runtime._node_by_id("triage")
        # Pass verdict
        port = runtime._resolve_next_port(node, {"verdict": "pass"})
        self.assertEqual(port, "pass")
        # Retry verdict
        port = runtime._resolve_next_port(node, {"verdict": "retry"})
        self.assertEqual(port, "retry")
        # Escalate verdict
        port = runtime._resolve_next_port(node, {"verdict": "escalate"})
        self.assertEqual(port, "escalate")

    def test_port_resolution_tests(self):
        p = default_pipeline()
        runtime = PipelineRuntime(p, {"id": "T1"})
        node = runtime._node_by_id("tests")
        port = runtime._resolve_next_port(node, {"all_pass": True})
        self.assertEqual(port, "pass")
        port = runtime._resolve_next_port(node, {"all_pass": False})
        self.assertEqual(port, "fail")


# ---------------------------------------------------------------------------
# Persistence tests
# ---------------------------------------------------------------------------

class TestPipelinePersistence(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mktemp(suffix=".db")
        self.conn = sqlite3.connect(self.tmp)
        self.conn.row_factory = sqlite3.Row
        init_pipeline_tables(self.conn)

    def tearDown(self):
        self.conn.close()
        os.unlink(self.tmp)

    def test_save_and_load_pipeline(self):
        p = default_pipeline()
        pid = save_pipeline(self.conn, p)
        loaded = load_pipeline(self.conn, pid)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.pipeline, p.pipeline)
        self.assertEqual(loaded.version, p.version)

    def test_list_pipelines(self):
        p = default_pipeline()
        save_pipeline(self.conn, p)
        pipelines = list_pipelines(self.conn)
        self.assertEqual(len(pipelines), 1)

    def test_save_and_load_execution(self):
        exec_ = PipelineExecution(
            execution_id="exec-1",
            task_id="T1",
            pipeline_name="test",
            pipeline_version=1,
        )
        save_execution(self.conn, exec_)
        loaded = load_execution(self.conn, "exec-1")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.execution_id, "exec-1")

    def test_list_executions(self):
        exec_ = PipelineExecution(
            execution_id="exec-1",
            task_id="T1",
            pipeline_name="test",
            pipeline_version=1,
        )
        save_execution(self.conn, exec_)
        executions = list_executions(self.conn, task_id="T1")
        self.assertEqual(len(executions), 1)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases(unittest.TestCase):
    def test_empty_pipeline(self):
        p = PipelineDefinition(pipeline="empty", version=1, nodes=[], edges=[])
        runtime = PipelineRuntime(p, {"id": "T1"})
        execution = runtime.run()
        self.assertEqual(execution.status, PipelineStatus.COMPLETED)

    def test_single_node_no_edges(self):
        p = PipelineDefinition(
            pipeline="test",
            version=1,
            nodes=[PipelineNode(id="lonely", type=NodeType.DONE)],
            edges=[],
        )
        runtime = PipelineRuntime(p, {"id": "T1"})
        execution = runtime.run()
        self.assertEqual(execution.status, PipelineStatus.COMPLETED)

    def test_invalid_node_id(self):
        p = default_pipeline()
        runtime = PipelineRuntime(p, {"id": "T1"})
        with self.assertRaises(ValueError):
            runtime.execute_node("nonexistent")

    def test_serialization(self):
        p = default_pipeline()
        j = p.to_json()
        p2 = PipelineDefinition.from_json(j)
        self.assertEqual(p.to_json(), p2.to_json())


if __name__ == "__main__":
    unittest.main()
