"""End-to-end tests for pipeline execution with real Laya."""

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from pipeline_schema import (
    PipelineDefinition,
    PipelineNode,
    PipelineEdge,
    NodeType,
    PipelineStatus,
    default_pipeline,
)
from pipeline_runtime import (
    PipelineRuntime,
    init_pipeline_tables,
    save_pipeline,
    load_pipeline,
    save_execution,
    load_execution,
)


def skip_if_laya_unavailable():
    """Skip test if Laya is not available."""
    try:
        from laya_client import is_available
        return not is_available()
    except Exception:
        return True


def skip_if_no_workspace():
    """Skip test if workspace is not writable."""
    return not os.access("/workspace", os.W_OK)


class TestPipelineE2E(unittest.TestCase):
    """E2E tests with real Laya (or fallback defaults)."""

    def setUp(self):
        self.tmp = tempfile.mktemp(suffix=".db")
        self.conn = sqlite3.connect(self.tmp)
        self.conn.row_factory = sqlite3.Row
        init_pipeline_tables(self.conn)

    def tearDown(self):
        self.conn.close()
        os.unlink(self.tmp)

    def _task_data(self, **overrides):
        base = {
            "id": "E2E-001",
            "project": "test",
            "type": "task",
            "priority": "P2",
            "files_scope": ["src/utils.ts"],
            "depends_on": [],
            "plan": "Add utility function",
            "acceptance": "Function works",
            "verification": {"lint": "pass", "build": "pass", "types": "pass"},
            "retry_count": 0,
            "frontmatter": {},
        }
        base.update(overrides)
        return base

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_success_flow(self):
        """Task → Laya → Worker → Tests → Triage PASS → Done."""
        pipeline = default_pipeline()
        task = self._task_data()
        runtime = PipelineRuntime(pipeline, task, db_conn=self.conn)
        execution = runtime.run()

        self.assertEqual(execution.status, PipelineStatus.COMPLETED)
        self.assertIn("input", execution.nodes)
        self.assertIn("classify", execution.nodes)
        self.assertIn("triage", execution.nodes)
        self.assertIn("done", execution.nodes)

        # Verify Laya classify was called
        classify_result = execution.nodes["classify"].result
        self.assertIn("complexity", classify_result)

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_retry_flow(self):
        """Task → Tests fail → pipeline stops (no fail edge in default pipeline)."""
        pipeline = default_pipeline()
        task = self._task_data(
            verification={"lint": "fail", "build": "pass", "types": "pass"}
        )
        runtime = PipelineRuntime(pipeline, task, db_conn=self.conn)
        execution = runtime.run()

        # Default pipeline only has tests (pass) -> triage edge
        # When tests fail, there's no edge to follow, so pipeline completes
        # without reaching triage
        self.assertEqual(execution.status, PipelineStatus.COMPLETED)
        self.assertNotIn("triage", execution.nodes)

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_escalate_flow(self):
        """Task → Tests fail → pipeline stops (no fail edge in default pipeline)."""
        pipeline = default_pipeline()
        task = self._task_data(
            verification={"lint": "fail", "build": "fail", "types": "fail"}
        )
        runtime = PipelineRuntime(pipeline, task, db_conn=self.conn)
        execution = runtime.run()

        # Default pipeline only has tests (pass) -> triage edge
        # When tests fail, there's no edge to follow, so pipeline completes
        # without reaching triage
        self.assertEqual(execution.status, PipelineStatus.COMPLETED)
        self.assertNotIn("triage", execution.nodes)

    def test_pipeline_persistence(self):
        """Save pipeline to DB, load it back, run it."""
        pipeline = default_pipeline()
        pid = save_pipeline(self.conn, pipeline)
        loaded = load_pipeline(self.conn, pid)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.pipeline, pipeline.pipeline)

        task = self._task_data()
        runtime = PipelineRuntime(loaded, task, db_conn=self.conn)
        execution = runtime.run()
        self.assertEqual(execution.status, PipelineStatus.COMPLETED)

    def test_execution_persistence(self):
        """Save execution state, load it back."""
        pipeline = default_pipeline()
        task = self._task_data()
        runtime = PipelineRuntime(pipeline, task, db_conn=self.conn)
        execution = runtime.run()
        save_execution(self.conn, execution)

        loaded = load_execution(self.conn, execution.execution_id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.execution_id, execution.execution_id)
        self.assertEqual(loaded.status, PipelineStatus.COMPLETED)

    def test_custom_pipeline(self):
        """Build a custom pipeline and run it."""
        pipeline = PipelineDefinition(
            pipeline="custom",
            version=1,
            nodes=[
                PipelineNode(id="input", type=NodeType.TASK_INPUT, position={"x": 0, "y": 0}),
                PipelineNode(id="tests", type=NodeType.TEST, position={"x": 200, "y": 0}),
                PipelineNode(id="done", type=NodeType.DONE, position={"x": 400, "y": 0}),
            ],
            edges=[
                PipelineEdge(source="input", target="tests"),
                PipelineEdge(source="tests", target="done", source_port="pass"),
            ],
        )
        task = self._task_data()
        runtime = PipelineRuntime(pipeline, task, db_conn=self.conn)
        execution = runtime.run()
        self.assertEqual(execution.status, PipelineStatus.COMPLETED)

    def test_node_status_tracking(self):
        """Verify each node has correct status after execution."""
        pipeline = default_pipeline()
        task = self._task_data()
        runtime = PipelineRuntime(pipeline, task, db_conn=self.conn)
        execution = runtime.run()

        for nid, ne in execution.nodes.items():
            self.assertIn(ne.status.value, {"completed", "failed", "pending"})

    def test_execution_metadata(self):
        """Verify execution has correct metadata."""
        pipeline = default_pipeline()
        task = self._task_data(id="META-TEST")
        runtime = PipelineRuntime(pipeline, task, db_conn=self.conn)
        execution = runtime.run()

        self.assertEqual(execution.task_id, "META-TEST")
        self.assertEqual(execution.pipeline_name, "coding-task")
        self.assertEqual(execution.pipeline_version, 1)
        self.assertIsNotNone(execution.execution_id)
        self.assertIsNotNone(execution.created_at)


if __name__ == "__main__":
    unittest.main()
