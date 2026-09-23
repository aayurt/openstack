"""Tests for Laya integration in the task pipeline."""

import json
import unittest
from unittest.mock import patch, MagicMock
from laya_client import TaskState, classify_task, triage_result, health, is_available


class TestLayaClient(unittest.TestCase):
    """Unit tests for Laya client module."""

    def test_task_state_to_json(self):
        """Test TaskState serialization."""
        task = TaskState(
            id="TEST-001",
            project="test-project",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=["TASK-000"],
            plan="Implement feature X",
            acceptance="Feature X works correctly",
            frontmatter={"id": "TEST-001"},
        )
        result = json.loads(task.to_json())
        self.assertEqual(result["id"], "TEST-001")
        self.assertEqual(result["project"], "test-project")
        self.assertEqual(result["files_scope"], ["src/"])
        self.assertEqual(result["depends_on"], ["TASK-000"])

    def test_classify_task_success(self):
        """Test successful task classification."""
        task = TaskState(
            id="TEST-001",
            project="test-project",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Implement feature X",
            acceptance="Feature X works correctly",
            frontmatter={"id": "TEST-001"},
        )

        with patch("laya_client._post") as mock_post:
            mock_post.return_value = {
                "ok": True,
                "answers": {
                    "complexity": {"choice": "small"},
                    "priority_adjust": {"choice": "keep"},
                    "ready": {"choice": "yes"},
                },
                "latency_ms": 150.0,
            }

            result = classify_task(task)

            self.assertEqual(result["complexity"], "small")
            self.assertEqual(result["priority_adjust"], "keep")
            self.assertEqual(result["ready"], "yes")
            self.assertEqual(result["latency_ms"], 150.0)

    def test_classify_task_fallback(self):
        """Test fallback when Laya is unavailable."""
        task = TaskState(
            id="TEST-002",
            project="test-project",
            type="task",
            priority="P2",
            files_scope=["*"],
            depends_on=[],
            plan="",
            acceptance="",
            frontmatter={},
        )

        with patch("laya_client._post") as mock_post:
            mock_post.side_effect = Exception("Laya unavailable")

            result = classify_task(task)

            # Should return safe defaults
            self.assertEqual(result["complexity"], "medium")
            self.assertEqual(result["priority_adjust"], "keep")
            self.assertEqual(result["ready"], "yes")
            self.assertEqual(result["latency_ms"], 0)

    def test_classify_task_laya_error(self):
        """Test fallback when Laya returns ok=false."""
        task = TaskState(
            id="TEST-003",
            project="test-project",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Fix bug",
            acceptance="Bug fixed",
            frontmatter={},
        )

        with patch("laya_client._post") as mock_post:
            mock_post.return_value = {"ok": False, "error": "invalid"}

            result = classify_task(task)

            self.assertEqual(result["complexity"], "medium")
            self.assertEqual(result["ready"], "yes")

    def test_triage_result_success(self):
        """Test successful result triage."""
        task = TaskState(
            id="TEST-004",
            project="test-project",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Fix bug",
            acceptance="Bug fixed",
            frontmatter={},
        )

        verification = {"lint": "pass", "build": "pass", "types": "pass"}

        with patch("laya_client._post") as mock_post:
            mock_post.return_value = {
                "ok": True,
                "answers": {
                    "verdict": {"choice": "pass"},
                    "retry_reason": {"choice": "flaky_test"},
                },
                "latency_ms": 100.0,
            }

            result = triage_result(task, verification, "All tests passed")

            self.assertEqual(result["verdict"], "pass")
            self.assertEqual(result["retry_reason"], "flaky_test")
            self.assertEqual(result["latency_ms"], 100.0)

    def test_triage_result_escalate(self):
        """Test escalation triage."""
        task = TaskState(
            id="TEST-005",
            project="test-project",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Refactor module",
            acceptance="Module refactored",
            frontmatter={},
        )

        verification = {"lint": "fail", "build": "fail", "types": "fail"}

        with patch("laya_client._post") as mock_post:
            mock_post.return_value = {
                "ok": True,
                "answers": {
                    "verdict": {"choice": "escalate"},
                    "retry_reason": {"choice": "real_bug"},
                },
                "latency_ms": 200.0,
            }

            result = triage_result(task, verification, "Critical failure")

            self.assertEqual(result["verdict"], "escalate")
            self.assertEqual(result["retry_reason"], "real_bug")

    def test_triage_result_fallback(self):
        """Test fallback when Laya is unavailable."""
        task = TaskState(
            id="TEST-006",
            project="test-project",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Fix bug",
            acceptance="Bug fixed",
            frontmatter={},
        )

        with patch("laya_client._post") as mock_post:
            mock_post.side_effect = Exception("Connection refused")

            result = triage_result(task, {}, "Error output")

            self.assertEqual(result["verdict"], "retry")
            self.assertEqual(result["retry_reason"], "real_bug")
            self.assertEqual(result["latency_ms"], 0)

    def test_health_check(self):
        """Test health check function."""
        with patch("laya_client._get") as mock_get:
            mock_get.return_value = {"ok": True, "loaded": True}
            result = health()
            self.assertTrue(result["ok"])
            self.assertTrue(result["loaded"])

    def test_is_available(self):
        """Test availability check."""
        with patch("laya_client.health") as mock_health:
            mock_health.return_value = {"ok": True, "loaded": True}
            self.assertTrue(is_available())

            mock_health.side_effect = Exception("Connection refused")
            self.assertFalse(is_available())


if __name__ == "__main__":
    unittest.main()
