"""Unit tests with mocked Laya for deterministic testing."""

import json
import unittest
from unittest.mock import patch
from laya_client import (
    classify_task, triage_result,
    validate_classify_result, validate_triage_result,
    TaskState, is_available
)


class TestValidation(unittest.TestCase):
    """Test input validation."""

    def test_validate_classify_valid(self):
        result = validate_classify_result({
            "complexity": "large",
            "priority_adjust": "escalate",
            "ready": "yes",
            "latency_ms": 100,
        })
        self.assertEqual(result["complexity"], "large")
        self.assertEqual(result["priority_adjust"], "escalate")
        self.assertEqual(result["ready"], "yes")
        self.assertEqual(result["latency_ms"], 100)

    def test_validate_classify_invalid_complexity(self):
        result = validate_classify_result({
            "complexity": "INVALID",
            "priority_adjust": "keep",
            "ready": "yes",
        })
        self.assertEqual(result["complexity"], "medium")  # fallback

    def test_validate_classify_invalid_priority(self):
        result = validate_classify_result({
            "complexity": "small",
            "priority_adjust": "INVALID",
            "ready": "yes",
        })
        self.assertEqual(result["priority_adjust"], "keep")  # fallback

    def test_validate_classify_invalid_ready(self):
        result = validate_classify_result({
            "complexity": "small",
            "priority_adjust": "keep",
            "ready": "INVALID",
        })
        self.assertEqual(result["ready"], "yes")  # fallback

    def test_validate_classify_all_invalid(self):
        result = validate_classify_result({
            "complexity": "INVALID",
            "priority_adjust": "INVALID",
            "ready": "INVALID",
        })
        self.assertEqual(result["complexity"], "medium")
        self.assertEqual(result["priority_adjust"], "keep")
        self.assertEqual(result["ready"], "yes")

    def test_validate_triage_valid(self):
        result = validate_triage_result({
            "verdict": "escalate",
            "retry_reason": "env_issue",
            "latency_ms": 200,
        })
        self.assertEqual(result["verdict"], "escalate")
        self.assertEqual(result["retry_reason"], "env_issue")
        self.assertEqual(result["latency_ms"], 200)

    def test_validate_triage_invalid_verdict(self):
        result = validate_triage_result({
            "verdict": "INVALID",
            "retry_reason": "real_bug",
        })
        self.assertEqual(result["verdict"], "retry")  # fallback

    def test_validate_triage_invalid_reason(self):
        result = validate_triage_result({
            "verdict": "pass",
            "retry_reason": "INVALID",
        })
        self.assertEqual(result["retry_reason"], "real_bug")  # fallback

    def test_validate_triage_all_invalid(self):
        result = validate_triage_result({
            "verdict": "INVALID",
            "retry_reason": "INVALID",
        })
        self.assertEqual(result["verdict"], "retry")
        self.assertEqual(result["retry_reason"], "real_bug")


class TestClassifyTask(unittest.TestCase):
    """Test classify_task with mocked Laya."""

    def setUp(self):
        self.task = TaskState(
            id="TEST-001",
            project="test",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Test plan",
            acceptance="Test acceptance",
            frontmatter={},
        )

    def test_classify_success(self):
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

            result = classify_task(self.task)
            self.assertEqual(result["complexity"], "small")
            self.assertEqual(result["priority_adjust"], "keep")
            self.assertEqual(result["ready"], "yes")
            self.assertEqual(result["latency_ms"], 150.0)

    def test_classify_fallback_on_exception(self):
        with patch("laya_client._post") as mock_post:
            mock_post.side_effect = Exception("Connection refused")

            result = classify_task(self.task)
            self.assertEqual(result["complexity"], "medium")
            self.assertEqual(result["priority_adjust"], "keep")
            self.assertEqual(result["ready"], "yes")
            self.assertEqual(result["latency_ms"], 0)

    def test_classify_fallback_on_error_response(self):
        with patch("laya_client._post") as mock_post:
            mock_post.return_value = {"ok": False, "error": "invalid"}

            result = classify_task(self.task)
            self.assertEqual(result["complexity"], "medium")
            self.assertEqual(result["ready"], "yes")

    def test_classify_validates_output(self):
        with patch("laya_client._post") as mock_post:
            mock_post.return_value = {
                "ok": True,
                "answers": {
                    "complexity": {"choice": "INVALID"},
                    "priority_adjust": {"choice": "keep"},
                    "ready": {"choice": "yes"},
                },
                "latency_ms": 100.0,
            }

            result = classify_task(self.task)
            # Should be validated/fallback
            self.assertEqual(result["complexity"], "medium")


class TestTriageResult(unittest.TestCase):
    """Test triage_result with mocked Laya."""

    def setUp(self):
        self.task = TaskState(
            id="TEST-002",
            project="test",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Fix bug",
            acceptance="Bug fixed",
            frontmatter={},
        )
        self.verification = {"lint": "fail", "build": "pass"}

    def test_triage_success(self):
        with patch("laya_client._post") as mock_post:
            mock_post.return_value = {
                "ok": True,
                "answers": {
                    "verdict": {"choice": "retry"},
                    "retry_reason": {"choice": "real_bug"},
                },
                "latency_ms": 100.0,
            }

            result = triage_result(self.task, self.verification, "Error")
            self.assertEqual(result["verdict"], "retry")
            self.assertEqual(result["retry_reason"], "real_bug")

    def test_triage_escalate(self):
        with patch("laya_client._post") as mock_post:
            mock_post.return_value = {
                "ok": True,
                "answers": {
                    "verdict": {"choice": "escalate"},
                    "retry_reason": {"choice": "real_bug"},
                },
                "latency_ms": 200.0,
            }

            result = triage_result(self.task, self.verification, "Critical")
            self.assertEqual(result["verdict"], "escalate")

    def test_triage_pass(self):
        with patch("laya_client._post") as mock_post:
            mock_post.return_value = {
                "ok": True,
                "answers": {
                    "verdict": {"choice": "pass"},
                    "retry_reason": {"choice": "flaky_test"},
                },
                "latency_ms": 80.0,
            }

            result = triage_result(self.task, self.verification, "Success")
            self.assertEqual(result["verdict"], "pass")

    def test_triage_fallback_on_exception(self):
        with patch("laya_client._post") as mock_post:
            mock_post.side_effect = Exception("Timeout")

            result = triage_result(self.task, self.verification, "Error")
            self.assertEqual(result["verdict"], "retry")
            self.assertEqual(result["retry_reason"], "real_bug")
            self.assertEqual(result["latency_ms"], 0)

    def test_triage_validates_output(self):
        with patch("laya_client._post") as mock_post:
            mock_post.return_value = {
                "ok": True,
                "answers": {
                    "verdict": {"choice": "INVALID"},
                    "retry_reason": {"choice": "INVALID"},
                },
                "latency_ms": 100.0,
            }

            result = triage_result(self.task, self.verification, "Error")
            # Should be validated/fallback
            self.assertEqual(result["verdict"], "retry")
            self.assertEqual(result["retry_reason"], "real_bug")


class TestIsAvailable(unittest.TestCase):
    """Test is_available function."""

    def test_available(self):
        with patch("laya_client.health") as mock_health:
            mock_health.return_value = {"ok": True, "loaded": True}
            self.assertTrue(is_available())

    def test_not_available_ok_false(self):
        with patch("laya_client.health") as mock_health:
            mock_health.return_value = {"ok": False, "loaded": True}
            self.assertFalse(is_available())

    def test_not_available_loaded_false(self):
        with patch("laya_client.health") as mock_health:
            mock_health.return_value = {"ok": True, "loaded": False}
            self.assertFalse(is_available())

    def test_not_available_exception(self):
        with patch("laya_client.health") as mock_health:
            mock_health.side_effect = Exception("Connection refused")
            self.assertFalse(is_available())


if __name__ == "__main__":
    unittest.main()
