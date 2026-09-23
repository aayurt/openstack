"""End-to-end integration tests for Laya pipeline with real Laya."""

import json
import unittest
from laya_client import classify_task, triage_result, is_available, TaskState


def skip_if_laya_unavailable():
    """Skip test if Laya is not available."""
    return not is_available()


class TestLayaIntegrationReal(unittest.TestCase):
    """Integration tests with real Laya."""

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_classify_simple_task(self):
        """Test classification of a simple task."""
        task = TaskState(
            id="E2E-001",
            project="test",
            type="task",
            priority="P2",
            files_scope=["src/components/Button.tsx"],
            depends_on=[],
            plan="Add a new button component",
            acceptance="Button renders correctly",
            frontmatter={},
        )

        result = classify_task(task)

        # Verify valid output
        self.assertIn(result["complexity"], {"trivial", "small", "medium", "large"})
        self.assertIn(result["priority_adjust"], {"keep", "escalate", "de-escalate"})
        self.assertIn(result["ready"], {"yes", "blocked_missing_info", "blocked_deps"})
        self.assertGreater(result["latency_ms"], 0)

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_classify_complex_task(self):
        """Test classification of a complex task."""
        task = TaskState(
            id="E2E-002",
            project="test",
            type="task",
            priority="P1",
            files_scope=[
                "src/api/auth.ts",
                "src/api/users.ts",
                "src/models/user.ts",
                "src/routes/auth.ts",
                "tests/auth.test.ts",
            ],
            depends_on=["E2E-001"],
            plan="Implement authentication system with JWT tokens, user roles, and session management",
            acceptance="Users can login, logout, and access protected routes based on roles",
            frontmatter={},
        )

        result = classify_task(task)

        # Verify valid output
        self.assertIn(result["complexity"], {"trivial", "small", "medium", "large"})
        self.assertIn(result["ready"], {"yes", "blocked_missing_info", "blocked_deps"})

        # Complex task should be medium/large
        self.assertIn(result["complexity"], {"medium", "large"})

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_classify_blocked_task(self):
        """Test classification of a blocked task."""
        task = TaskState(
            id="E2E-003",
            project="test",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=["E2E-999"],  # Non-existent dependency
            plan="Depends on missing task",
            acceptance="N/A",
            frontmatter={},
        )

        result = classify_task(task)

        # Should be blocked
        self.assertEqual(result["ready"], "blocked_deps")

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_triage_pass(self):
        """Test triage of passing verification."""
        task = TaskState(
            id="E2E-004",
            project="test",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Fix bug",
            acceptance="Bug fixed",
            frontmatter={},
        )

        verification = {
            "lint": "pass",
            "build": "pass",
            "types": "pass",
        }

        result = triage_result(task, verification, "All tests passed")

        # Should pass
        self.assertEqual(result["verdict"], "pass")
        self.assertIn(result["retry_reason"], {"flaky_test", "real_bug", "env_issue"})

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_triage_retry(self):
        """Test triage of retryable failure."""
        task = TaskState(
            id="E2E-005",
            project="test",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Fix bug",
            acceptance="Bug fixed",
            frontmatter={},
        )

        verification = {
            "lint": "fail",
            "build": "pass",
            "types": "pass",
        }

        result = triage_result(task, verification, "Error: missing import")

        # Verify valid output
        self.assertIn(result["verdict"], {"pass", "retry", "escalate"})
        self.assertIn(result["retry_reason"], {"flaky_test", "real_bug", "env_issue"})
        self.assertGreater(result["latency_ms"], 0)

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_triage_escalate(self):
        """Test triage of critical failure."""
        task = TaskState(
            id="E2E-006",
            project="test",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Refactor module",
            acceptance="Module refactored",
            frontmatter={},
        )

        verification = {
            "lint": "fail",
            "build": "fail",
            "types": "fail",
        }

        result = triage_result(
            task,
            verification,
            "Critical: database schema mismatch, data loss risk"
        )

        # Should escalate
        self.assertEqual(result["verdict"], "escalate")

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_full_lifecycle(self):
        """Test full task lifecycle with Laya."""
        # 1. Create task
        task = TaskState(
            id="E2E-LIFECYCLE",
            project="test",
            type="task",
            priority="P2",
            files_scope=["src/utils.ts"],
            depends_on=[],
            plan="Add utility function",
            acceptance="Function works",
            frontmatter={},
        )

        # 2. Classify
        classify_result = classify_task(task)
        self.assertIn(classify_result["complexity"], {"trivial", "small", "medium", "large"})

        # 3. Simulate execution
        # (In real usage, OpenCode would execute here)

        # 4. Triage with passing verification
        verification = {"lint": "pass", "build": "pass", "types": "pass"}
        triage_result_pass = triage_result(task, verification, "Success")
        self.assertEqual(triage_result_pass["verdict"], "pass")

        # 5. Triage with failing verification
        verification_fail = {"lint": "fail", "build": "pass"}
        triage_result_fail = triage_result(task, verification_fail, "Error")
        self.assertIn(triage_result_fail["verdict"], {"retry", "escalate"})


class TestLayaPerformance(unittest.TestCase):
    """Performance tests with real Laya."""

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_classify_latency(self):
        """Test that classification completes within acceptable time."""
        import time

        task = TaskState(
            id="PERF-001",
            project="test",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Performance test",
            acceptance="Works",
            frontmatter={},
        )

        start = time.perf_counter()
        result = classify_task(task)
        elapsed = (time.perf_counter() - start) * 1000

        # Should complete within 5 seconds (including cold start)
        self.assertLess(elapsed, 5000)
        self.assertGreater(result["latency_ms"], 0)

    @unittest.skipIf(skip_if_laya_unavailable(), "Laya not available")
    def test_triage_latency(self):
        """Test that triage completes within acceptable time."""
        import time

        task = TaskState(
            id="PERF-002",
            project="test",
            type="task",
            priority="P2",
            files_scope=["src/"],
            depends_on=[],
            plan="Performance test",
            acceptance="Works",
            frontmatter={},
        )

        start = time.perf_counter()
        result = triage_result(task, {"lint": "fail"}, "Error")
        elapsed = (time.perf_counter() - start) * 1000

        # Should complete within 3 seconds
        self.assertLess(elapsed, 3000)
        self.assertGreater(result["latency_ms"], 0)


if __name__ == "__main__":
    unittest.main()
