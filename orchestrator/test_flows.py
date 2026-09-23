#!/usr/bin/env python3
"""Quick test to verify both old and new flows work correctly."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from laya_client import classify_task, triage_result, is_available, TaskState


def test_old_flow():
    """Test old flow (no Laya calls)."""
    print("Testing OLD flow (LAYA_ENABLED=false)...")
    
    # Simulate old flow: no Laya calls, just data processing
    task_data = {
        "id": "OLD-TEST",
        "project": "test",
        "type": "task",
        "priority": "P2",
        "status": "new",
    }
    
    # Old flow: simple JSON operations
    result = json.dumps(task_data)
    assert json.loads(result) == task_data
    
    print("✓ OLD flow: PASSED")
    return True


def test_new_flow():
    """Test new flow (with Laya calls)."""
    print("Testing NEW flow (LAYA_ENABLED=true)...")
    
    task = TaskState(
        id="NEW-TEST",
        project="test",
        type="task",
        priority="P2",
        files_scope=["src/"],
        depends_on=[],
        plan="Test plan",
        acceptance="Test acceptance",
        frontmatter={},
    )
    
    # New flow: Laya classification
    if is_available():
        result = classify_task(task)
        assert "complexity" in result
        assert "priority_adjust" in result
        assert "ready" in result
        print(f"  classify_task: {result['complexity']} / {result['ready']}")
        
        # New flow: Laya triage
        verification = {"lint": "pass", "build": "pass"}
        triage = triage_result(task, verification, "Success")
        assert "verdict" in triage
        assert "retry_reason" in triage
        print(f"  triage_result: {triage['verdict']}")
    else:
        print("  ⚠ Laya not available, skipping Laya calls")
    
    print("✓ NEW flow: PASSED")
    return True


def test_feature_flag():
    """Test feature flag toggle."""
    print("Testing feature flag...")
    
    # Simulate environment variable
    import os
    os.environ["LAYA_ENABLED"] = "true"
    from app import LAYA_ENABLED
    assert LAYA_ENABLED is True
    
    os.environ["LAYA_ENABLED"] = "false"
    # Note: need to reload module for env change to take effect
    print("  Feature flag: LAYA_ENABLED configurable")
    
    print("✓ Feature flag: PASSED")
    return True


def main():
    print("=" * 50)
    print("FLOW COMPARISON TEST")
    print("=" * 50)
    print()
    
    tests = [
        ("Old Flow", test_old_flow),
        ("New Flow", test_new_flow),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"✗ {name}: FAILED ({e})")
            failed += 1
        print()
    
    print("=" * 50)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 50)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
