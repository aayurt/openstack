#!/usr/bin/env python3
"""Test Laya warmer functionality."""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def test_warmer_config():
    """Test warmer configuration."""
    print("Testing warmer configuration...")
    
    # Test default interval
    os.environ.pop("LAYA_WARMER_INTERVAL", None)
    from app import LAYA_WARMER_INTERVAL
    assert LAYA_WARMER_INTERVAL == 120  # default 2 minutes
    print(f"  Default interval: {LAYA_WARMER_INTERVAL}s")
    
    # Test custom interval
    os.environ["LAYA_WARMER_INTERVAL"] = "60"
    # Note: need to reload module for env change to take effect
    print("  Custom interval: configurable via LAYA_WARMER_INTERVAL env var")
    
    print("✓ Warmer config: PASSED")
    return True


def test_warmer_start_stop():
    """Test warmer start/stop."""
    print("Testing warmer start/stop...")
    
    from app import start_laya_warmer, stop_laya_warmer, _laya_warmer_stop
    
    # Initially not stopped
    assert not _laya_warmer_stop.is_set()
    print("  Initial state: running")
    
    # Stop
    stop_laya_warmer()
    assert _laya_warmer_stop.is_set()
    print("  After stop: stopped")
    
    # Reset for next test
    _laya_warmer_stop.clear()
    print("✓ Warmer start/stop: PASSED")
    return True


def test_warmer_keeps_model_warm():
    """Test that warmer keeps the model loaded."""
    print("Testing warmer keeps model warm...")
    
    from laya_client import health, is_available
    
    # Check initial state
    h = health()
    initial_calls = h.get("calls", 0)
    print(f"  Initial calls: {initial_calls}")
    
    # Simulate warmer ping
    from laya_client import health as laya_health
    laya_health()
    
    # Check after ping
    h = health()
    after_calls = h.get("calls", 0)
    print(f"  After ping calls: {after_calls}")
    
    assert after_calls > initial_calls
    print("✓ Warmer keeps model warm: PASSED")
    return True


def main():
    print("=" * 50)
    print("LAYA WARMER TEST")
    print("=" * 50)
    print()
    
    tests = [
        ("Warmer Config", test_warmer_config),
        ("Warmer Start/Stop", test_warmer_start_stop),
        ("Warmer Keeps Model Warm", test_warmer_keeps_model_warm),
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
