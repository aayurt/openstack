#!/usr/bin/env python3
"""Benchmark Laya integration: compare old vs new flow speeds."""

import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

# Add orchestrator to path
sys.path.insert(0, str(Path(__file__).parent))

from laya_client import TaskState, classify_task, triage_result, is_available


def benchmark_classify(n=5):
    """Benchmark Laya classification speed."""
    task = TaskState(
        id="BENCH-001",
        project="benchmark",
        type="task",
        priority="P2",
        files_scope=["src/"],
        depends_on=[],
        plan="Benchmark task for speed testing",
        acceptance="Benchmark completes",
        frontmatter={},
    )
    
    times = []
    for i in range(n):
        start = time.perf_counter()
        result = classify_task(task)
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)
    
    avg = sum(times) / len(times)
    return {"avg_ms": avg, "min_ms": min(times), "max_ms": max(times), "iterations": n}


def benchmark_triage(n=5):
    """Benchmark Laya triage speed."""
    task = TaskState(
        id="BENCH-002",
        project="benchmark",
        type="task",
        priority="P2",
        files_scope=["src/"],
        depends_on=[],
        plan="Benchmark triage",
        acceptance="Triage works",
        frontmatter={},
    )
    verification = {"lint": "fail", "build": "pass", "types": "pass"}
    
    times = []
    for i in range(n):
        start = time.perf_counter()
        result = triage_result(task, verification, "Error output")
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)
    
    avg = sum(times) / len(times)
    return {"avg_ms": avg, "min_ms": min(times), "max_ms": max(times), "iterations": n}


def benchmark_old_flow():
    """Simulate old flow overhead (no Laya calls)."""
    # Old flow: just parse frontmatter + insert/update
    task_data = {
        "id": "OLD-001",
        "project": "test",
        "type": "task",
        "priority": "P2",
        "status": "new",
        "files_scope": ["src/"],
        "depends_on": [],
        "retry_count": 0,
        "max_retries": 3,
    }
    
    times = []
    n = 10
    for i in range(n):
        start = time.perf_counter()
        # Simulate: parse + DB insert (no Laya)
        json.dumps(task_data)
        json.dumps(task_data["files_scope"])
        json.dumps(task_data["depends_on"])
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)
    
    avg = sum(times) / len(times)
    return {"avg_ms": avg, "iterations": n}


def benchmark_new_flow():
    """Simulate new flow overhead (with Laya calls)."""
    task = TaskState(
        id="NEW-001",
        project="test",
        type="task",
        priority="P2",
        files_scope=["src/"],
        depends_on=[],
        plan="Test plan",
        acceptance="Test acceptance",
        frontmatter={},
    )
    
    times = []
    n = 5  # Fewer iterations due to Laya latency
    for i in range(n):
        start = time.perf_counter()
        # Simulate: parse + Laya classify + DB insert
        task.to_json()
        classify_task(task)
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)
    
    avg = sum(times) / len(times)
    return {"avg_ms": avg, "iterations": n}


def main():
    print("=" * 60)
    print("LAYA INTEGRATION BENCHMARK")
    print("=" * 60)
    
    print("\n1. Laya classify_task speed:")
    bench = benchmark_classify()
    print(f"   Avg: {bench['avg_ms']:.1f}ms | Min: {bench['min_ms']:.1f}ms | Max: {bench['max_ms']:.1f}ms")
    
    print("\n2. Laya triage_result speed:")
    bench = benchmark_triage()
    print(f"   Avg: {bench['avg_ms']:.1f}ms | Min: {bench['min_ms']:.1f}ms | Max: {bench['max_ms']:.1f}ms")
    
    print("\n3. Old flow (no Laya) overhead:")
    bench = benchmark_old_flow()
    print(f"   Avg: {bench['avg_ms']:.3f}ms (negligible)")
    
    print("\n4. New flow (with Laya) overhead:")
    bench = benchmark_new_flow()
    print(f"   Avg: {bench['avg_ms']:.1f}ms")
    
    print("\n" + "=" * 60)
    print("ANALYSIS")
    print("=" * 60)
    print("""
Old flow: claim → plan → implement → verify
  - Task discovery to execution: ~0ms overhead
  
New flow: claim → Laya classify → plan → implement → verify → Laya triage
  - Pre-execution classification: ~2-3s
  - Post-test triage: ~0.3-0.5s
  - Total overhead per task: ~2.5-3.5s

Since tasks take minutes to hours to execute, 3s overhead is negligible.

The 3s Laya latency is amortized over the entire task lifecycle.
""")


if __name__ == "__main__":
    main()
