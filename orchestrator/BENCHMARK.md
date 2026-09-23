# Laya Integration Speed Test Results

## Benchmark Summary

| Metric | Old Flow | New Flow | Overhead |
|--------|----------|----------|----------|
| Task discovery to execution | ~1ms | ~126ms | +125ms |
| Pre-execution classification | — | ~126ms | +126ms |
| Post-test triage | — | ~77ms | +77ms |
| **Total overhead per task** | **~1ms** | **~194ms** | **+193ms** |

## Performance Characteristics

- **First call latency**: ~2-3s (model warmup)
- **Steady-state latency**: ~100-130ms per classification
- **Memory footprint**: ~50MB (MPS device)
- **Concurrent capacity**: Single-threaded (sequential requests)

## Impact Analysis

```
Typical task duration: 5-30 minutes (300-1800 seconds)
Laya overhead per task: ~0.19 seconds
Overhead percentage: 0.032% - 0.063%
```

**Conclusion**: The ~200ms Laya overhead is negligible compared to task execution time.

## Flow Comparison

### Old Flow (LAYA_ENABLED=false)
```
claim → plan → implement → verify
- Simple retry counter
- No complexity assessment
- No concurrency constraints
- Overhead: ~1ms per task
```

### New Flow (LAYA_ENABLED=true)
```
claim → Laya classify → plan → implement → verify → Laya triage
- Complexity-based concurrency
- Exponential backoff (60s, 120s, 240s)
- Escalation to Hermes on critical failures
- Overhead: ~194ms per task
```

## When to Use Each Flow

**Old flow**: Simple tasks, low retry rate, no concurrency issues

**New flow**: Complex projects, high retry rate, need for intelligent routing

## Toggle at Runtime

```bash
# Check current mode
curl http://localhost:3080/api/config

# Switch to old flow
curl -X POST http://localhost:3080/api/config \
  -H "Content-Type: application/json" \
  -d '{"laya_enabled": false}'

# Switch to new flow
curl -X POST http://localhost:3080/api/config \
  -H "Content-Type: application/json" \
  -d '{"laya_enabled": true}'
```
