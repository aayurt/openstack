# Laya MCP Integration Plan

## Overview

Laya is a **decision/triage layer** for tactical task decisions in the openstack-orchestrator pipeline.

```
                         HERMES
                    Strategic Planner
                           │
                           ▼
                  ┌─────────────────┐
                  │  ORCHESTRATOR   │
                  │                 │
                  │ Task Queue      │
                  │ Priority        │
                  │ Laya Decisions  │
                  │ Readiness       │
                  │ Worker Routing  │
                  │ Worktrees       │
                  │ Heartbeats      │
                  │ Retry Handling  │
                  └────────┬────────┘
                           │
                 ┌─────────┼─────────┐
                 ▼         ▼         ▼
             OpenCode   OpenCode   OpenCode
             Worker 1   Worker 2   Worker 3
                 │         │         │
                 └─────────┼─────────┘
                           ▼
                         Tests
                           │
                           ▼
                         LAYA
                      Post-test
                        Triage
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
             DONE         RETRY       ESCALATE
                                        │
                                        ▼
                                      HERMES
                                    Re-planning
```

## Two Flows

Toggle at runtime with `LAYA_ENABLED`:

### Old Flow (`LAYA_ENABLED=false`)

```
claim → plan → implement → verify → simple retry counter
```

- No Laya calls
- Simple retry counter (no backoff)
- No complexity assessment
- No concurrency constraints
- **Overhead: 0.007ms per task**

### New Flow (`LAYA_ENABLED=true`)

```
claim → Laya classify → plan → implement → verify → Laya triage → retry/escalate
```

- Laya pre-execution classification (complexity, priority, readiness)
- Laya post-test triage (pass/retry/escalate)
- Exponential backoff (60s, 120s, 240s)
- Large task concurrency constraint
- Escalation to Hermes on critical failures
- **Overhead: 176ms per task (0.03% of 10min task)**

### Toggle

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

## Architecture Notes

### Freebuff

**Not part of the orchestration pipeline.** If enabled, it is only an external LLM/token proxy at `:3457`.

### Laya Calls

Two calls per task lifecycle:

| Point | Laya Responsibility |
|-------|---------------------|
| `sync_tasks()` | Complexity, priority/readiness classification |
| `status_report()` | Test-result triage: pass/retry/escalate |

### Escalation Path

When Laya says "escalate":
1. Task status → `blocked`
2. Structured JSON written to `notify/`
3. Hermes drains `notify/` and re-plans
4. New/updated task created
5. Orchestrator picks up new task

## Implementation Status

### Phase 1: Laya Client Module ✅
- [x] Create `orchestrator/laya_client.py`
- HTTP client for Laya MCP at `http://127.0.0.1:8787`
- Methods: `classify_task()`, `triage_result()`, `health()`
- Structured JSON state format
- Input validation with fallbacks

### Phase 2: Schema Changes ✅
- [x] Add `complexity`, `laya_pre`, `laya_triage`, `retry_delay_seconds`, `laya_pre_version` columns
- [x] SQLite migrations in `db_init()`

### Phase 3: Pre-execution Classification ✅
- [x] Integrate `classify_task()` in `sync_tasks()`
- Classify new tasks on creation
- Reclassify on material changes (plan, acceptance, files, dependencies)
- Store results in `laya_pre` column
- Adjust priority based on Laya recommendation

### Phase 4: Readiness Check ✅
- [x] Integrate readiness check in `eligible_claim()`
- Skip tasks Laya marks as not ready
- Implement large task concurrency constraint

### Phase 5: Post-test Triage ✅
- [x] Integrate `triage_result()` in `status_report()`
- Classify verification failures
- Implement exponential backoff (60s, 120s, 240s)
- Escalate to Hermes when Laya says so
- Idempotent triage (skip if already triaged)

### Phase 6: Executor Updates ✅
- [x] Pass Laya results from executor to orchestrator
- [x] Include `laya_triage` in status report

### Phase 7: Tests ✅
- [x] Create `test_laya_integration.py` (unit tests)
- [x] Create `test_laya_unit.py` (mocked Laya)
- [x] Create `test_laya_e2e.py` (real Laya)
- [x] Manual curl verification

### Phase 8: Documentation ✅
- [x] Update `PLAN-laya-integration.md`
- [x] Create `WARMER.md`
- [x] Create `BENCHMARK.md`

## Key Design Decisions

1. **Laya sits in orchestrator** — centralized, not per-worker
2. **Structured JSON state** — frontmatter/plan/acceptance normalized
3. **Large tasks = concurrency constraint** — one worker at a time
4. **Exponential backoff** — 60s, 120s, 240s, max 3 retries
5. **Escalation = blocked + notify/** — structured JSON for Hermes
6. **Fallback** — if Laya unavailable, use safe defaults
7. **Idempotent decisions** — don't reclassify unless material change
8. **Validation** — log warning + fall back, don't block execution

## Files

| File | Action | Status |
|------|--------|--------|
| `orchestrator/laya_client.py` | CREATE | ✅ |
| `orchestrator/app.py` | MODIFY | ✅ |
| `executor/executor.py` | MODIFY | ✅ |
| `orchestrator/test_laya_integration.py` | CREATE | ✅ |
| `orchestrator/test_laya_unit.py` | CREATE | ✅ |
| `orchestrator/test_laya_e2e.py` | CREATE | ✅ |
| `orchestrator/WARMER.md` | CREATE | ✅ |
| `orchestrator/BENCHMARK.md` | CREATE | ✅ |

## Laya Decision Points

| Decision Point | Questions | Fallback |
|----------------|-----------|----------|
| Pre-execution | complexity, priority_adjust, ready | medium, keep, yes |
| Post-test | verdict, retry_reason | retry, real_bug |

## Validation

Laya output is validated before use:

```python
VALID_COMPLEXITY = {"trivial", "small", "medium", "large"}
VALID_PRIORITY_ADJUST = {"keep", "escalate", "de-escalate"}
VALID_READY = {"yes", "blocked_missing_info", "blocked_deps"}
VALID_VERDICT = {"pass", "retry", "escalate"}
VALID_RETRY_REASON = {"flaky_test", "real_bug", "env_issue"}
```

Invalid output → log warning + fall back to defaults.

## Idempotency

- **Classification**: Only on new task or material change (plan, acceptance, files, dependencies)
- **Triage**: Only once per verification result
- **Re-classification triggers**:
  - objective/plan changes
  - acceptance criteria change
  - relevant files/dependencies change
  - Hermes re-plans the task

## Speed Comparison

| Flow | Overhead | Impact on 10min task |
|------|----------|---------------------|
| Old (no Laya) | 0.007ms | 0.0000% |
| New (with Laya) | 176ms | 0.029% |

**Conclusion**: New flow adds ~176ms overhead per task, which is negligible for tasks lasting minutes/hours.

## Manual Verification

```bash
# Health check
curl http://127.0.0.1:8787/health

# Check config
curl http://localhost:3080/api/config

# Toggle Laya
curl -X POST http://localhost:3080/api/config \
  -H "Content-Type: application/json" \
  -d '{"laya_enabled": false}'

# Classify a task
curl -X POST http://127.0.0.1:8787/ask \
  -H "Content-Type: application/json" \
  -d '{"state": "{...}", "questions": {...}}'

# Triage a result
curl -X POST http://127.0.0.1:8787/ask \
  -H "Content-Type: application/json" \
  -d '{"state": "{...}", "questions": {...}}'
```
