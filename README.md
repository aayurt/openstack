# OpenStack Orchestrator

Distributed task orchestrator with Laya MCP integration for autonomous development.

## Architecture

```
                         HERMES
                    Strategic Planner
                           │
                           ▼
                  ┌─────────────────┐
                  │  ORCHESTRATOR   │
                  │  :3080 (FastAPI)│
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

## Prerequisites

- **Python 3.10+** with `pip`
- **Docker Desktop** (or Docker engine) with **compose v2**
- **OpenCode** running on `:4096` (headless coding engine)
- **Laya MCP** running on `:8787` (decision/triage layer)
- **SQLite** (included with Python, no setup needed)
- **WireGuard** (optional, for remote access)

### Install Python Dependencies

```bash
cd orchestrator
pip install -r requirements.txt
```

## Quickstart

```bash
cd orchestrator

# Start orchestrator
python app.py

# Verify health
curl http://localhost:3080/health

# Check Laya connection
curl http://localhost:3080/api/config
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

## Task Lifecycle

1. **Claim**: Worker claims task from queue
2. **Classify** (new flow only): Laya assesses complexity, priority, readiness
3. **Plan**: OpenCode generates implementation plan
4. **Implement**: OpenCode writes code
5. **Verify**: Tests run (lint, build, types)
6. **Triage** (new flow only): Laya decides pass/retry/escalate
7. **Complete**: Task marked done, or retried/escalated

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/api/config` | Get config (laya_enabled, laya_available) |
| `POST` | `/api/config` | Update config |
| `GET` | `/api/tasks` | List tasks |
| `POST` | `/api/claim` | Claim a task |
| `POST` | `/api/plan` | Submit plan |
| `POST` | `/api/status` | Report status |
| `GET` | `/api/notify` | Drain notifications |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LAYA_ENABLED` | `true` | Enable/disable Laya integration |
| `LAYA_URL` | `http://127.0.0.1:8787` | Laya MCP endpoint |
| `PORT` | `3080` | Orchestrator port |
| `DB_PATH` | `orchestrator.db` | SQLite database path |

### .env File

```bash
# Laya
LAYA_ENABLED=true
LAYA_URL=http://127.0.0.1:8787

# Orchestrator
PORT=3080
DB_PATH=orchestrator.db
```

## Database Schema

```sql
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    project TEXT,
    type TEXT,
    priority TEXT,
    objective TEXT,
    plan TEXT,
    acceptance TEXT,
    files_scope TEXT,
    depends_on TEXT,
    status TEXT DEFAULT 'new',
    complexity TEXT,
    laya_pre TEXT,
    laya_triage TEXT,
    retry_count INTEGER DEFAULT 0,
    retry_delay_seconds INTEGER,
    laya_pre_version TEXT,
    worker_id TEXT,
    worktree_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Laya Integration

### Decision Points

| Point | Questions | Fallback |
|-------|-----------|----------|
| Pre-execution | complexity, priority_adjust, ready | medium, keep, yes |
| Post-test | verdict, retry_reason | retry, real_bug |

### Validation

Laya output is validated before use:

```python
VALID_COMPLEXITY = {"trivial", "small", "medium", "large"}
VALID_PRIORITY_ADJUST = {"keep", "escalate", "de-escalate"}
VALID_READY = {"yes", "blocked_missing_info", "blocked_deps"}
VALID_VERDICT = {"pass", "retry", "escalate"}
VALID_RETRY_REASON = {"flaky_test", "real_bug", "env_issue"}
```

Invalid output → log warning + fall back to defaults.

### Idempotency

- **Classification**: Only on new task or material change (plan, acceptance, files, dependencies)
- **Triage**: Only once per verification result
- **Re-classification triggers**:
  - objective/plan changes
  - acceptance criteria change
  - relevant files/dependencies change
  - Hermes re-plans the task

## Testing

### Unit Tests (Mocked Laya)

```bash
cd orchestrator
python -m unittest test_laya_unit -v
```

### E2E Tests (Real Laya)

```bash
cd orchestrator
python -m unittest test_laya_e2e -v
```

### All Tests

```bash
cd orchestrator
python -m unittest discover -p "test_*.py" -v
```

## Speed Comparison

| Flow | Overhead | Impact on 10min task |
|------|----------|---------------------|
| Old (no Laya) | 0.007ms | 0.0000% |
| New (with Laya) | 176ms | 0.029% |

**Conclusion**: New flow adds ~176ms overhead per task, which is negligible for tasks lasting minutes/hours.

## Scripts

| Script | Purpose |
|--------|---------|
| `app.py` | Main orchestrator |
| `laya_client.py` | Laya MCP client |
| `test_laya_unit.py` | Unit tests (mocked Laya) |
| `test_laya_e2e.py` | E2E tests (real Laya) |
| `test_laya_integration.py` | Integration tests |
| `benchmark_laya.py` | Speed benchmark |
| `test_flows.py` | Old vs new flow comparison |

## Troubleshooting

- **Laya unavailable**: Check `curl http://127.0.0.1:8787/health`
- **Orchestrator not starting**: Check port 3080 is free
- **Tasks stuck**: Check `curl http://localhost:3080/api/tasks`
- **Tests failing**: Ensure Laya is running for E2E tests

## Out of Scope

- Multiple orchestrator instances
- Horizontal scaling
- External databases (SQLite only)
- Kubernetes deployment
