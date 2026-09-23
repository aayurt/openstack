# Laya Warmer Configuration

## Overview

The Laya warmer keeps the model loaded in memory to avoid cold-start latency.

## How It Works

1. **Background thread** runs every 120 seconds (configurable)
2. **Pings Laya** health endpoint to keep model loaded
3. **Prevents model unloading** due to inactivity

## Configuration

```bash
# Set warmer interval (default: 120 seconds)
export LAYA_WARMER_INTERVAL=60  # Ping every 60 seconds

# Disable warmer (not recommended)
export LAYA_ENABLED=false
```

## Performance Impact

| Metric | Cold Start | Warm (after ping) |
|--------|------------|-------------------|
| First call | ~1.3s | ~110ms |
| Steady state | — | ~110ms |
| Memory usage | — | ~50MB (MPS) |

## API Endpoints

```bash
# Check warmer status
curl http://localhost:3080/api/config
# Response:
# {
#   "laya_enabled": true,
#   "laya_available": true,
#   "laya_warmer_interval": 120,
#   "laya_warmer_active": true
# }

# Toggle warmer
curl -X POST http://localhost:3080/api/config \
  -H "Content-Type: application/json" \
  -d '{"laya_enabled": true}'
```

## Why This Matters

Without warmer:
- First Laya call after 5min idle: ~2-3s
- Subsequent calls: ~100ms
- User pays startup cost on first task

With warmer:
- All Laya calls: ~100ms
- No cold-start penalty
- Consistent performance

## Trade-offs

- **Memory**: ~50MB constant (MPS model loaded)
- **CPU**: Negligible (health check every 2min)
- **Network**: ~1KB per ping (minimal)

## Recommendation

Keep warmer enabled (default) for:
- Production deployments
- Interactive dashboards
- Frequent task execution

Disable warmer for:
- Resource-constrained environments
- Infrequent task execution
- Development/testing
