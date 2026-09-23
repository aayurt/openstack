# OpenHands Agent Stack

A self-hosted autonomous-development stack on a single machine (macOS / Docker
Desktop, but works on any Docker host):

- **OpenClaw** (`:18789`) — the orchestrator agent: WebChat/Control UI,
  gateway API, and the hub that delegates coding to OpenCode.
- **OpenCode** (`:4096`, internal only) — a headless coding engine OpenClaw
  drives over REST via the bundled `skills/opencode/SKILL.md`.
- **OpenHands Agent Canvas** (`:8000`) — full agent web UI with the shared
  workspace + Docker sandboxing.
- **WireGuard ingress** (`wg-easy:15`, `51820/udp`) — private remote access to
  every service over the VPN; admin UI on `:51821`.
  (v15 rewrite: config lives in web UI; first boot seeded via `INIT_*` vars.)
- **PostgreSQL + Redis** on an internal network — reserved for future wiring.
- **On-demand browser sandbox** — disposable Chromium containers spawned by
  OpenClaw, for UI tests / screenshots; never persistent, never on
  `internal_net`.
- **Outbound VPN (gluetun, ProtonVPN free)** — every OpenCode call and every
  OpenClaw browser exits through a WireGuard tunnel. The public IP rotates on
  demand: `./scripts/rotate-ip.sh`.

Private by default: only `127.0.0.1` ports and the WireGuard UDP port are
exposed on the host. See `SECURITY.md`.

## Topology

```
                 ┌────────────────────────────────────────────────────────────┐
   LAN/Internet  │  Docker host                                                │
  ──51820/udp──► │   10.10.1.0/24  (vpn_net)                                  │
                 │   ┌─────────────────────────────────────────────────┐      │
                 │   │  wg-easy  .2  ← WireGuard peers (10.8.0.x)      │      │
                 │   │  admin :51821 (loopback + VPN)                  │      │
                 │   └─────────────────────────────────────────────────┘      │
                 │                                                            │
                 │   ┌──────────────────┐      ┌───────────────────────────┐  │
                 │   │  openclaw  .10   │ HTTP │  gluetun  .11 (ProtonVPN) │  │
                 │   │   :18789 WebChat ├──4096►│   + opencode (shares      │  │
                 │   │   egress_net .3  │      │   gluetun netns)          │  │
                 │   │                  │      │   · opencode  :4096       │  │
                 │   └────────┬─────────┘      │   · CONNECT proxy :8888   │  │
                 │            │ docker.sock    └──────────┬────────────────┘  │
                 │            ▼  (DooD)                   │ egress_net        │
                 │   ┌───────────────────┐                │ 10.30.0.0/24      │
                 │   │ sandbox containers│◄───────────────┘ (browser sandboxes│
                 │   │ (isolated, no net)│                  join egress_net    │
                 │   └───────────────────┘                  → :8888 proxy     │
                 │                                            → VPN egress)    │
                 │   ┌───────────────────┐  internal_net                      │
                 │   │ openhands .12     │  10.11.0.0/24                       │
                 │   │  :8000            ├──►  postgres .2 / redis .3          │
                 │   └───────────────────┘                                     │
                 └────────────────────────────────────────────────────────────┘
```

## Orchestrator Architecture

The orchestrator runs on the host (not in Docker) and manages the task pipeline:

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

### Two Flows

Toggle at runtime with `LAYA_ENABLED`:

**Old Flow** (`LAYA_ENABLED=false`):
```
claim → plan → implement → verify → simple retry counter
```
- No Laya calls
- Simple retry counter (no backoff)
- No complexity assessment
- **Overhead: 0.007ms per task**

**New Flow** (`LAYA_ENABLED=true`):
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

### Task Lifecycle

1. **Claim**: Worker claims task from queue
2. **Classify** (new flow only): Laya assesses complexity, priority, readiness
3. **Plan**: OpenCode generates implementation plan
4. **Implement**: OpenCode writes code
5. **Verify**: Tests run (lint, build, types)
6. **Triage** (new flow only): Laya decides pass/retry/escalate
7. **Complete**: Task marked done, or retried/escalated

## Requirements

- Docker Desktop (or Docker engine) with **compose v2** (`docker compose version`)
- `openssl` (macOS ships it)
- For remote access: a public IP/FQDN for `WG_HOST`

## Quickstart

```bash
cd OpenHands-agent-stack
./scripts/setup.sh        # .env + secrets + renders openclaw.json + builds images
docker compose up -d
./scripts/health-check.sh
```

`setup.sh` generates every secret — including a random wg-easy admin
password that it prints (save it). Then:

| What                | Where                                                            |
| ------------------- | ---------------------------------------------------------------- |
| OpenClaw WebChat    | http://127.0.0.1:18789 (or http://10.10.1.10:18789 over VPN)     |
| OpenHands           | http://127.0.0.1:8000  (or http://10.10.1.12:8000 over VPN)      |
| wg-easy admin       | http://127.0.0.1:51821 (or http://10.10.1.2:51821 over VPN)      |
| OpenCode API        | internal only — http://opencode:4096 (+ /doc for the OpenAPI)    |

Add a WireGuard client in wg-easy (admin panel), connect, and all services are
reachable at their `10.10.1.x` addresses. Peers live on `10.8.0.0/24`,
separate from the bridge subnets.

## How the pieces talk

- **OpenClaw → OpenHands (manager → executor)**: `skills/openhands/SKILL.md`
  drives the OpenHands agent-server REST API on `http://openhands:8000`
  (auth: `X-Session-API-Key` = `LOCAL_BACKEND_API_KEY`). OpenClaw assigns one
  repo task at a time from its memory queue, OpenHands runs the
  analyze/plan/modify/test/fix/report loop on the shared `projects/` mount,
  and OpenClaw reviews the result and decides the next step.
- **OpenClaw → OpenCode**: a skill (`skills/opencode/SKILL.md`) with two
  options: **A** drives `POST /session`, `POST /session/:id/message`,
  `prompt_async`, `status`, `message`, and `diff` on `http://opencode:4096`,
  authenticated with the `OPENCODE_SRV_PASSWORD` (basic auth, user `opencode`);
  **B** shells into the bundled CLI headless via
  `./scripts/opencode-cli.sh run|attach <repo> [model] 'TASK'`
  (`opencode run --pure --format json`, needs `--pure` because external
  plugins hang in this container). Both edit the shared `projects/` bind mount
  so every service sees changes immediately. Executor order (manager):
  OpenHands REST → OpenCode A → OpenCode B → OpenClaw itself.
- **OpenCode LLM**: defaults to the free **OpenCode Zen** model
  `opencode/big-pickle` over `https://opencode.ai/zen/v1`
  (`config/opencode.json`). Swap the model/provider there.
- **OpenClaw LLM**: set `OPENCLAW_MODEL` + the matching provider key in `.env`.
  Defaults to OpenCode Zen via the custom `zen` provider
  (`zen/nemotron-3-ultra-free`): `config/openclaw.json` defines an
  OpenAI-compat provider that sends Zen bare model ids plus the required
  `x-opencode-session` header (`.env` `OPENCLAW_X_SESSION`, auto-generated by
  `scripts/setup.sh`). A **free-pool fallback chain** rides out Zen
  502/429s via the same `/v1` path:
  `agents.defaults.model.fallbacks = ["zen/nemotron-3.5-lightning-free",
  "zen/ling-3.0-flash-fin-free"]` (both verified to serve tool-carrying
  requests). Do **not** add `zen/big-pickle` here: pool 429s on that route are
  classified as an auth/billing failure and temporarily disable the whole
  `zen` provider key (~10 min), taking the primary down with it. The built-in
  `opencode` provider is **not** used for
  primary LLM calls because it mixes the local `opencode/<model>` namespace,
  which the Zen `/v1` gateway rejects — but it works as a last-resort fallback
  (native endpoint). OpenClaw *does* call local `opencode` on `:4096`
  through the skill above for coding tasks.
- **OpenHands**: standalone UI + Docker sandboxing on `projects/`; its LLM is
  preconfigured to use **OpenCode Zen** (see "OpenHands LLM" below). Uses its
  default SQLite store — PostgreSQL is not prewired because agent-canvas DB
  support is version-specific; see "Reserved services".
- **Browser sandbox**: OpenClaw spawns a disposable Chromium container only
  when a sandboxed session uses the browser tool. For the *sandboxed browser*
  container, build `openclaw-sandbox-browser` from an OpenClaw source checkout
  (`scripts/sandbox-browser-setup.sh`); the main session uses the Chromium
  baked into the `-browser` gateway image.

## Linking an external repo

Point the whole flow at a repo that lives outside `projects/` by bind-mounting
it into the same shared tree (three identical lines in `docker-compose.yml`):

```yaml
openhands:
  volumes: ["$HOME/Projects/supreme/syasyah-samaj:/projects/syasyah-samaj"]
opencode:
  volumes: ["$HOME/Projects/supreme/syasyah-samaj:/projects/syasyah-samaj"]
openclaw:
  volumes: ["$HOME/Projects/supreme/syasyah-samaj:${PWD}/projects/syasyah-samaj"]
```

Then `docker compose up -d openhands opencode openclaw`. From then on:

- OpenClaw delegates with `working_dir: projects/syasyah-samaj`
  (OpenHands conversation) and reviews the repo inside its own workspace.
- The opencode fallback sees the same files.
- Seed the queue: "queue these tasks for syasyah-samaj …" — entries land in
  OpenClaw's memory as `openhands/queue/syasyah-samaj`.

Mounts are strictly path-scoped (one repo per mount); the repo's own `.env` is
visible to the engines, so keep secrets out of the mounted tree — see
`SECURITY.md` item 5.

## OpenHands LLM (OpenCode Zen)

OpenHands' agent-server keeps its LLM config server-side (backend
`PATCH /api/settings`) — it is **not** set via compose env. The stack
preconfigures it to the same model opencode uses:

| Field     | Value                                   |
| --------- | --------------------------------------- |
| model     | `openai/nemotron-3-ultra-free`          |
| base URL  | `https://opencode.ai/zen/v1`            |
| api key   | your `OPENCODE_API_KEY` (or none)       |

Zen is OpenAI-compatible there, so OpenHands treats it as a custom
`openai/` provider. The default model is the free **Nemotron 3 Ultra Free**,
not `big-pickle`: Zen marks big-pickle "free tier can only be used in
OpenCode", so API clients like LitellM/OpenHands are refused
(`litellm.BadRequestError ... free tier can only be used in OpenCode`).
big-pickle remains available inside the opencode server itself. Apply/patch
it at any time with:

```bash
./scripts/set-openhands-llm.sh     # reads OPENCODE_API_KEY from .env
```

Create a key at https://opencode.ai/zen → **Create API Key**, set
`OPENCODE_API_KEY` in `.env`, then re-run the script. The key is shared with
the headless opencode server too — restart it to pick it up:

```bash
docker compose up -d --force-recreate opencode
```

**Why a key is effectively required:** keyless (guest) Zen calls work on the
bare HTTP API but hit the shared free pool (`429 FreeUsageLimitError`), and
OpenHands' LiteLLM can't send keyless requests at all, so without a key the
OpenHands agent won't complete a turn. The same step can also be done by hand
in the UI: Settings → LLM → add an OpenAI-compatible provider with the three
values above.

**Zen session headers (required since 2026-09):** Zen rejects API requests
that lack an `x-opencode-session` header with
`MissingSessionID ... "OpenCode's free tier can only be used in OpenCode"` —
even with a valid key and regardless of model. OpenHands forwards custom
headers via the LLM config's `extra_headers`, so `set-openhands-llm.sh`
injects `x-opencode-session` (plus `x-opencode-client` / `-project` /
`-request`) into the profile; a profile edited outside the script (e.g. the
UI wizard) must include them or every turn will fail with that error.

## Outbound VPN & IP rotation

`gluetun` runs a ProtonVPN **free-tier WireGuard** tunnel. Two egress paths use
it:

- **OpenCode** shares gluetun's network namespace (`network_mode:
  service:gluetun`) — *all* of its traffic leaves via the tunnel.
- **Every openclaw browser** (main session + sandbox Chromium) is launched with
  `--proxy-server=http://10.30.0.2:8888` (`egress_net`), gluetun's built-in
  HTTP CONNECT proxy, so browser egress is the tunnel too.

To activate the tunnel put your ProtonVPN WireGuard private key in `.env`:

1. Generate a config at https://account.proton.me/u/0/vpn/WireGuard
   (any server; one key covers them all).
2. Set `WIREGUARD_PRIVATE_KEY=<PrivateKey>` in `.env`.
3. `docker compose up -d --force-recreate gluetun opencode`

`WIREGUARD_PRIVATE_KEY` empty ⇒ gluetun stays down and opencode has no egress
(a deliberate safe default). The rotation pool and active pin live in `.env`
(`GLUETUN_COUNTRIES` / `GLUETUN_HOSTNAMES` define the pool; one of
`GLUETUN_COUNTRY` / `GLUETUN_HOSTNAME` is the active pin — hostname wins).

Rotate the public IP on demand:

```bash
./scripts/rotate-ip.sh     # picks the next country/hostname, recreates
                           # gluetun + opencode, verifies the new egress IP
```

Verification: `./scripts/health-check.sh` shows the current egress IP, or ask
gluetun directly: `docker compose exec gluetun wget -qO- --user=gluetun
--password=<GLUETUN_CONTROL_PASSWORD> http://127.0.0.1:8000/v1/publicip/ip`.

## Scripts

| Script                       | Purpose                                             |
| ---------------------------- | --------------------------------------------------- |
| `scripts/setup.sh`           | idempotent bootstrap (secrets, config, images)      |
| `scripts/health-check.sh`    | per-service health probes                           |
| `scripts/smoke-test.sh`      | full wiring test; add `SMOKE_E2E=1` for an LLM turn (`SMOKE_E2E_MODEL=nemotron-3-ultra-free` to bypass a rate-limited default) |
| `scripts/rotate-ip.sh`       | rotate the VPN egress IP (next pool server)         |
| `scripts/set-openhands-llm.sh`| point OpenHands' LLM at OpenCode Zen (model + key) |

## Reserved services (not yet wired)

PostgreSQL and Redis run for the stack's future (queues, shared state,
metrics) but no service consumes them yet. The block below is intentionally
NOT in `docker-compose.yml` — OpenHands uses in-process SQLite until DB
support is verified for its image.

```yaml
# openhands:
#   environment:
#     - DATABASE_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
#     - REDIS_URL=redis://redis:6379
```

## Troubleshooting

- `docker compose logs -f openclaw` — gateway/schema errors (`openclaw.json`
  is strictly validated; run `openclaw doctor` inside the container:
  `docker compose exec openclaw openclaw doctor`).
- `config/openclaw.json` is a **template**; the real file is
  `.openclaw/openclaw.json` (host-absolute workspace, required for DooD).
  Rerun `setup.sh` after moving the stack.
- OpenCode API shape is authoritative at `GET /doc` on its container.
- WireGuard "no handshake"? Set `WG_HOST` in `.env` (or fix it in the Admin
  Panel: Settings -> WireGuard) and check `51820/udp` reachability.
- Healthchecks: `./scripts/health-check.sh`.

## Out of scope (deliberately)

Outbound VPN, public reverse proxy / TLS, Tailscale, Kubernetes, auto git
push, permanent browser containers, multiple orchestrator instances.