# High-Concurrency Flash Sale Architecture (goML Technical Case Study)

> **Zero overselling by construction: stock is reserved atomically in Redis and re-checked by the database.**  
> *Role Evaluation: Full-Stack Software Engineer*  

![Python](https://img.shields.io/badge/Python-3.12-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-green.svg)
![Redis](https://img.shields.io/badge/Redis-Streams%20%2B%20Lua-red.svg)
![Streaming](https://img.shields.io/badge/Real--Time-SSE%20Streaming-purple.svg)
![First visit](https://img.shields.io/badge/First%20visit-81KB-brightgreen.svg)
![Status](https://img.shields.io/badge/Audit-100%25%20Consistent-success.svg)

---

## 🚀 Project Overview & Architecture

```
                 ┌──────────────┐   healthy replicas only    ┌────────────────────────┐
  browsers ────▶ │    NGINX     │ ─────────────────────────▶ │  API replicas (2 → 8)  │ ──┐
                 │ edge + LB +  │ ◀── graceful reload ──┐    │  FastAPI, stateless    │   │ Lua: reserve
                 │ per-IP limit │                       │    └────────────────────────┘   │ + enqueue
                 └──────────────┘                ┌──────┴─────┐                           ▼ (atomic)
                                                 │ autoscaler │ scales on CPU      ┌─────────────┐
                                                 └────────────┘                    │    Redis    │
                                                                                   │ stock+stream│
                 ┌────────────────────────┐   XREADGROUP / XAUTOCLAIM               └──────┬──────┘
                 │ order workers (2)      │ ◀──────────────────────────────────────────────┘
                 │ micro-batch consumers  │ ──▶ conditional SQL (stock > 0) ──▶ Postgres (ACID)
                 └────────────────────────┘
```

| Layer | What it does |
|---|---|
| **Client** | Locks the button on the first click, retries safely, shows a server-synced countdown, tracks the order over SSE (adaptive polling fallback). First visit is 81 KB. |
| **NGINX** | Serves static files, rate-limits per IP, load-balances (`least_conn`) over the *healthy* replicas, retries connection failures on another replica. |
| **Autoscaler** | HPA-style control loop: scales the API between 2 and 8 replicas on CPU, adds a replica to NGINX only when healthy, drains it before removal. |
| **API replicas** | Stateless. One atomic Lua script reserves stock, records the claim and enqueues the order; answers `202` + a tracking URL. |
| **Workers** | Separate containers. Drain the Redis Stream in micro-batches into Postgres with `UPDATE … WHERE stock > 0`; reclaim stuck messages with `XAUTOCLAIM`. |

```
flash-sale-system/
├── backend/
│   ├── app/
│   │   ├── main.py              # API: checkout (202), SSE, product status, admin, demo endpoints
│   │   ├── config.py            # Settings (env-driven)
│   │   ├── database.py          # SQLAlchemy async engine (Postgres in compose, SQLite locally)
│   │   ├── models.py            # Product & Order models with strict unique indexes
│   │   ├── redis_client.py      # Redis client (blocking pool), Lua loader, sale window
│   │   ├── rate_limiter.py      # Redis-backed per-IP limiter shared by all replicas
│   │   ├── schemas.py           # Pydantic request/response schemas
│   │   ├── worker.py            # Stream consumer: micro-batches, idempotent, XAUTOCLAIM
│   │   ├── worker_main.py       # Entrypoint for the dedicated worker containers
│   │   └── lua_scripts/reserve_stock.lua   # Atomic: sale window, idempotency, stock, enqueue
│   ├── tests/
│   │   ├── test_basic.py / test_protection.py   # 8 integration tests
│   │   ├── load_test.py         # 3,000-request correctness test (500 sold, 0 oversold)
│   │   ├── sustained_load.py    # Sustained load; classifies served / throttled / DROPPED
│   │   └── oversell_demo.py     # Naive SELECT-then-UPDATE vs this system, side by side
│   └── Dockerfile
├── autoscaler/                  # autoscaler.py: CPU-based scaling + NGINX upstream controller
├── frontend/                    # 81 KB first visit
│   ├── index.html               # Critical CSS is inlined by scripts/inline_css.py
│   ├── styles.css               # Source of truth for styles (run scripts/inline_css.py after editing)
│   ├── app.js
│   └── assets/                  # goml-logo.webp, phone.webp/jpg, fonts/ (self-hosted subsets)
├── nginx/nginx.conf
├── scripts/                     # inline_css.py (critical CSS), distributed_load.sh (100k-request test)
├── docker-compose.yml           # NGINX + autoscaled API + workers + autoscaler + Redis + Postgres
└── docs/                        # ARCHITECTURE_SYSTEM_DESIGN.md, INTERVIEW_DEFENSE_GUIDE.md
```

---

## ⚡ Quick Start

### Full topology (recommended)
```bash
# Windows PowerShell:
$env:DEMO_MODE=1; docker compose up -d --build

# macOS / Linux / Git Bash:
DEMO_MODE=1 docker compose up -d --build
```
Open **http://localhost/**. Starts NGINX, 2 API replicas (autoscaled up to 8), 2 workers, the autoscaler, Redis and Postgres.
Watch scaling decisions with `docker logs -f flash_autoscaler`.

| Setting (env) | Default | Purpose |
|---|---|---|
| `DEMO_MODE` | `0` | Enables unlimited demo/naive endpoints and tokenless `/admin/reset`. **Never in production.** |
| `ADMIN_TOKEN` | empty | `X-Admin-Token` that allows `/api/v1/admin/reset` when demo mode is off. |
| `MIN_REPLICAS` / `MAX_REPLICAS` | `2` / `8` | Autoscaling bounds for the API tier. |
| `TARGET_CPU_PERCENT` | `50` | Average CPU the autoscaler aims for (100 = one full core per replica). |
| `SCALE_DOWN_STABILIZATION_SECONDS` | `60` | Scale down only to the highest recommendation seen in this window. |
| `WORKER_REPLICAS` | `2` | Order-persistence worker containers. |

### Schedule the 8:00 PM opening
The sale window lives in Redis and is enforced **inside the Lua script using the Redis server clock**, so a wrong device clock can't open the sale early.
```bash
curl -X POST "http://localhost/api/v1/admin/reset?starts_in=60"   # opens in 60 s (needs DEMO_MODE=1 or the admin token)
```
The page shows a countdown and keeps the button locked until the server says the sale is live. In the Audit lab, "Reset, open in 30 s" does the same.

### Local single process (no Docker)
```bash
cd backend && python -m uvicorn app.main:app --host 0.0.0.0 --port 8000   # needs Redis; in-process worker, SQLite
```

### Tests and measurements
```bash
cd backend && REDIS_DB=1 DATABASE_URL=sqlite+aiosqlite:///./t.db python -m pytest        # 8 tests
# Inside the compose network (see docs for the exact docker run):
python tests/load_test.py          # correctness: 3,000 requests -> exactly 500 sold
python tests/sustained_load.py     # sustained load with drop accounting (TARGET_URL, DURATION, CONCURRENCY)
python tests/oversell_demo.py      # reproduces the case study's oversell bug, then shows this system
scripts/distributed_load.sh 5 20000 400   # 100,000 requests from 5 synchronized containers
python scripts/inline_css.py       # after editing frontend/styles.css
```

---

## 📊 Measured Results

All numbers are from this repository on one Windows laptop (12 cores), with the load generator running on the same machine in Docker.

### Time-to-Interactive (Lighthouse 12.6, 3 runs per profile, cold load)
| Profile | TTI | FCP | LCP | Blocking time | CLS | Score | Transferred |
|---|---|---|---|---|---|---|---|
| **Mobile**: Moto G, simulated slow 4G (150 ms RTT, 1.6 Mbps), 4× CPU slowdown | **1,458 ms** (runs: 1,458 / 1,456 / 1,466) | 1,067 ms | 1,451 ms | 0 ms | 0 | 100 | 81 KB |
| **Desktop** | **375 ms** | 301 ms | 375 ms | 0 ms | 0 | 100 | 81 KB |

The requirement is under 1.5 s. The mobile profile is the strict one and passes **by about 40 ms**, so there is little headroom; HTTP/2 and a real CDN would add margin. On this machine, unthrottled, FCP/LCP are about 138 ms. Script evaluation is 31 ms, so the remaining time is simulated network round trips.

### 0% dropped requests (through NGINX, with autoscaling happening during the run)
A request counts as **dropped** if it ends in a connection error, a timeout, a 5xx, or any status other than 202/403/409/410 (429 from the per-IP limiter is reported separately as *throttled*, because refusing bad actors is intended).

| Phase | Requests | Duration / load | Replicas | Dropped |
|---|---|---|---|---|
| Surge | 159,278 | 75 s, 600 concurrent, 2,122 req/s, p50 251 ms, p99 739 ms | 2 → 4 → 8 | **0** |
| Trickle during scale-down | 88,473 | 170 s, 1 client, 520 req/s | 8 → 3 → 2 | **0** |

NGINX closes idle keep-alive connections when it reloads, so a few requests per scaling event reach a closed connection (187 in the surge, 1 in the trickle). The load generator retries those once, like a browser, and reports them as "recovered by retry". Retrying is safe because checkout is idempotent. Without that retry the surge showed 251 `ServerDisconnectedError`s.

Getting to zero took four fixes, each found by reading logs rather than guessing:
1. NGINX passive health checks marked **every** replica down during overload (`no live upstreams`, a wall of 502s). The upstream is now controlled by the autoscaler: only healthy replicas, drained before removal.
2. The Redis client pool ran out of connections (`Too many connections` → HTTP 500). It is now a blocking pool, so a burst waits instead of failing.
3. uvicorn closed keep-alive connections (5 s) before NGINX's (60 s) did. uvicorn is now at 75 s.
4. Connection failures to just-removed replicas are retried on another replica.

### Correctness and the oversell bug
| Test | Result |
|---|---|
| 3,000 requests across replicas | 500 × 202, 2,500 × 410, **500 orders saved, 0 oversold**, audit consistent |
| 182,000 requests during scale-up | exactly 500 saved, 0 oversold |
| **Naive SELECT-then-UPDATE** (same load), concurrency 10 / 40 / 100 | oversold by **8 / 29 / 59** |
| **This system**, same load | oversold by **0** every time |

Oversell grows with concurrency because each in-flight request has already read a stale "stock > 0". The case study's 540 is that effect at roughly 40 in-flight requests.

### Event-loop safety (measured)
Each API replica reports how late a 100 ms timer fires (`GET /health` → `event_loop_lag_ms`). A handler that blocked the loop (sync I/O, heavy CPU) would show stalls of seconds.

| Load | CPU per replica | Median lateness | p99 | Worst |
|---|---|---|---|---|
| Idle | about 0% | 2 ms | 6 ms | 7 ms |
| At the autoscaler's target (1,324 req/s across 5 replicas, 0 dropped) | about 45% | 0.2 to 0.3 ms | 1 to 6 ms | 46 ms |
| Saturated (1,500 clients, 8 replicas) | 67 to 86% | 0.6 to 0.7 ms | 18 to 57 ms | 130 ms |

Under saturation the lag is queueing behind other requests' work, not blocking; the autoscaler exists to keep replicas near 50% CPU. A source search also finds no blocking calls (`time.sleep`, sync HTTP/DB drivers, subprocess) on the request path.

### 100,000 requests (`scripts/distributed_load.sh`)
| Run (API pre-scaled to 8 replicas, 5 synchronized generators) | Time for 100,000 requests | Throughput | Dropped |
|---|---|---|---|
| 1 | 42.1 s | 2,377 req/s | **0** |
| 2 | 38.0 s | 2,633 req/s | **0** |
| 3 | 38.2 s | 2,616 req/s | **0** |
| 4 (autoscaler grew the tier to 12 mid-run) | 32.7 s | 3,059 req/s | **0** |
| 5 | 37.6 s | 2,662 req/s | **0** |
| `scripts/distributed_load.sh` (final check) | 36.3 s | 2,756 req/s | **0** |

**All 600,000 requests across six runs were served; none were dropped.** But 100,000 requests took about 36 to 42 seconds, not 10 (about 4× too slow for the case study's headline target).

Where the CPU went at the ceiling (10.2 of this laptop's 12 logical cores busy):

| Component | CPU (percent of one core) |
|---|---|
| 8 API replicas | 814% |
| Load generators | 145% |
| NGINX | 38% |
| Redis | 18% |
| Postgres | 4% |

So the **Python API tier is the bottleneck** (about 330 req/s per replica, roughly 3 ms of CPU per request), while the stateful parts have lots of headroom. Growing to 12 replicas was no faster, because the host itself was saturated. Rewriting the rate limiter as plain ASGI and turning off uvicorn's access log made no measurable difference here.

Extrapolation, clearly **not demonstrated**: 10,000 req/s needs roughly 30 API cores (about 4 times this laptop), and Redis would sit near 70% of one core at that rate. That is a handful of ordinary cloud instances, and the API tier is stateless, so it scales by adding replicas. For a known 8 PM launch you would pre-scale (`MIN_REPLICAS`) instead of waiting for the autoscaler to react.

---

## ✅ Requirements Coverage (case study → this repo)

| Case-study requirement | How it is met | Where / evidence |
|---|---|---|
| Frontend bundle < 200 KB | 81 KB first visit (HTML with inlined critical CSS, JS, WebP image, subset fonts) | `frontend/`, Lighthouse run below |
| Time to Interactive < 1.5 s | Mobile 1,458 ms (simulated slow 4G), desktop 375 ms | Measured Results |
| Click-spam prevention | Button locks on first click, `isSubmitting` guard, server-side idempotency per user | `frontend/app.js`, `reserve_stock.lua` |
| Graceful loading / failed queues | Optimistic UI with rollback, step tracker, retry with jittered backoff, SSE with adaptive-polling fallback | `frontend/app.js` |
| Rate-limit bad actors | NGINX `limit_req` per IP plus a Redis limiter shared by all replicas (no header bypass) | `nginx/nginx.conf`, `rate_limiter.py` |
| 0% dropped requests | 600,000 requests across six runs plus a final 20,000-request check on a clean rebuild: 0 dropped, including during autoscaling | `tests/sustained_load.py`, `scripts/distributed_load.sh` |
| Distribute load safely | NGINX `least_conn` over health-checked replicas, autoscaled 2 to 8, drain before removal | `nginx/`, `autoscaler/` |
| Decouple DB writes | API only touches Redis (202 Accepted); workers persist from a Redis Stream | `worker.py`, `worker_main.py` |
| Stateless clusters, event-loop safety | No in-process state; all I/O async; event-loop lag measured per replica (`/health`) | Event-loop section below |
| Exactly 500 sold, ACID, atomic decrements | One Redis Lua script (sale window, idempotency, `DECRBY`, enqueue) plus conditional SQL `UPDATE … WHERE stock > 0` | `reserve_stock.lua`, `/admin/stats` audit |
| Reproduce / explain the 540 oversell | Naive SELECT-then-UPDATE endpoint vs this system, side by side | `tests/oversell_demo.py` |
| 8:00 PM opening | Server-driven sale window enforced inside Lua with the Redis clock; countdown synced to server time | `?starts_in=` reset, UI countdown |

## 🧰 Feature Summary

- **UI:** goML-branded, 500-unit live "wall" (one tile per phone turns grey as stock drops, your own unit is highlighted), server-synced countdown, ticket tracker (queued, confirmed), minimal scrollable Audit Lab drawer.
- **Audit Lab:** live reconciliation of Redis stock, database stock, in-transit queue and confirmed orders with a consistency banner; "Simulate 500-user rush" and "Reset" buttons (demo mode only).
- **Demo mode (`DEMO_MODE=1`):** lets one browser fire 500 concurrent orders from one IP by bypassing the per-IP limits on a dedicated demo route; off by default, never for production.
- **Security hardening:** admin reset needs a token (or demo mode), constant-time token compare, client-IP headers trusted only behind NGINX, no User-Agent bypass.
- **Resilience:** worker crash recovery via `XAUTOCLAIM`, idempotent retries, NGINX retries on another replica, Redis connection pool with back-pressure, health checks.
- **Autoscaling:** HPA-style controller (target 50% CPU, scale up at most 2x at once, scale down after a 60 s stabilization window), NGINX upstream updated and gracefully reloaded.

## 🧪 Tests Performed

| Test | What it shows | Result |
|---|---|---|
| `pytest` (8 integration tests) | Atomic enqueue, no UA bypass, admin/demo gating, crash reclaim, repeat claim returns original ticket, sale window blocks early purchases | 8 passed |
| `oversell_demo.py`, 3,000 buyers, 300 at a time | Naive code vs this system | Naive: 529 orders (**29 oversold**). This system: **exactly 500, 0 oversold, consistent** |
| `load_test.py` | 3,000 requests for 500 units | 500 sold, 0 oversold |
| `sustained_load.py` through NGINX with autoscaling | Dropped-request accounting (served / throttled / DROPPED) | 0 dropped; scaled 2 to 12 replicas with no errors |
| `distributed_load.sh` (100,000 requests, 5 generators) | Capacity and drops | 0 dropped in 6 runs; 33 to 42 s per run (target of 10 s **not** met, see above) |
| Lighthouse (3 runs per profile) | TTI and payload | Mobile 1,458 ms, desktop 375 ms, 81 KB |
| Event-loop lag monitor | No blocked loop | p99 under 60 ms even when saturated |
| Browser check (Playwright) | Wall tiles update on claim, drawer scrolls, countdown unlocks the button, reset flows | Passed |
| Clean rebuild (`docker compose down -v`, then `up --build`) | Everything starts from scratch | All containers healthy; oversell demo and 20,000-request run passed |

### Suggested manual checks
1. Start the stack (commands in Quick Start), open http://localhost/, click **Claim your phone**: one tile turns grey and the ticket moves to Confirmed.
2. Open **Audit Lab**, press **Simulate 500-user rush**: stock goes to 0, "Oversold" stays 0, the banner says consistent.
3. **Reset, open in 30 s**: the button stays locked until the countdown ends, then unlocks.
4. Run `docker logs -f flash_autoscaler` during a load test to watch replicas scale.

## 🧭 Design Notes

- **Two stock counters (Redis and the database)?** Redis is the high-speed gate: it decides who gets one of the 500 tokens. The database is the durable ledger, and its conditional `UPDATE … WHERE stock > 0` guarantees it can never go negative even if Redis were wrong. The audit endpoint reconciles them. The case study warns against application-level counters, meaning in-process variables or read-modify-write counters. Neither exists here.
- **N+1 queries.** The failing system issued per-request queries (read the product, then check, then write, then read orders) so cost grew with requests, and every one contended on the same row. Here the hot path makes zero database calls (one Redis script), and the worker does one batched `UPDATE` plus one `IN (…)` select per micro-batch instead of a query per order. Only the rare fallback path is per-message.
- **CDN.** NGINX stands in for the edge: `/assets` are served `immutable` for 30 days, and HTML/JS are `no-cache` with ETags. In production, put CloudFront or Cloudflare in front and fingerprint filenames.
- **Retries are safe** because claims are keyed by `user_id` inside the Lua script: a repeat returns the original ticket (409) instead of reserving a second unit.

### Known limitations
- NGINX and Redis are single instances (single points of failure). Production would use an ALB pair and Redis with replicas and Sentinel.
- Each SSE connection polls Redis every 0.5 s; at very large scale this should become Redis pub/sub.
- `user_id` is client-supplied, so one-per-customer relies on authentication in a real deployment; per-IP limits are the only abuse control here.
- If the database rejects an already-granted claim, the unit is not returned to Redis (under-selling, never overselling).
- The autoscaler uses the Docker socket, which is root-equivalent. It is a stand-in for a Kubernetes HPA.
- Workers are fixed at 2; scale them on stream lag if persistence becomes the bottleneck.
- The Python API costs about 3 ms of CPU per request. A Go or Node service would likely serve more per core; here we compensate by scaling out.

---

## 📚 Key Technical Documentation

* 📖 **[System Design & Technical Architecture](docs/ARCHITECTURE_SYSTEM_DESIGN.md)**
* 🎙️ **[Interview Defense & Presentation Guide](docs/INTERVIEW_DEFENSE_GUIDE.md)**
