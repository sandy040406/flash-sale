# High-Concurrency Flash Sale Architecture (goML Technical Case Study)

> **Zero overselling by construction: stock is reserved atomically in Redis and re-checked by the database.**  
> *Role Evaluation: Full-Stack Software Engineer*  

![License](https://img.shields.io/badge/License-MIT-blue.svg)
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
├── scripts/inline_css.py        # Inlines minified styles.css into index.html
├── docker-compose.yml           # NGINX + autoscaled API + workers + autoscaler + Redis + Postgres
└── docs/                        # ARCHITECTURE_SYSTEM_DESIGN.md, INTERVIEW_DEFENSE_GUIDE.md
```

---

## ⚡ Quick Start

### Full topology (recommended)
```bash
DEMO_MODE=1 docker compose up -d --build     # PowerShell: $env:DEMO_MODE=1; docker compose up -d --build
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

### Throughput honesty
About 2,100 to 2,400 req/s through NGINX on this laptop. 100,000 requests in 10 seconds needs about 10,000 req/s, roughly 4 to 5 times more than was measured. The API tier is stateless and scales horizontally, but that target was **not** demonstrated here.

---

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

---

## 📚 Key Technical Documentation

* 📖 **[System Design & Technical Architecture](docs/ARCHITECTURE_SYSTEM_DESIGN.md)**
* 🎙️ **[Interview Defense & Presentation Guide](docs/INTERVIEW_DEFENSE_GUIDE.md)**
