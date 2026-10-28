# High-Concurrency Flash Sale Architecture (goML Technical Case Study)

> **Zero overselling by construction: stock is reserved atomically in Redis and re-checked by the database.**  
> *Role Evaluation: Full-Stack Software Engineer*  

![License](https://img.shields.io/badge/License-MIT-blue.svg)
![Python](https://img.shields.io/badge/Python-3.12-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-green.svg)
![Redis](https://img.shields.io/badge/Redis-Streams%20%2B%20Lua-red.svg)
![Streaming](https://img.shields.io/badge/Real--Time-SSE%20Streaming-purple.svg)
![First visit](https://img.shields.io/badge/First%20visit-~116KB-brightgreen.svg)
![Status](https://img.shields.io/badge/Audit-100%25%20Consistent-success.svg)

---

## 🚀 Project Overview & Architecture

The project is structured according to the **goML Candidate Evaluation Framework** and **Model Reference Solution (Phases A, B, and C)**:

```
flash-sale-system/
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI app, SSE streaming, static mounting, CORS
│   │   ├── config.py            # Settings & stream configurations
│   │   ├── database.py          # SQLAlchemy async engine (Postgres in compose, SQLite locally)
│   │   ├── models.py            # Product & Order models with strict unique indexes
│   │   ├── redis_client.py      # Redis async client & Lua script loader
│   │   ├── rate_limiter.py      # Sliding window token-bucket rate limiter middleware
│   │   ├── schemas.py           # Pydantic request/response schemas
│   │   ├── worker.py            # Redis Streams batch persistence consumer
│   │   └── lua_scripts/
│   │       └── reserve_stock.lua # Atomic reserve + enqueue (stock, user set, stream, ticket in one script)
│   ├── tests/
│   │   ├── test_basic.py        # Lifecycle integration tests
│   │   └── load_test.py         # 3,000+ concurrent requests stress test
│   ├── requirements.txt
│   └── Dockerfile               # Production container image
├── frontend/                    # ~116KB first visit (40KB code, 11KB gzipped)
│   ├── index.html               # Semantic HTML5 with preloading & virtual waiting room UI
│   ├── styles.css               # Luxury dark glassmorphism styling
│   ├── app.js                   # Client debouncing, SSE streaming & live audit lab
│   └── assets/
│       ├── goml-logo.webp       # goML logo
│       ├── phone.webp / phone.jpg
│       └── fonts/               # Self-hosted subset woff2 (Bricolage Grotesque, Instrument Sans, IBM Plex Mono)
├── nginx/
│   └── nginx.conf               # Edge reverse proxy, least-conn load balancing & rate limiting
├── docker-compose.yml           # Multi-replica orchestration (NGINX + 3 Backends + Redis)
└── docs/
    ├── ARCHITECTURE_SYSTEM_DESIGN.md # Complete technical paper covering all rubrics
    └── INTERVIEW_DEFENSE_GUIDE.md    # Point-by-point interview Q&A defense script
```

---

## ⚡ Quick Start (Local Run)

### 1. Launch Backend Server
```bash
# Run FastAPI server (serves both API & Frontend UI)
cd backend
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 2. View the UI in Browser
Open **`http://localhost:8000/`** to interact with the Flash Sale interface, test the debounced button, and use the **Audit & Architecture Lab**.

### 3. Run Concurrency Load Test
```bash
python tests/load_test.py
```

---

## 🐳 Docker Production Multi-Replica Deployment

To launch the full production topology (NGINX Load Balancer + 3 Stateless FastAPI Replicas + Redis + shared Postgres):
```bash
docker compose up --build
```
Access the application through the NGINX edge gateway at **`http://localhost/`**.

| Setting (env) | Default | Purpose |
|---|---|---|
| `DEMO_MODE` | `0` | Enables the unlimited demo checkout and tokenless `/admin/reset`. Never use in production. |
| `ADMIN_TOKEN` | empty | Sent as `X-Admin-Token` to allow `/api/v1/admin/reset` when demo mode is off. |
| `TRUST_PROXY_HEADERS` | `1` in compose | Use NGINX's `X-Real-IP` as the client IP for rate limiting. |

### Demo mode (500 users from one browser)
The real `/api/v1/checkout` is rate-limited per IP (bot protection), so one browser cannot
place 500 orders through it. For live demos, start the stack with demo mode on:
```bash
DEMO_MODE=1 docker compose up -d          # PowerShell: $env:DEMO_MODE=1; docker compose up -d
```
The "Trigger Concurrent Spike" button then uses `/api/v1/demo/checkout` (404 unless `DEMO_MODE=1`)
and reports accepted / sold-out / rate-limited counts. The real endpoint stays rate-limited.
**Never enable demo mode in production.** Restart without the variable to turn it off.

---

## 🛡️ Measured Result (3,000 requests across 3 replicas)
```
======================================================================
🛡️ MATHEMATICAL DATA CONSISTENCY & ZERO-OVERSELLING AUDIT
======================================================================
Initial Inventory:             500
Redis Remaining Stock:         0
Database Remaining Stock:      0
Confirmed Database Orders:     500
Total Items Accounted For:     500 / 500
Oversold Count:                0
Perfect Consistency Verified:  ✅ YES (PASSED)
======================================================================
```

---

Measured on a laptop with the load generator in Docker: about 1,900 requests/sec and p99 of about 1.4 s for 3,000 requests at 150 concurrency
(`LOAD_TEST_URLS=http://backend_1:8000,... python tests/load_test.py`, server started with `DEMO_MODE=1`).
This is **not** a 100,000-request test; it shows correctness (500 sold, 0 oversold) under contention, not production capacity.

### Known limitations
- NGINX and Redis are single instances (single points of failure). Production would use an ALB pair and Redis with replicas/Sentinel.
- Each SSE connection polls Redis every 0.5 s. At very large scale this should become Redis pub/sub.
- The countdown timer on the page is cosmetic; the server only enforces `SALE_ACTIVE`.
- `user_id` is client-supplied, so one-per-customer relies on authentication in a real deployment; per-IP rate limits are the only abuse control here.

---

## 📚 Key Technical Documentation

* 📖 **[System Design & Technical Architecture](docs/ARCHITECTURE_SYSTEM_DESIGN.md):** Deep comparison of Pessimistic vs. Optimistic vs. Redis Lua locking, Kafka vs. RabbitMQ vs. Redis Streams, and Dynamic Polling vs. WebSockets vs. SSE.
* 🎙️ **[Interview Defense & Presentation Guide](docs/INTERVIEW_DEFENSE_GUIDE.md):** The 3-minute elevator pitch and answers to every question the goML interview panel can ask.
