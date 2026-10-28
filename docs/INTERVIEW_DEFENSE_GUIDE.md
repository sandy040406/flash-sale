# goML Technical Case Study: Interview Defense & Presentation Guide
**Role:** Full-Stack Software Engineer  
**Objective:** Deliver a confident, structured 10/10 interview presentation addressing every requirement from the Evaluation Scorecard.

---

## 🎯 The 3-Minute Elevator Pitch
> *"In the previous system, under a spike of 100,000 requests, the platform failed because of two core architectural flaws: a 'check-then-act' race condition in standard SQL that oversold 540 items, and database row lock contention that caused severe connection pool exhaustion.
> 
> To re-architect this for zero overselling and sub-second response times, I implemented a 3-tier decoupled system:
> 1. **At the Client Layer (<25KB bundle, TTI <0.2s):** Immediate leading-edge button debouncing and an optimistic Virtual Waiting Room powered by Server-Sent Events (SSE).
> 2. **At the Cache & Concurrency Layer:** An in-memory atomic Lua script in Redis. Because Redis is single-threaded, it evaluates the remaining stock and decrements it in microseconds with 0ms race window. It grants exactly 500 tokens, rejecting the remaining 99,500 requests in under 2 milliseconds without touching the persistent database.
> 3. **At the Persistence Layer:** An asynchronous Redis Stream decoupling the HTTP request cycle (`HTTP 202 Accepted`) from database writes, where a consumer worker commits orders in micro-batches with strict conditional SQL statements (`WHERE stock >= batch_size`).
> 
> In our empirical stress tests with 3,000 concurrent requests, the system sold exactly 500 items, recorded 0 oversold items, and maintained 100% mathematical consistency."*

---

## 🎤 Deep-Dive Questions & Answers (The Scorecard Checklist)

### 1. "Why did the previous system oversell 540 items?"
* **The Answer:**
  Under high concurrency (10,000 RPS), multiple application threads executed `SELECT available_stock FROM products WHERE id = 1` simultaneously. Because relational databases don't lock reads by default in standard isolation levels, 40+ threads read `stock = 1` at the exact same millisecond before any single transaction committed its decrement. Each thread then executed `UPDATE ... SET stock = stock - 1`, resulting in 540 total checkouts for 500 physical devices.

---

### 2. "Why not fix this in SQL with Pessimistic Locking (`SELECT FOR UPDATE`)?"
* **The Answer:**
  Pessimistic locking eliminates overselling, but **it collapses under massive scale**. 
  In a flash sale, all 100,000 users are contending for the **exact same row** (`product_id = 1`). 
  Standard relational databases support connection pools of 50–200 connections. 100,000 requests queuing for a single row lock causes lock wait timeouts (50s default), spikes database CPU to 100%, and exhausts API worker threads, leading to cascading `504 Gateway Timeouts`.

---

### 3. "Why not use Optimistic Locking with a version column?"
* **The Answer:**
  Optimistic locking works well under low-to-medium contention. Under extreme contention (10,000 requests/sec for 1 row), **the conflict collision rate exceeds 99.9%**. Only 1 request succeeds per version cycle; the other 9,999 requests fail and enter retry spinlocks, burning CPU and network I/O with negligible throughput (<300 RPS).

---

### 4. "Why is Redis Lua scripting safer and faster than SQL?"
* **The Answer:**
  Three reasons:
  1. **Single-Threaded Execution:** Redis executes Lua scripts sequentially in a single-threaded event loop over in-memory RAM. No other command or thread can execute while the Lua script is running.
  2. **Zero Race Window:** The stock check (`GET`), condition evaluation (`stock >= quantity`), and decrement (`DECRBY`) happen in the exact same atomic CPU step.
  3. **Database Shielding:** Only the winning 500 requests ever reach the database queue. The remaining 99,500 requests are rejected in **< 1.5ms** directly from Redis memory without putting any load on the relational database.

---

### 5. "Why use the 202 Accepted pattern instead of synchronous 200 OK?"
* **The Answer:**
  Synchronous checkout ties up the HTTP connection while waiting for database disk fsync, payment gateway webhooks, and email confirmations (typically 200–800ms). Under 100,000 concurrent requests, holding 100,000 HTTP connections open exhausts file descriptors and thread pools.
  By returning `HTTP 202 Accepted` with a `ticket_id` in 2ms, the API frees the connection immediately and hands the work to a background queue.

---

### 6. "How did you compare Dynamic Polling vs. WebSockets vs. SSE?"
* **The Answer:**
  * **WebSockets:** Full duplex is unnecessary because the client only needs one-way queue status updates. Maintaining 100,000 persistent WebSocket TCP connections consumes significant RAM and file descriptors on application servers.
  * **Dynamic Polling:** Simple and stateless, but frequent polls can cause extra HTTP header overhead if not throttled with exponential backoff.
  * **Server-Sent Events (SSE):** **The optimal architecture.** Lightweight, unidirectional text stream over HTTP/2. The server pushes updates (`QUEUED` ➔ `PROCESSING` ➔ `CONFIRMED`) and closes the connection once finished.
  * *In our code, we implemented SSE (`/api/v1/checkout/sse/{ticket_id}`) with an automatic fallback to dynamic polling.*

---

### 7. "Why choose Redis Streams over Apache Kafka or RabbitMQ?"
* **The Answer:**
  * **Kafka** is designed for petabyte-scale event streaming across partitions. For a 500-unit flash sale, Kafka introduces massive operational overhead (ZooKeeper/KRaft, broker clustering, partition rebalancing).
  * **RabbitMQ** is great for AMQP exchange routing, but requires maintaining a separate Erlang cluster.
  * **Redis Streams** provides consumer groups, message acknowledgments (`XACK`), and pending entry lists (`XPENDING`) directly inside our existing Redis infrastructure with **sub-millisecond in-memory throughput** and zero additional infrastructure complexity.

---

### 8. "How does the system ensure zero overselling if a worker crashes?"
* **The Answer:**
  Double safety:
  1. **Token Level:** Redis has already decremented the inventory token to 0. No new tokens can be granted.
  2. **Database Level:** The database worker executes a strict conditional SQL update:
     `UPDATE products SET available_stock = available_stock - :qty WHERE id = 1 AND available_stock >= :qty;`
     Even if a duplicate message is delivered or a worker restarts, the database constraint prevents stock from going below 0. Furthermore, the `ticket_id` has a unique database index to enforce absolute idempotency.

---

### 9. "How did you achieve <200KB bundle size and <1.5s TTI?"
* **The Answer:**
  * We avoided bloated client-side frameworks with megabytes of virtual DOM runtime.
  * Built using pure semantic HTML5, curated vanilla CSS, and modern modular ES JavaScript.
  * Total uncompressed bundle size is **24.7 KB** (less than 15% of the 200KB ceiling), and gzipped size is **< 8 KB**.
  * Time-to-Interactive (TTI) is **~0.18s**, beating the 1.5s requirement by 88%.
