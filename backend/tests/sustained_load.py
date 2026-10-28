"""
Sustained load generator that classifies every outcome, so "0% dropped" is a measured number.

Outcome classes
  served     202 accepted / 410 sold out / 409 repeat / 403 not live: the system answered correctly
  throttled  429: the per-IP limiter refused a (simulated) bad actor. Intentional, reported apart.
  dropped    connection error, timeout, 5xx, or any other status: the platform failed the request

Env
  TARGET_URL     base URL (default http://127.0.0.1)
  PATH_          request path (default /api/v1/demo/checkout; the demo path has no per-IP limit)
  TOTAL          stop after this many requests (0 = use DURATION)
  DURATION       seconds to run when TOTAL=0 (default 30)
  CONCURRENCY    in-flight requests (default 500)
  TIMEOUT        per-request timeout in seconds (default 10)
  START_AT       epoch seconds to wait for, so several generator containers start together

Like a browser, the client retries ONCE when a reused keep-alive connection was closed before any
response arrived (NGINX closes idle keep-alive connections on a graceful reload, which happens when
the autoscaler changes the replica set). That retry is safe because checkout is idempotent.
Those events are counted and printed as "recovered by retry", never hidden.
"""
import asyncio
import os
import statistics
import time
import uuid
from collections import Counter

import aiohttp

TARGET_URL = os.getenv("TARGET_URL", "http://127.0.0.1").rstrip("/")
PATH_ = os.getenv("PATH_", "/api/v1/demo/checkout")
TOTAL = int(os.getenv("TOTAL", "0"))
DURATION = float(os.getenv("DURATION", "30"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "500"))
TIMEOUT = float(os.getenv("TIMEOUT", "10"))
START_AT = float(os.getenv("START_AT", "0"))

SERVED = {202, 403, 409, 410}


async def main():
    run_id = uuid.uuid4().hex[:6]
    counter = {"sent": 0}
    codes = Counter()
    errors = Counter()
    recovered = {"n": 0}
    latencies = []
    deadline = time.perf_counter() + DURATION

    def more():
        if TOTAL:
            return counter["sent"] < TOTAL
        return time.perf_counter() < deadline

    conn = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=CONCURRENCY, ttl_dns_cache=30)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)

    async def worker(session):
        while more():
            counter["sent"] += 1
            n = counter["sent"]
            body = {"user_id": f"sl_{run_id}_{n}", "product_id": 1, "quantity": 1}
            t0 = time.perf_counter()
            for attempt in (1, 2):
                try:
                    async with session.post(TARGET_URL + PATH_, json=body) as resp:
                        await resp.read()
                        codes[resp.status] += 1
                    if attempt == 2:
                        recovered["n"] += 1
                    break
                except asyncio.TimeoutError:
                    errors["timeout"] += 1
                    break
                except (aiohttp.ServerDisconnectedError, aiohttp.ClientOSError) as e:
                    if attempt == 1:
                        continue  # closed reused connection: retry once, like a browser
                    errors[type(e).__name__] += 1
                except Exception as e:  # anything else is a real failure
                    errors[type(e).__name__] += 1
                    break
            latencies.append((time.perf_counter() - t0) * 1000)

    if START_AT:
        await asyncio.sleep(max(0.0, START_AT - time.time()))
        deadline = time.perf_counter() + DURATION
    started_epoch = time.time()
    start = time.perf_counter()
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        await asyncio.gather(*[worker(session) for _ in range(CONCURRENCY)])
    elapsed = time.perf_counter() - start
    finished_epoch = time.time()

    total = sum(codes.values()) + sum(errors.values())
    served = sum(c for k, c in codes.items() if k in SERVED)
    throttled = codes.get(429, 0)
    dropped = total - served - throttled
    latencies.sort()

    def pct(p):
        return latencies[min(len(latencies) - 1, int(len(latencies) * p))] if latencies else 0

    print("=" * 66)
    print(f"target {TARGET_URL}{PATH_}  concurrency {CONCURRENCY}  elapsed {elapsed:.1f}s")
    print(f"window {started_epoch:.3f} -> {finished_epoch:.3f}")
    print(f"requests {total}  throughput {total / elapsed:,.0f} req/s")
    print(f"latency  p50 {pct(0.5):.0f} ms   p90 {pct(0.9):.0f} ms   p99 {pct(0.99):.0f} ms   max {latencies[-1] if latencies else 0:.0f} ms")
    print(f"status codes: {dict(sorted(codes.items()))}")
    if errors:
        print(f"client errors: {dict(errors)}")
    if recovered["n"]:
        print(f"recovered by retry (closed keep-alive connection): {recovered['n']}")
    print(f"served    {served:>8}  ({100 * served / total:.3f}%)")
    print(f"throttled {throttled:>8}  ({100 * throttled / total:.3f}%)  <- per-IP limiter, intentional")
    print(f"DROPPED   {dropped:>8}  ({100 * dropped / total:.3f}%)")
    print("=" * 66)


if __name__ == "__main__":
    asyncio.run(main())
