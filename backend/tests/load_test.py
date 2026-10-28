import asyncio
import os
import time
import uuid
import aiohttp
import statistics
from typing import List, Dict, Any

# LOAD_TEST_URLS="http://backend_1:8000,http://backend_2:8000,..." spreads requests round-robin
# across replicas; stats/reset always go to the first one (shared DB + Redis).
TARGETS = os.getenv("LOAD_TEST_URLS", "http://127.0.0.1:8000").split(",")
BASE_URL = TARGETS[0]
CHECKOUT_ENDPOINT = f"{BASE_URL}/api/v1/checkout"
STATS_ENDPOINT = f"{BASE_URL}/api/v1/admin/stats"
RESET_ENDPOINT = f"{BASE_URL}/api/v1/admin/reset"

async def send_checkout_request(
    session: aiohttp.ClientSession, 
    user_id: str, 
    product_id: int = 1,
    base_url: str = BASE_URL
) -> Dict[str, Any]:
    start_time = time.perf_counter()
    payload = {
        "user_id": user_id,
        "product_id": product_id,
        "quantity": 1
    }
    try:
        async with session.post(f"{base_url}/api/v1/checkout", json=payload) as resp:
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            data = await resp.json()
            return {
                "status_code": resp.status,
                "latency_ms": latency_ms,
                "ticket_id": data.get("ticket_id"),
                "status": data.get("status")
            }
    except Exception as e:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return {
            "status_code": 0,
            "latency_ms": latency_ms,
            "error": str(e)
        }

async def run_flash_sale_benchmark(
    total_requests: int = 5000, 
    concurrency_limit: int = 200
):
    print("=" * 70)
    print(f"🔥 STARTING HIGH-CONCURRENCY FLASH SALE STRESS TEST")
    print(f"Target Total Requests: {total_requests}")
    print(f"Max Concurrent Connections: {concurrency_limit}")
    print("=" * 70)

    # 1. Reset flash sale state
    async with aiohttp.ClientSession() as session:
        async with session.post(RESET_ENDPOINT) as resp:
            reset_data = await resp.json()
            print(f"✓ Initialized clean state: {reset_data.get('message')}")

    # 2. Generate simulated users
    user_ids = [f"usr_{uuid.uuid4().hex[:8]}_{i}" for i in range(total_requests)]
    
    conn = aiohttp.TCPConnector(limit=concurrency_limit, limit_per_host=concurrency_limit)
    timeout = aiohttp.ClientTimeout(total=30)
    
    start_benchmark = time.perf_counter()
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        tasks = [
            send_checkout_request(session, uid, base_url=TARGETS[i % len(TARGETS)])
            for i, uid in enumerate(user_ids)
        ]
        results: List[Dict[str, Any]] = await asyncio.gather(*tasks)

    total_time = time.perf_counter() - start_benchmark
    rps = total_requests / total_time

    # 3. Analyze status codes and latencies
    status_counts = {}
    latencies = []
    accepted_tickets = []
    
    for r in results:
        code = r["status_code"]
        status_counts[code] = status_counts.get(code, 0) + 1
        latencies.append(r["latency_ms"])
        if code == 202:
            accepted_tickets.append(r.get("ticket_id"))

    latencies.sort()
    p50 = statistics.median(latencies)
    p90 = latencies[int(len(latencies) * 0.90)]
    p99 = latencies[int(len(latencies) * 0.99)]
    avg_latency = statistics.mean(latencies)

    print("\n" + "=" * 70)
    print("📊 BENCHMARK INGESTION RESULTS")
    print("=" * 70)
    print(f"Total Requests Completed: {len(results)}")
    print(f"Total Time Taken:        {total_time:.2f} seconds")
    print(f"Throughput (RPS):        {rps:.2f} requests/sec")
    print(f"Average Latency:         {avg_latency:.2f} ms")
    print(f"P50 Latency:             {p50:.2f} ms")
    print(f"P90 Latency:             {p90:.2f} ms")
    print(f"P99 Latency:             {p99:.2f} ms")
    print("\nHTTP Status Breakdown:")
    for code, count in sorted(status_counts.items()):
        status_label = {
            202: "ACCEPTED (Token Granted)",
            410: "GONE (Sold Out Handled Cleanly)",
            409: "CONFLICT (Duplicate Prevented)",
            0: "FAILED / CONNECTION ERROR"
        }.get(code, "OTHER")
        print(f"  HTTP {code} [{status_label}]: {count}")

    # 4. Wait for background queue worker to persist all accepted tickets to database
    print("\n⏳ Awaiting background stream worker queue drain and DB persistence...")
    audit = {}
    for _ in range(50):
        async with aiohttp.ClientSession() as session:
            async with session.get(STATS_ENDPOINT) as resp:
                audit = await resp.json()
                if audit.get("database_confirmed_orders", 0) == 500:
                    break
        await asyncio.sleep(0.2)

    print("\n" + "=" * 70)
    print("🛡️ MATHEMATICAL DATA CONSISTENCY & ZERO-OVERSELLING AUDIT")
    print("=" * 70)
    print(f"Initial Inventory:             {audit.get('initial_stock')}")
    print(f"Redis Remaining Stock:         {audit.get('redis_remaining_stock')}")
    print(f"Database Remaining Stock:      {audit.get('database_remaining_stock')}")
    print(f"Confirmed Database Orders:     {audit.get('database_confirmed_orders')}")
    print(f"Total Items Accounted For:     {audit.get('total_items_accounted_for')} / {audit.get('initial_stock')}")
    print(f"Oversold Count:                {audit.get('oversold_count')}")
    print(f"Perfect Consistency Verified:  {'✅ YES (PASSED)' if audit.get('is_perfectly_consistent') else '❌ NO (FAILED)'}")
    print("=" * 70)

    # Strict Assertions
    assert audit.get("oversold_count") == 0, f"OVERSELLING DETECTED! Oversold: {audit.get('oversold_count')}"
    assert audit.get("database_confirmed_orders") == 500, f"Expected 500 confirmed orders, got {audit.get('database_confirmed_orders')}"
    assert audit.get("is_perfectly_consistent") is True, "Audit consistency check failed!"
    print("\n🎯 GOML CRITICAL CRITERIA VALIDATED: ZERO OVERSELLING UNDER CONCURRENT SPIKE!\n")

if __name__ == "__main__":
    asyncio.run(run_flash_sale_benchmark(total_requests=3000, concurrency_limit=150))
