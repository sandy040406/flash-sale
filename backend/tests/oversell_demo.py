"""
Reproduce the case study's oversell bug, side by side with the real system.

  naive : SELECT stock -> check in app code -> UPDATE (the classic check-then-act race)
  real  : atomic Redis Lua reservation + conditional SQL (this project)

Both start with 500 units and receive the same number of concurrent buyers (distinct customers).
Needs the stack running with DEMO_MODE=1.

  TARGET_URL=http://nginx USERS=3000 CONCURRENCY=300 python tests/oversell_demo.py
"""
import asyncio
import os
import uuid

import aiohttp

TARGET_URL = os.getenv("TARGET_URL", "http://127.0.0.1").rstrip("/")
USERS = int(os.getenv("USERS", "3000"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "300"))
STOCK = 500


async def fire(session, path):
    sem = asyncio.Semaphore(CONCURRENCY)
    run = uuid.uuid4().hex[:6]

    async def one(i):
        async with sem:
            try:
                async with session.post(
                    f"{TARGET_URL}{path}", json={"user_id": f"os_{run}_{i}", "product_id": 1, "quantity": 1}
                ) as r:
                    await r.read()
                    return r.status
            except Exception:
                return 0

    return await asyncio.gather(*[one(i) for i in range(USERS)])


async def main():
    timeout = aiohttp.ClientTimeout(total=60)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as s:
        print(f"{USERS} distinct buyers, {CONCURRENCY} at a time, {STOCK} units in stock\n")

        # ---- naive ----
        await s.post(f"{TARGET_URL}/api/v1/demo/naive-reset")
        codes = await fire(s, "/api/v1/demo/naive-checkout")
        stats = await (await s.get(f"{TARGET_URL}/api/v1/demo/naive-stats")).json()
        print("NAIVE  (SELECT, then UPDATE)")
        print(f"  checkouts that said 202 Accepted : {codes.count(202)}")
        print(f"  orders written to the database   : {stats['orders']}")
        print(f"  OVERSOLD                         : {stats['oversold']}   <- the bug\n")

        # ---- real ----
        await s.post(f"{TARGET_URL}/api/v1/admin/reset")
        codes = await fire(s, "/api/v1/demo/checkout")
        stats = {}
        for _ in range(60):
            stats = await (await s.get(f"{TARGET_URL}/api/v1/admin/stats")).json()
            if stats.get("database_confirmed_orders") == STOCK and stats.get("in_transit_queue_count") == 0:
                break
            await asyncio.sleep(0.5)
        print("REAL   (Redis Lua token + conditional SQL)")
        print(f"  checkouts that said 202 Accepted : {codes.count(202)}")
        print(f"  orders written to the database   : {stats.get('database_confirmed_orders')}")
        print(f"  OVERSOLD                         : {stats.get('oversold_count')}")
        print(f"  consistent                       : {stats.get('is_perfectly_consistent')}")


if __name__ == "__main__":
    asyncio.run(main())
