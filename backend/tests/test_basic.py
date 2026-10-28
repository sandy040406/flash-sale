import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.config import settings

@pytest.mark.asyncio
async def test_full_sale_lifecycle():
    # Use FastAPI lifespan context so DB, Redis Lua, and worker are properly initialized
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Reset sale to clean state
            res = await client.post("/api/v1/admin/reset")
            assert res.status_code == 200
            assert res.json()["success"] is True

            # 2. Check initial product stock
            res = await client.get(f"/api/v1/products/{settings.PRODUCT_ID}")
            assert res.status_code == 200
            data = res.json()
            assert data["remaining_stock"] == 500
            assert data["is_sold_out"] is False

            # 3. Perform a checkout
            res = await client.post("/api/v1/checkout", json={
                "user_id": "user_test_101",
                "product_id": settings.PRODUCT_ID,
                "quantity": 1
            })
            assert res.status_code == 202
            body = res.json()
            assert body["status"] == "ACCEPTED"
            ticket_id = body["ticket_id"]
            assert ticket_id.startswith("tok_")

            # 4. Attempt duplicate checkout with same user_id (Must be rejected)
            dup_res = await client.post("/api/v1/checkout", json={
                "user_id": "user_test_101",
                "product_id": settings.PRODUCT_ID,
                "quantity": 1
            })
            assert dup_res.status_code == 409
            assert dup_res.json()["status"] == "ALREADY_ORDERED"

            # 5. Wait for worker to consume stream and persist to DB
            status_data = None
            for _ in range(30):
                status_res = await client.get(f"/api/v1/checkout/status/{ticket_id}")
                if status_res.status_code == 200:
                    status_data = status_res.json()
                    if status_data["status"] == "CONFIRMED":
                        break
                await asyncio.sleep(0.1)

            # 6. Check ticket status
            assert status_data is not None
            assert status_data["status"] == "CONFIRMED"
            assert status_data["order_id"] is not None

            # 7. Check audit stats (await worker DB write)
            stats = None
            for _ in range(30):
                stats_res = await client.get("/api/v1/admin/stats")
                assert stats_res.status_code == 200
                stats = stats_res.json()
                if stats["database_confirmed_orders"] == 1:
                    break
                await asyncio.sleep(0.1)

            assert stats is not None
            assert stats["database_confirmed_orders"] == 1
            assert stats["redis_remaining_stock"] == 499
            assert stats["database_remaining_stock"] == 499
            assert stats["oversold_count"] == 0
            assert stats["is_perfectly_consistent"] is True

            print("\n[PASSED] Full Flash Sale Lifecycle Test Verified Successfully!")
