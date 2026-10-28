import asyncio
import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.config import settings


async def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_rate_limit_has_no_user_agent_bypass():
    """A client claiming to be the load tester gets no exemption from the per-IP limit."""
    async with app.router.lifespan_context(app):
        async with await _client() as client:
            await client.post("/api/v1/admin/reset")
            headers = {"user-agent": "aiohttp/3.9 load_test"}
            responses = await asyncio.gather(*[
                client.post(
                    "/api/v1/checkout",
                    json={"user_id": f"rl_user_{i}", "product_id": settings.PRODUCT_ID, "quantity": 1},
                    headers=headers,
                )
                for i in range(settings.MAX_REQUESTS_PER_SEC_PER_IP * 2)
            ])
            codes = [r.status_code for r in responses]
            assert 429 in codes, "spoofed User-Agent must not bypass the rate limiter"
            await client.post("/api/v1/admin/reset")


@pytest.mark.asyncio
async def test_reset_requires_admin_token_outside_demo_mode(monkeypatch):
    async with app.router.lifespan_context(app):
        async with await _client() as client:
            monkeypatch.setattr(settings, "DEMO_MODE", False)
            monkeypatch.setattr(settings, "ADMIN_TOKEN", "s3cret")

            assert (await client.post("/api/v1/admin/reset")).status_code == 403
            bad = await client.post("/api/v1/admin/reset", headers={"X-Admin-Token": "nope"})
            assert bad.status_code == 403
            ok = await client.post("/api/v1/admin/reset", headers={"X-Admin-Token": "s3cret"})
            assert ok.status_code == 200


@pytest.mark.asyncio
async def test_demo_endpoint_is_404_when_demo_mode_off(monkeypatch):
    async with app.router.lifespan_context(app):
        async with await _client() as client:
            monkeypatch.setattr(settings, "DEMO_MODE", False)
            res = await client.post(
                "/api/v1/demo/checkout",
                json={"user_id": "x", "product_id": settings.PRODUCT_ID, "quantity": 1},
            )
            assert res.status_code == 404


@pytest.mark.asyncio
async def test_token_grant_and_enqueue_are_one_atomic_step():
    """The moment a 202 is returned, the ticket is QUEUED and the order is on the stream."""
    from app.redis_client import redis_manager
    async with app.router.lifespan_context(app):
        async with await _client() as client:
            await client.post("/api/v1/admin/reset")
            res = await client.post(
                "/api/v1/demo/checkout",
                json={"user_id": "atomic_user", "product_id": settings.PRODUCT_ID, "quantity": 1},
            )
            assert res.status_code == 202
            ticket_id = res.json()["ticket_id"]
            ticket = await redis_manager.get_ticket_status(ticket_id)
            assert ticket["status"] in ("QUEUED", "CONFIRMED")
            entries = await redis_manager.client.xrange(settings.STREAM_NAME)
            assert any(data.get("ticket_id") == ticket_id for _, data in entries)
            await client.post("/api/v1/admin/reset")


@pytest.mark.asyncio
async def test_unacked_message_from_crashed_consumer_is_reclaimed(monkeypatch):
    """A consumer that read an order and died before ACKing must not strand the order."""
    from sqlalchemy import select, func
    from app import worker as worker_module
    from app.database import init_db, AsyncSessionLocal
    from app.models import Order
    from app.redis_client import redis_manager

    await init_db()
    await redis_manager.connect()
    try:
        await redis_manager.client.delete(settings.STREAM_NAME)  # drop pending entries left by other tests
        await redis_manager._init_stream_group()
        await redis_manager.reset_flash_sale(settings.PRODUCT_ID, settings.INITIAL_STOCK)
        async with AsyncSessionLocal() as session:
            from sqlalchemy import delete, update
            from app.models import Product
            await session.execute(delete(Order))
            await session.execute(update(Product).values(available_stock=settings.INITIAL_STOCK))
            await session.commit()

        # Grant a token (stock reserved + order enqueued atomically), then have a consumer read
        # it and "crash" without acknowledging.
        ticket_id = "tok_crashtest01"
        assert (await redis_manager.reserve_stock_atomic(settings.PRODUCT_ID, "crash_user", ticket_id))[0] == 1
        got = await redis_manager.client.xreadgroup(
            settings.CONSUMER_GROUP, "crashed_consumer", {settings.STREAM_NAME: ">"}, count=10
        )
        assert got, "the dead consumer should have received the message"
        stuck = await redis_manager.client.xpending_range(
            settings.STREAM_NAME, settings.CONSUMER_GROUP, min="-", max="+", count=100, consumername="crashed_consumer"
        )
        assert len(stuck) == 1

        # A healthy worker reclaims it and persists the order.
        monkeypatch.setattr(worker_module, "RECLAIM_MIN_IDLE_MS", 0)
        await worker_module.worker.reclaim_stale()

        ticket = await redis_manager.get_ticket_status(ticket_id)
        assert ticket["status"] == "CONFIRMED"
        async with AsyncSessionLocal() as session:
            count = (await session.execute(select(func.count(Order.id)).where(Order.ticket_id == ticket_id))).scalar_one()
        assert count == 1
        stuck = await redis_manager.client.xpending_range(
            settings.STREAM_NAME, settings.CONSUMER_GROUP, min="-", max="+", count=100, consumername="crashed_consumer"
        )
        assert stuck == []
    finally:
        await redis_manager.disconnect()


@pytest.mark.asyncio
async def test_repeat_claim_returns_the_original_ticket():
    """A client that lost the first response can retry and resume tracking its own ticket."""
    async with app.router.lifespan_context(app):
        async with await _client() as client:
            await client.post("/api/v1/admin/reset")
            body = {"user_id": "retry_user", "product_id": settings.PRODUCT_ID, "quantity": 1}
            first = await client.post("/api/v1/demo/checkout", json=body)
            again = await client.post("/api/v1/demo/checkout", json=body)
            assert first.status_code == 202
            assert again.status_code == 409
            assert again.json()["ticket_id"] == first.json()["ticket_id"]
            assert again.json()["tracking_url"] == first.json()["tracking_url"]
            await client.post("/api/v1/admin/reset")


@pytest.mark.asyncio
async def test_sale_window_is_enforced_by_the_server_clock():
    async with app.router.lifespan_context(app):
        async with await _client() as client:
            await client.post("/api/v1/admin/reset", params={"starts_in": 30})
            product = (await client.get(f"/api/v1/products/{settings.PRODUCT_ID}")).json()
            assert product["is_live"] is False
            assert product["starts_at_ms"] - product["server_time_ms"] > 25_000

            body = {"user_id": "early_bird", "product_id": settings.PRODUCT_ID, "quantity": 1}
            early = await client.post("/api/v1/demo/checkout", json=body)
            assert early.status_code == 403
            assert early.json()["status"] == "NOT_LIVE"

            # Opening the sale (reset with no delay) lets the same user in, and stock was untouched
            await client.post("/api/v1/admin/reset")
            product = (await client.get(f"/api/v1/products/{settings.PRODUCT_ID}")).json()
            assert product["is_live"] is True and product["remaining_stock"] == settings.INITIAL_STOCK
            ok = await client.post("/api/v1/demo/checkout", json=body)
            assert ok.status_code == 202
            await client.post("/api/v1/admin/reset")
