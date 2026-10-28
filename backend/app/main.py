import asyncio
import hmac
import time
import logging
import uuid
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header, Query, status, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import select, func, delete, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import init_db, get_db, AsyncSessionLocal
from app.models import Product, Order
from app.redis_client import redis_manager
from app.schemas import (
    CheckoutRequest,
    CheckoutResponse,
    TicketStatusResponse,
    ProductStatusResponse,
    SystemAuditStats
)
from app.worker import worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("flash_sale")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Initializing Database...")
    await init_db()
    
    logger.info("Connecting to Redis and caching Lua script...")
    await redis_manager.connect()
    
    # Pre-warm Redis stock if key doesn't exist
    current_redis_stock = await redis_manager.client.get(f"product:{settings.PRODUCT_ID}:stock")
    if current_redis_stock is None:
        await redis_manager.reset_flash_sale(settings.PRODUCT_ID, settings.INITIAL_STOCK)
    
    # Start background stream consumer
    worker_task = asyncio.create_task(worker.start()) if settings.RUN_WORKER else None
    logger.info("Application startup complete.")
    
    yield
    
    # Shutdown
    logger.info("Shutting down worker and Redis...")
    if worker_task:
        worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
    await redis_manager.disconnect()
    logger.info("Application shutdown complete.")

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="High-concurrency flash sale system preventing overselling using Redis Lua and Async Streams",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for frontend client
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from app.rate_limiter import RateLimitMiddleware
app.add_middleware(RateLimitMiddleware)

from pathlib import Path
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

frontend_dir = Path(__file__).parent.parent.parent / "frontend"
if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

@app.get("/")
async def serve_frontend():
    index_file = frontend_dir / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {
        "status": "online",
        "service": settings.PROJECT_NAME,
        "docs": "/docs"
    }

@app.get("/styles.css")
async def get_styles():
    return FileResponse(frontend_dir / "styles.css")

@app.get("/app.js")
async def get_js():
    return FileResponse(frontend_dir / "app.js")

@app.get("/assets/{filename}")
async def get_asset(filename: str):
    file_path = frontend_dir / "assets" / filename
    if file_path.exists():
        return FileResponse(file_path)
    raise HTTPException(status_code=404, detail="Asset not found")

@app.get("/health")
async def health_check():
    return {
        "status": "online",
        "service": settings.PROJECT_NAME,
        "docs": "/docs",
        "sale_product_id": settings.PRODUCT_ID
    }

_product_meta_cache: dict = {}
PRODUCT_META_TTL_SECONDS = 60

async def _get_product_meta(product_id: int):
    """Static product metadata (name/price/total) cached in-process so polling never touches the DB."""
    cached = _product_meta_cache.get(product_id)
    now = time.monotonic()
    if cached and now - cached[0] < PRODUCT_META_TTL_SECONDS:
        return cached[1]
    async with AsyncSessionLocal() as session:
        product = (await session.execute(select(Product).where(Product.id == product_id))).scalar_one_or_none()
    if not product:
        return None
    meta = {"id": product.id, "name": product.name, "price": product.price, "total_stock": product.total_stock}
    _product_meta_cache[product_id] = (now, meta)
    return meta

@app.get("/api/v1/products/{product_id}", response_model=ProductStatusResponse)
async def get_product_status(product_id: int):
    """Hot polling endpoint: stock and sale window come from Redis; metadata is cached in-process."""
    meta = await _get_product_meta(product_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Product not found")

    redis_stock = await redis_manager.get_remaining_stock(product_id)
    server_time_ms = await redis_manager.server_time_ms()
    starts_at_ms = await redis_manager.get_sale_start_ms()
    started = starts_at_ms is None or server_time_ms >= starts_at_ms

    return ProductStatusResponse(
        product_id=meta["id"],
        name=meta["name"],
        price=meta["price"],
        total_stock=meta["total_stock"],
        remaining_stock=redis_stock,
        is_sold_out=(redis_stock <= 0),
        is_live=settings.SALE_ACTIVE and started,
        starts_at_ms=starts_at_ms,
        server_time_ms=server_time_ms
    )

@app.post("/api/v1/checkout", response_model=CheckoutResponse)
async def checkout(payload: CheckoutRequest):
    return await _process_checkout(payload)

@app.post("/api/v1/demo/checkout", include_in_schema=False)
async def demo_checkout(payload: CheckoutRequest):
    """Same logic as /checkout but outside the per-IP limiters. 404 unless DEMO_MODE=1."""
    if not settings.DEMO_MODE:
        raise HTTPException(status_code=404, detail="Not found")
    return await _process_checkout(payload)

@app.get("/api/v1/demo/status", include_in_schema=False)
async def demo_status():
    return {"demo_mode": settings.DEMO_MODE}

async def _process_checkout(payload: CheckoutRequest):
    """
    High-Concurrency Flash Sale Ingestion Endpoint.
    
    1. Executes single-threaded atomic Lua script in Redis.
    2. Instantly rejects requests if the sale is not live (403), sold out (410) or a repeat (409,
       which returns the customer's existing ticket).
    3. If token granted, the same script has already queued the order on the Redis Stream;
       returns HTTP 202 Accepted.
    """
    if not settings.SALE_ACTIVE:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={
                "success": False,
                "status": "NOT_LIVE",
                "ticket_id": None,
                "message": "The sale is not live.",
                "tracking_url": None
            }
        )

    # 1. Execute Atomic Lua Reservation (reserves stock AND enqueues the order in one step)
    new_ticket_id = f"tok_{uuid.uuid4().hex[:12]}"
    token_status, ticket_id = await redis_manager.reserve_stock_atomic(
        product_id=payload.product_id,
        user_id=payload.user_id,
        ticket_id=new_ticket_id,
        quantity=payload.quantity
    )

    # Case N: Sale has not started (decided by the Redis server clock, inside the Lua script)
    if token_status == -2:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={
                "success": False,
                "status": "NOT_LIVE",
                "ticket_id": None,
                "message": "The sale has not started yet.",
                "tracking_url": None
            }
        )

    # Case A: Sold Out
    if token_status == 0:
        return JSONResponse(
            status_code=status.HTTP_410_GONE,
            content={
                "success": False,
                "status": "SOLD_OUT",
                "ticket_id": None,
                "message": "Flash sale sold out! All 500 items have been claimed.",
                "tracking_url": None
            }
        )

    # Case B: Same user again. Idempotent: return THEIR existing ticket, so a client that lost the
    # first response (timeout, dropped connection) can resume tracking instead of being stranded.
    if token_status == -1:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "success": False,
                "status": "ALREADY_ORDERED",
                "ticket_id": ticket_id,
                "message": "Limit 1 per customer. You already hold a unit; here is your ticket.",
                "tracking_url": f"/api/v1/checkout/status/{ticket_id}"
            }
        )

    # Case C: Token Granted (1). The order is already on the stream (queued inside the Lua script).
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "success": True,
            "status": "ACCEPTED",
            "ticket_id": ticket_id,
            "message": "Reservation token granted. Order placed in processing queue.",
            "tracking_url": f"/api/v1/checkout/status/{ticket_id}"
        }
    )

@app.get("/api/v1/checkout/status/{ticket_id}", response_model=TicketStatusResponse)
async def get_checkout_status(ticket_id: str):
    """Polling endpoint to track ticket fulfillment state."""
    ticket_data = await redis_manager.get_ticket_status(ticket_id)
    if not ticket_data:
        raise HTTPException(status_code=404, detail="Ticket not found or expired")

    order_id = int(ticket_data["order_id"]) if "order_id" in ticket_data else None
    
    return TicketStatusResponse(
        ticket_id=ticket_id,
        status=ticket_data.get("status", "QUEUED"),
        product_id=int(ticket_data.get("product_id", settings.PRODUCT_ID)),
        user_id=ticket_data.get("user_id", ""),
        message=ticket_data.get("message", "Processing..."),
        order_id=order_id
    )

from fastapi.responses import StreamingResponse
import json

@app.get("/api/v1/checkout/sse/{ticket_id}")
async def stream_checkout_status(ticket_id: str):
    """
    Server-Sent Events (SSE) Stream.
    Streams real-time ticket fulfillment status to the client without polling overhead.
    """
    async def event_generator():
        for _ in range(60): # 30s max connection lifetime
            ticket_data = await redis_manager.get_ticket_status(ticket_id)
            if not ticket_data:
                yield f"data: {json.dumps({'status': 'NOT_FOUND', 'message': 'Ticket not found or expired'})}\n\n"
                break

            current_status = ticket_data.get("status", "QUEUED")
            order_id = int(ticket_data["order_id"]) if "order_id" in ticket_data else None
            
            payload = {
                "ticket_id": ticket_id,
                "status": current_status,
                "message": ticket_data.get("message", "Processing..."),
                "order_id": order_id
            }
            yield f"data: {json.dumps(payload)}\n\n"

            if current_status in ["CONFIRMED", "FAILED"]:
                break

            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

def require_admin(x_admin_token: Optional[str] = Header(default=None)):
    """Destructive admin actions need the admin token, or demo mode (never use demo mode in production)."""
    if settings.DEMO_MODE:
        return
    if settings.ADMIN_TOKEN and x_admin_token and hmac.compare_digest(x_admin_token, settings.ADMIN_TOKEN):
        return
    raise HTTPException(status_code=403, detail="Admin token required")

@app.post("/api/v1/admin/reset", dependencies=[Depends(require_admin)])
async def reset_flash_sale(starts_in: int = Query(default=0, ge=0, le=86400, description="Seconds until the sale opens (0 = open now)")):
    """Resets inventory back to 500 items in both Redis and Database. starts_in schedules the opening."""
    # 1. Reset Database
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Delete existing orders
            await session.execute(delete(Order))
            # Reset stock
            await session.execute(
                update(Product)
                .where(Product.id == settings.PRODUCT_ID)
                .values(available_stock=settings.INITIAL_STOCK, total_stock=settings.INITIAL_STOCK)
            )

    # 2. Reset Redis Cache and Stream
    await redis_manager.reset_flash_sale(settings.PRODUCT_ID, settings.INITIAL_STOCK, starts_in_seconds=starts_in)
    _product_meta_cache.clear()

    return {
        "success": True,
        "message": f"Flash sale successfully reset to {settings.INITIAL_STOCK} stock."
                   + (f" Opens in {starts_in}s." if starts_in else "")
    }

# ---------------------------------------------------------------------------
# Demo only: the "before" picture. A deliberately naive SELECT-then-UPDATE checkout, kept on a
# separate product row, so the 540-oversell bug from the case study can be reproduced next to
# the real system. 404 unless DEMO_MODE=1.
# ---------------------------------------------------------------------------
NAIVE_PRODUCT_ID = 2

def _require_demo():
    if not settings.DEMO_MODE:
        raise HTTPException(status_code=404, detail="Not found")

@app.post("/api/v1/demo/naive-reset", include_in_schema=False)
async def naive_reset():
    _require_demo()
    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(delete(Order).where(Order.product_id == NAIVE_PRODUCT_ID))
            exists = (await session.execute(select(Product.id).where(Product.id == NAIVE_PRODUCT_ID))).first()
            if exists:
                await session.execute(update(Product).where(Product.id == NAIVE_PRODUCT_ID)
                                      .values(available_stock=settings.INITIAL_STOCK, total_stock=settings.INITIAL_STOCK))
            else:
                session.add(Product(id=NAIVE_PRODUCT_ID, name="Naive checkout (demo)", price=69900,
                                    total_stock=settings.INITIAL_STOCK, available_stock=settings.INITIAL_STOCK))
    return {"success": True}

@app.post("/api/v1/demo/naive-checkout", include_in_schema=False)
async def naive_checkout(payload: CheckoutRequest):
    """SELECT stock, check in app code, then UPDATE: the classic check-then-act race."""
    _require_demo()
    async with AsyncSessionLocal() as session:
        stock = (await session.execute(
            select(Product.available_stock).where(Product.id == NAIVE_PRODUCT_ID)
        )).scalar_one()
        if stock <= 0:
            return JSONResponse(status_code=410, content={"status": "SOLD_OUT"})
        await asyncio.sleep(0.005)  # stand-in for pricing/fraud/validation work between read and write
        # Relative decrement guarded only by the stale read above: no `AND stock > 0` in the UPDATE.
        await session.execute(
            update(Product).where(Product.id == NAIVE_PRODUCT_ID).values(available_stock=Product.available_stock - 1)
        )
        session.add(Order(ticket_id=f"naive_{uuid.uuid4().hex[:12]}", product_id=NAIVE_PRODUCT_ID,
                          user_id=payload.user_id, status="CONFIRMED"))
        await session.commit()
    return JSONResponse(status_code=202, content={"status": "ACCEPTED"})

@app.get("/api/v1/demo/naive-stats", include_in_schema=False)
async def naive_stats():
    _require_demo()
    async with AsyncSessionLocal() as session:
        orders = (await session.execute(
            select(func.count(Order.id)).where(Order.product_id == NAIVE_PRODUCT_ID)
        )).scalar_one()
    return {"orders": orders, "stock": settings.INITIAL_STOCK, "oversold": max(0, orders - settings.INITIAL_STOCK)}

@app.get("/api/v1/admin/stats", response_model=SystemAuditStats)
async def get_system_audit_stats(db: AsyncSession = Depends(get_db)):
    """
    Mathematical Consistency Verifier:
    Validates that:
    1. Total Confirmed Orders <= Initial Stock (500)
    2. Oversold Count == 0
    3. Redis Stock + Confirmed Orders == 500
    4. Database Stock + Confirmed Orders == 500
    """
    redis_stock = await redis_manager.get_remaining_stock(settings.PRODUCT_ID)
    
    # Database counts
    product_res = await db.execute(select(Product).where(Product.id == settings.PRODUCT_ID))
    product = product_res.scalar_one()
    
    orders_count_res = await db.execute(
        select(func.count(Order.id))
        .where(Order.product_id == settings.PRODUCT_ID, Order.status == "CONFIRMED")
    )
    confirmed_orders = orders_count_res.scalar_one()

    in_transit = max(0, (settings.INITIAL_STOCK - redis_stock) - confirmed_orders)
    queue_drain_complete = (in_transit == 0)
    total_accounted = product.available_stock + confirmed_orders
    oversold = max(0, confirmed_orders - settings.INITIAL_STOCK)
    
    # Perfectly consistent means all 500 items are accounted for and no pending messages in queue
    is_consistent = (
        oversold == 0 and 
        confirmed_orders <= settings.INITIAL_STOCK and
        total_accounted == settings.INITIAL_STOCK and
        queue_drain_complete
    )

    return SystemAuditStats(
        product_id=settings.PRODUCT_ID,
        initial_stock=settings.INITIAL_STOCK,
        redis_remaining_stock=redis_stock,
        database_remaining_stock=product.available_stock,
        database_confirmed_orders=confirmed_orders,
        total_items_accounted_for=total_accounted,
        in_transit_queue_count=in_transit,
        oversold_count=oversold,
        is_perfectly_consistent=is_consistent,
        queue_drain_complete=queue_drain_complete
    )
