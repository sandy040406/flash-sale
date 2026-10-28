import asyncio
import logging
import time
from typing import List, Tuple
from sqlalchemy import update, select
from sqlalchemy.exc import IntegrityError
from app.config import settings
from app.database import AsyncSessionLocal
from app.models import Product, Order
from app.redis_client import redis_manager

logger = logging.getLogger(__name__)

RECLAIM_INTERVAL_SECONDS = 5
RECLAIM_MIN_IDLE_MS = 15000  # a message unacked this long belongs to a crashed/failed consumer


class OrderProcessingWorker:
    def __init__(self):
        self.is_running = False
        self._last_reclaim = 0.0

    async def start(self):
        self.is_running = True
        logger.info(f"Order consumer worker started: {settings.CONSUMER_NAME}")
        
        while self.is_running:
            try:
                if time.monotonic() - self._last_reclaim >= RECLAIM_INTERVAL_SECONDS:
                    self._last_reclaim = time.monotonic()
                    await self.reclaim_stale()

                # Read stream messages in batches via Consumer Group
                entries = await redis_manager.client.xreadgroup(
                    groupname=settings.CONSUMER_GROUP,
                    consumername=settings.CONSUMER_NAME,
                    streams={settings.STREAM_NAME: ">"},
                    count=50,
                    block=500
                )

                if not entries:
                    await asyncio.sleep(0.01)
                    continue

                for stream_name, messages in entries:
                    if messages:
                        await self.process_batch(messages)

            except asyncio.CancelledError:
                logger.info("Worker received cancel signal.")
                break
            except Exception as e:
                if "UNBLOCKED" in str(e) or "NOGROUP" in str(e):
                    await asyncio.sleep(0.05)
                    await redis_manager._init_stream_group()
                    continue
                logger.error(f"Worker iteration error: {e}", exc_info=True)
                await asyncio.sleep(0.1)

    def stop(self):
        self.is_running = False

    async def reclaim_stale(self):
        """
        Take over messages that were delivered but never acknowledged (consumer crashed, or an
        unexpected error skipped the ACK). Reprocessing is safe: the order insert is idempotent
        on ticket_id, and a batch that already committed falls back to the idempotent path.
        """
        start_id = "0-0"
        while True:
            result = await redis_manager.client.xautoclaim(
                settings.STREAM_NAME,
                settings.CONSUMER_GROUP,
                settings.CONSUMER_NAME,
                min_idle_time=RECLAIM_MIN_IDLE_MS,
                start_id=start_id,
                count=50,
            )
            next_id, messages = result[0], result[1]
            messages = [(mid, data) for mid, data in messages if data]
            if messages:
                logger.warning(f"Reclaimed {len(messages)} stale stream messages for reprocessing.")
                await self.process_batch(messages)
            if next_id in ("0-0", b"0-0") or not next_id:
                break
            start_id = next_id

    async def process_batch(self, messages: List[Tuple[str, dict]]):
        """Processes a batch of order messages in a single ACID transaction for high throughput."""
        valid_items = []
        for msg_id, data in messages:
            ticket_id = data.get("ticket_id")
            product_id = int(data.get("product_id", settings.PRODUCT_ID))
            user_id = data.get("user_id")
            if ticket_id and user_id:
                valid_items.append((msg_id, ticket_id, product_id, user_id))
            else:
                await redis_manager.client.xack(settings.STREAM_NAME, settings.CONSUMER_GROUP, msg_id)

        if not valid_items:
            return

        batch_size = len(valid_items)
        msg_ids = [item[0] for item in valid_items]
        batch_persisted = False
        saved_orders = []

        async with AsyncSessionLocal() as session:
            try:
                async with session.begin():
                    # Strict conditional batch decrement
                    stmt = (
                        update(Product)
                        .where(Product.id == settings.PRODUCT_ID, Product.available_stock >= batch_size)
                        .values(available_stock=Product.available_stock - batch_size)
                    )
                    res = await session.execute(stmt)

                    if res.rowcount > 0:
                        # Full batch decrement succeeded! Insert orders
                        for _, ticket_id, product_id, user_id in valid_items:
                            order = Order(
                                ticket_id=ticket_id,
                                product_id=product_id,
                                user_id=user_id,
                                status="CONFIRMED"
                            )
                            session.add(order)
                        await session.flush()
                        
                        # Re-query newly created orders with IDs
                        orders_res = await session.execute(
                            select(Order).where(Order.ticket_id.in_([item[1] for item in valid_items]))
                        )
                        for ord_obj in orders_res.scalars().all():
                            saved_orders.append((ord_obj.ticket_id, str(ord_obj.id)))
                        batch_persisted = True

            except Exception as e:
                logger.warning(f"Batch optimization bypassed ({e}), falling back to granular execution.")

        # If DB commit succeeded, update Redis status and ACK
        if batch_persisted:
            for ticket_id, ord_id in saved_orders:
                await redis_manager.set_ticket_status(ticket_id, "CONFIRMED", {
                    "order_id": ord_id,
                    "message": "Order confirmed and inventory locked."
                })
            await redis_manager.client.xack(settings.STREAM_NAME, settings.CONSUMER_GROUP, *msg_ids)
            logger.info(f"Batch committed: {batch_size} orders persisted to database.")
            return

        # Granular fallback for edge conditions (e.g. final few items)
        for msg_id, ticket_id, product_id, user_id in valid_items:
            await self.process_order_message(msg_id, {
                "ticket_id": ticket_id, 
                "product_id": product_id, 
                "user_id": user_id
            })

    async def process_order_message(self, message_id: str, data: dict):
        ticket_id = data.get("ticket_id")
        product_id = int(data.get("product_id", settings.PRODUCT_ID))
        user_id = data.get("user_id")

        if not ticket_id or not user_id:
            await redis_manager.client.xack(settings.STREAM_NAME, settings.CONSUMER_GROUP, message_id)
            return

        async with AsyncSessionLocal() as session:
            try:
                async with session.begin():
                    # 1. Idempotency Check
                    existing_order = (await session.execute(
                        select(Order).where(Order.ticket_id == ticket_id)
                    )).scalar_one_or_none()

                    if existing_order:
                        await redis_manager.set_ticket_status(ticket_id, "CONFIRMED", {
                            "order_id": existing_order.id,
                            "message": "Order confirmed successfully."
                        })
                        await redis_manager.client.xack(settings.STREAM_NAME, settings.CONSUMER_GROUP, message_id)
                        return

                    # 2. Strict Conditional SQL Update: stock > 0
                    stmt = (
                        update(Product)
                        .where(Product.id == product_id, Product.available_stock > 0)
                        .values(available_stock=Product.available_stock - 1)
                    )
                    res = await session.execute(stmt)

                    if res.rowcount == 0:
                        await redis_manager.set_ticket_status(ticket_id, "FAILED", {
                            "message": "Sold out during database settlement."
                        })
                        await redis_manager.client.xack(settings.STREAM_NAME, settings.CONSUMER_GROUP, message_id)
                        return

                    # 3. Create Order
                    order = Order(
                        ticket_id=ticket_id,
                        product_id=product_id,
                        user_id=user_id,
                        status="CONFIRMED"
                    )
                    session.add(order)
                    await session.flush()
                    saved_order_id = str(order.id)

                # DB transaction committed! Now update Redis and ACK
                await redis_manager.set_ticket_status(ticket_id, "CONFIRMED", {
                    "order_id": saved_order_id,
                    "message": "Order confirmed and inventory locked."
                })
                await redis_manager.client.xack(settings.STREAM_NAME, settings.CONSUMER_GROUP, message_id)

            except IntegrityError:
                await redis_manager.set_ticket_status(ticket_id, "FAILED", {
                    "message": "Duplicate order detected."
                })
                await redis_manager.client.xack(settings.STREAM_NAME, settings.CONSUMER_GROUP, message_id)
            except Exception as e:
                logger.error(f"Failed to process message {message_id}: {e}", exc_info=True)

worker = OrderProcessingWorker()
