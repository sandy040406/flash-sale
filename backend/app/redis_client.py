import json
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
import redis.asyncio as aioredis
from app.config import settings

logger = logging.getLogger(__name__)

TICKET_TTL_SECONDS = 7200  # expire ticket status keys after 2 hours

class RedisManager:
    def __init__(self):
        self.client: Optional[aioredis.Redis] = None
        self.lua_sha: Optional[str] = None
        self._lua_script_content: Optional[str] = None

    async def connect(self):
        # A blocking pool makes a request WAIT (briefly) for a free connection instead of failing with
        # "Too many connections" when a burst exceeds the pool: waiting is a slower answer, an error is a drop.
        pool = aioredis.BlockingConnectionPool(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
            timeout=settings.REDIS_POOL_TIMEOUT_SECONDS,
        )
        self.client = aioredis.Redis(connection_pool=pool)
        
        # Load Lua script into Redis script cache
        lua_path = Path(__file__).parent / "lua_scripts" / "reserve_stock.lua"
        with open(lua_path, "r", encoding="utf-8") as f:
            self._lua_script_content = f.read()

        try:
            self.lua_sha = await self.client.script_load(self._lua_script_content)
            logger.info(f"Loaded atomic reservation Lua script with SHA: {self.lua_sha}")
        except Exception as e:
            logger.error(f"Failed to load Lua script: {e}")
            raise e

        # Ensure Redis Stream and Consumer Group exist
        await self._init_stream_group()

    async def disconnect(self):
        if self.client:
            await self.client.aclose()

    async def _init_stream_group(self):
        try:
            # Create consumer group if it doesn't already exist
            await self.client.xgroup_create(
                name=settings.STREAM_NAME,
                groupname=settings.CONSUMER_GROUP,
                id="0",
                mkstream=True
            )
            logger.info(f"Created consumer group: {settings.CONSUMER_GROUP} on {settings.STREAM_NAME}")
        except aioredis.ResponseError as e:
            if "BUSYGROUP Consumer Group name already exists" in str(e):
                logger.info(f"Consumer group {settings.CONSUMER_GROUP} already exists.")
            else:
                logger.warning(f"Consumer group init note: {e}")

    def _stock_key(self, product_id: int) -> str:
        return f"product:{product_id}:stock"

    def _claims_key(self, product_id: int) -> str:
        return f"product:{product_id}:claims"

    @staticmethod
    def _window_key() -> str:
        return "sale:window"

    def _ticket_key(self, ticket_id: str) -> str:
        return f"ticket:{ticket_id}"

    async def reserve_stock_atomic(self, product_id: int, user_id: str, ticket_id: str, quantity: int = 1) -> Tuple[int, str]:
        """
        Executes the atomic Lua script: reserve stock AND enqueue the order in one step.
        Returns (status, ticket_id):
          ( 1, new_ticket)       -> Success (stock decremented, order on the stream, ticket QUEUED)
          ( 0, "")               -> Sold out
          (-1, existing_ticket)  -> This user already claimed (idempotent retry returns their ticket)
          (-2, "")               -> Sale not started yet
        """
        keys_and_args = (
            5,
            self._stock_key(product_id),
            self._claims_key(product_id),
            settings.STREAM_NAME,
            self._ticket_key(ticket_id),
            self._window_key(),
            user_id,
            quantity,
            ticket_id,
            product_id,
            TICKET_TTL_SECONDS,
        )
        try:
            result = await self.client.evalsha(self.lua_sha, *keys_and_args)
        except aioredis.NoScriptError:
            # Re-cache if script was flushed
            self.lua_sha = await self.client.script_load(self._lua_script_content)
            result = await self.client.evalsha(self.lua_sha, *keys_and_args)
        return int(result[0]), str(result[1])

    async def server_time_ms(self) -> int:
        seconds, micros = await self.client.time()
        return int(seconds) * 1000 + int(micros) // 1000

    async def get_sale_start_ms(self) -> Optional[int]:
        start = await self.client.hget(self._window_key(), "start")
        return int(start) if start is not None else None

    async def set_ticket_status(self, ticket_id: str, status: str, extra: Optional[Dict[str, Any]] = None):
        key = self._ticket_key(ticket_id)
        mapping = {"status": status}
        if extra:
            for k, v in extra.items():
                mapping[k] = str(v)
        await self.client.hset(key, mapping=mapping)
        await self.client.expire(key, TICKET_TTL_SECONDS)

    async def get_ticket_status(self, ticket_id: str) -> Dict[str, str]:
        key = self._ticket_key(ticket_id)
        return await self.client.hgetall(key)

    async def get_remaining_stock(self, product_id: int) -> int:
        stock = await self.client.get(self._stock_key(product_id))
        return int(stock) if stock is not None else 0

    async def reset_flash_sale(self, product_id: int, initial_stock: int = 500, starts_in_seconds: int = 0):
        """Pre-warms the cache for the flash sale. starts_in_seconds > 0 schedules a future start."""
        stock_key = self._stock_key(product_id)
        claims_key = self._claims_key(product_id)
        await self.client.set(stock_key, initial_stock)
        await self.client.delete(claims_key)
        if starts_in_seconds > 0:
            start_ms = await self.server_time_ms() + starts_in_seconds * 1000
            await self.client.hset(self._window_key(), "start", start_ms)
        else:
            await self.client.delete(self._window_key())
        # Trim stream cleanly without deleting the key so active consumer workers don't get UNBLOCKED errors
        try:
            await self.client.xtrim(settings.STREAM_NAME, maxlen=0)
            await self.client.xgroup_setid(settings.STREAM_NAME, settings.CONSUMER_GROUP, id="0")
        except Exception:
            await self._init_stream_group()
        logger.info(f"Flash sale reset: Product {product_id} initialized with {initial_stock} stock.")

redis_manager = RedisManager()
