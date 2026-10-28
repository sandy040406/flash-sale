import json
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
import redis.asyncio as aioredis
from app.config import settings

logger = logging.getLogger(__name__)

class RedisManager:
    def __init__(self):
        self.client: Optional[aioredis.Redis] = None
        self.lua_sha: Optional[str] = None
        self._lua_script_content: Optional[str] = None

    async def connect(self):
        self.client = aioredis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True
        )
        
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

    def _users_key(self, product_id: int) -> str:
        return f"product:{product_id}:users"

    def _ticket_key(self, ticket_id: str) -> str:
        return f"ticket:{ticket_id}"

    async def reserve_stock_atomic(self, product_id: int, user_id: str, quantity: int = 1) -> int:
        """
        Executes atomic Lua script.
        Returns:
          1  -> Success (Stock decremented)
          0  -> Sold out
         -1  -> Already purchased by this user
        """
        stock_key = self._stock_key(product_id)
        users_key = self._users_key(product_id)
        
        try:
            result = await self.client.evalsha(
                self.lua_sha,
                2,
                stock_key,
                users_key,
                user_id,
                quantity
            )
            return int(result)
        except aioredis.NoScriptError:
            # Re-cache if script was flushed
            self.lua_sha = await self.client.script_load(self._lua_script_content)
            result = await self.client.evalsha(
                self.lua_sha,
                2,
                stock_key,
                users_key,
                user_id,
                quantity
            )
            return int(result)

    async def publish_checkout_event(self, ticket_id: str, product_id: int, user_id: str) -> str:
        """Publishes reserved checkout event to Redis Stream for decoupled background persistence."""
        event_data = {
            "ticket_id": ticket_id,
            "product_id": str(product_id),
            "user_id": str(user_id)
        }
        msg_id = await self.client.xadd(settings.STREAM_NAME, event_data)
        
        # Save initial ticket status
        await self.set_ticket_status(ticket_id, "QUEUED", {
            "product_id": str(product_id),
            "user_id": str(user_id),
            "message": "Token acquired. Order processing in queue."
        })
        return msg_id

    async def set_ticket_status(self, ticket_id: str, status: str, extra: Optional[Dict[str, Any]] = None):
        key = self._ticket_key(ticket_id)
        mapping = {"status": status}
        if extra:
            for k, v in extra.items():
                mapping[k] = str(v)
        await self.client.hset(key, mapping=mapping)
        # Expire ticket status key after 2 hours to prevent memory leak
        await self.client.expire(key, 7200)

    async def get_ticket_status(self, ticket_id: str) -> Dict[str, str]:
        key = self._ticket_key(ticket_id)
        return await self.client.hgetall(key)

    async def get_remaining_stock(self, product_id: int) -> int:
        stock = await self.client.get(self._stock_key(product_id))
        return int(stock) if stock is not None else 0

    async def reset_flash_sale(self, product_id: int, initial_stock: int = 500):
        """Pre-warms the cache for the flash sale."""
        stock_key = self._stock_key(product_id)
        users_key = self._users_key(product_id)
        await self.client.set(stock_key, initial_stock)
        await self.client.delete(users_key)
        # Trim stream cleanly without deleting the key so active consumer workers don't get UNBLOCKED errors
        try:
            await self.client.xtrim(settings.STREAM_NAME, maxlen=0)
            await self.client.xgroup_setid(settings.STREAM_NAME, settings.CONSUMER_GROUP, id="0")
        except Exception:
            await self._init_stream_group()
        logger.info(f"Flash sale reset: Product {product_id} initialized with {initial_stock} stock.")

redis_manager = RedisManager()
