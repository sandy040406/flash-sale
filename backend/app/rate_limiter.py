import logging
import time

from starlette.datastructures import Headers
from starlette.responses import JSONResponse

from app.config import settings
from app.redis_client import redis_manager

logger = logging.getLogger(__name__)


def client_ip(scope) -> str:
    """Real client IP. X-Real-IP (set by NGINX) is only honored when TRUST_PROXY_HEADERS is on."""
    if settings.TRUST_PROXY_HEADERS:
        forwarded = Headers(scope=scope).get("x-real-ip")
        if forwarded:
            return forwarded
    client = scope.get("client")
    return client[0] if client else "unknown"


class RateLimitMiddleware:
    """
    Per-IP fixed-window rate limiter backed by Redis, so the limit holds across every replica
    (an in-memory counter would let a bot get N times the limit by spreading over N replicas).

    Defense in depth behind NGINX's limit_req. There is no User-Agent or header bypass.
    If Redis is unavailable the limiter fails open; checkout itself needs Redis anyway.

    A plain ASGI middleware (not BaseHTTPMiddleware): it adds no per-request task or stream wrapping
    to the hot path, which matters because the API tier is CPU-bound.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if (
            scope["type"] == "http"
            and scope["path"].startswith("/api/v1/checkout")
            and redis_manager.client is not None
        ):
            key = f"rl:{client_ip(scope)}:{int(time.time())}"
            try:
                pipe = redis_manager.client.pipeline(transaction=False)
                pipe.incr(key)
                pipe.expire(key, 2)
                count, _ = await pipe.execute()
            except Exception as e:
                logger.warning(f"Rate limiter unavailable, allowing request: {e}")
                count = 0

            if count > settings.MAX_REQUESTS_PER_SEC_PER_IP:
                response = JSONResponse(
                    status_code=429,
                    content={
                        "success": False,
                        "status": "RATE_LIMITED",
                        "message": "Too many requests. Please wait a moment."
                    },
                    headers={"Retry-After": "1"}
                )
                await response(scope, receive, send)
                return

        await self.app(scope, receive, send)
