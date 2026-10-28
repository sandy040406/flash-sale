import logging
import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.requests import Request

from app.config import settings
from app.redis_client import redis_manager

logger = logging.getLogger(__name__)


def client_ip(request: Request) -> str:
    """Real client IP. X-Real-IP (set by NGINX) is only honored when TRUST_PROXY_HEADERS is on."""
    if settings.TRUST_PROXY_HEADERS:
        forwarded = request.headers.get("x-real-ip")
        if forwarded:
            return forwarded
    return request.client.host if request.client else "unknown"


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Per-IP fixed-window rate limiter backed by Redis, so the limit holds across every replica
    (an in-memory counter would let a bot get N times the limit by spreading over N replicas).

    Defense in depth behind NGINX's limit_req. There is no User-Agent or header bypass.
    If Redis is unavailable the limiter fails open; checkout itself needs Redis anyway.
    """

    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith("/api/v1/checkout") and redis_manager.client is not None:
            key = f"rl:{client_ip(request)}:{int(time.time())}"
            try:
                pipe = redis_manager.client.pipeline(transaction=False)
                pipe.incr(key)
                pipe.expire(key, 2)
                count, _ = await pipe.execute()
            except Exception as e:
                logger.warning(f"Rate limiter unavailable, allowing request: {e}")
                count = 0

            if count > settings.MAX_REQUESTS_PER_SEC_PER_IP:
                return JSONResponse(
                    status_code=429,
                    content={
                        "success": False,
                        "status": "RATE_LIMITED",
                        "message": "Too many requests. Please wait a moment."
                    },
                    headers={"Retry-After": "1"}
                )

        return await call_next(request)
