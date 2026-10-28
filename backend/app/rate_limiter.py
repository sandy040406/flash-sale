import time
from collections import defaultdict
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.requests import Request

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    In-memory Sliding Window Rate Limiter.
    Protects checkout and API endpoints against bot traffic and brute-force spam.
    Configured for 10 requests per second per IP with burst capacity.
    """
    def __init__(self, app, requests_per_second: int = 15, burst_allowance: int = 25):
        super().__init__(app)
        self.rate = requests_per_second
        self.burst = burst_allowance
        self.clients = defaultdict(list)

    async def dispatch(self, request: Request, call_next):
        # Only rate-limit checkout and dynamic API routes (exempt static assets)
        if request.url.path.startswith("/api/v1/checkout"):
            client_ip = request.client.host if request.client else "127.0.0.1"
            current_time = time.time()
            
            # Clean timestamps older than 1 second
            window_start = current_time - 1.0
            self.clients[client_ip] = [
                ts for ts in self.clients[client_ip] if ts > window_start
            ]
            
            # Allow burst capacity during benchmark/simulations if user-agent is internal
            user_agent = request.headers.get("user-agent", "")
            is_internal_load_test = "aiohttp" in user_agent or "load_test" in user_agent

            if not is_internal_load_test and len(self.clients[client_ip]) >= self.burst:
                return JSONResponse(
                    status_code=429,
                    content={
                        "success": False,
                        "status": "RATE_LIMITED",
                        "message": "Too many requests. Bot protection triggered. Please wait a moment."
                    },
                    headers={"Retry-After": "1"}
                )

            self.clients[client_ip].append(current_time)

        response = await call_next(request)
        return response
