import os
import socket
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "Flash Sale High-Concurrency Engine"
    REDIS_HOST: str = os.getenv("REDIS_HOST", "172.23.39.197")
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_MAX_CONNECTIONS: int = 256  # per process; requests queue for a free connection beyond this
    REDIS_POOL_TIMEOUT_SECONDS: float = 10
    
    # Redis Stream & Consumer Configuration
    STREAM_NAME: str = "flash_sale_stream"
    CONSUMER_GROUP: str = "order_processing_group"
    # Unique per container (hostname = container id) so scaled replicas never share a consumer name
    CONSUMER_NAME: str = f"worker_{socket.gethostname()}"
    
    # Database
    DATABASE_URL: str = "sqlite+aiosqlite:///./flash_sale.db"
    
    # Flash Sale Parameters
    PRODUCT_ID: int = 1
    INITIAL_STOCK: int = 500
    SALE_ACTIVE: bool = True

    # Run the stream consumer inside the API process (convenient locally and in tests).
    # In docker-compose the API sets this to false and a dedicated `worker` service consumes.
    RUN_WORKER: bool = True

    # Demo-only: enables /api/v1/demo/checkout (no per-IP limit) so one browser can
    # simulate 500 distinct users. Off by default; never enable in production.
    DEMO_MODE: bool = False
    
    # Rate Limiting & Protection
    MAX_REQUESTS_PER_SEC_PER_IP: int = 40
    # Trust X-Real-IP from the reverse proxy (NGINX) for per-client limits. Only enable when the
    # backends are reachable solely through the proxy, otherwise clients can spoof their IP.
    TRUST_PROXY_HEADERS: bool = False

    # Required (via X-Admin-Token header) for destructive admin endpoints unless DEMO_MODE is on
    ADMIN_TOKEN: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
