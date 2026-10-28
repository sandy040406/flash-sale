import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "Flash Sale High-Concurrency Engine"
    REDIS_HOST: str = os.getenv("REDIS_HOST", "172.23.39.197")
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    
    # Redis Stream & Consumer Configuration
    STREAM_NAME: str = "flash_sale_stream"
    CONSUMER_GROUP: str = "order_processing_group"
    CONSUMER_NAME: str = f"worker_{os.getpid()}"
    
    # Database
    DATABASE_URL: str = "sqlite+aiosqlite:///./flash_sale.db"
    
    # Flash Sale Parameters
    PRODUCT_ID: int = 1
    INITIAL_STOCK: int = 500
    SALE_ACTIVE: bool = True
    
    # Rate Limiting & Protection
    MAX_REQUESTS_PER_SEC_PER_IP: int = 50

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
