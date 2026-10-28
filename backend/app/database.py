from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select, text
from app.config import settings
from app.models import Base, Product

from sqlalchemy import event

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False} if "sqlite" in settings.DATABASE_URL else {}
)

if "sqlite" in settings.DATABASE_URL:
    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=10000")
        cursor.close()

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

async def init_db():
    # Replicas boot concurrently against a shared Postgres: serialize schema
    # creation and seeding with a transaction-scoped advisory lock.
    async with engine.begin() as conn:
        if "postgresql" in settings.DATABASE_URL:
            await conn.execute(text("SELECT pg_advisory_xact_lock(727001)"))
        await conn.run_sync(Base.metadata.create_all)

        # Pre-seed initial product if it doesn't exist
        result = await conn.execute(select(Product.id).where(Product.id == settings.PRODUCT_ID))
        if result.first() is None:
            await conn.execute(
                Product.__table__.insert().values(
                    id=settings.PRODUCT_ID,
                    name="Flagship Ultra 5G (Limited Festival Edition)",
                    price=69900,
                    total_stock=settings.INITIAL_STOCK,
                    available_stock=settings.INITIAL_STOCK,
                )
            )
