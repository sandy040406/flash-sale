from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

def utcnow():
    return datetime.now(timezone.utc)

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    price = Column(Integer, nullable=False) # In cents or smallest unit
    total_stock = Column(Integer, nullable=False, default=500)
    available_stock = Column(Integer, nullable=False, default=500)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    orders = relationship("Order", back_populates="product")

class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    ticket_id = Column(String(64), unique=True, index=True, nullable=False)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False, index=True)
    user_id = Column(String(64), nullable=False, index=True)
    status = Column(String(20), nullable=False, default="CONFIRMED") # CONFIRMED, FAILED
    created_at = Column(DateTime, default=utcnow)

    product = relationship("Product", back_populates="orders")

    __table_args__ = (
        Index("idx_product_user", "product_id", "user_id", unique=True),
    )
