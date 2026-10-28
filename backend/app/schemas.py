from typing import Optional
from pydantic import BaseModel, Field

class CheckoutRequest(BaseModel):
    user_id: str = Field(..., description="Unique client/user identifier")
    product_id: int = Field(default=1, description="ID of the product being purchased")
    quantity: int = Field(default=1, ge=1, le=1, description="Quantity allowed per flash sale transaction (strictly 1)")

class CheckoutResponse(BaseModel):
    success: bool
    status: str # "ACCEPTED", "SOLD_OUT", "ALREADY_ORDERED", "RATE_LIMITED"
    ticket_id: Optional[str] = None
    message: str
    tracking_url: Optional[str] = None

class TicketStatusResponse(BaseModel):
    ticket_id: str
    status: str # "QUEUED", "PROCESSING", "CONFIRMED", "FAILED"
    product_id: int
    user_id: str
    message: str
    order_id: Optional[int] = None

class ProductStatusResponse(BaseModel):
    product_id: int
    name: str
    price: int
    total_stock: int
    remaining_stock: int
    is_sold_out: bool
    is_live: bool
    starts_at_ms: Optional[int] = None  # scheduled opening (epoch ms, server clock); None = open
    server_time_ms: int = 0  # server clock, so the client can sync its countdown

class SystemAuditStats(BaseModel):
    product_id: int
    initial_stock: int
    redis_remaining_stock: int
    database_remaining_stock: int
    database_confirmed_orders: int
    total_items_accounted_for: int
    in_transit_queue_count: int
    oversold_count: int
    is_perfectly_consistent: bool
    queue_drain_complete: bool
