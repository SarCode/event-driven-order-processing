from uuid import UUID

from pydantic import BaseModel, Field


class OrderRequest(BaseModel):
    sku: str = Field(min_length=1)
    quantity: int = Field(gt=0)


class Order(BaseModel):
    order_id: UUID
    sku: str
    quantity: int
    status: str = "pending"
