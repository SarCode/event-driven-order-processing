import os
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException

from .db import OrderRepository
from .events import ROUTING_KEY_ORDER_CREATED, order_created_message
from .schemas import Order, OrderRequest


def create_app(repo=None) -> FastAPI:
    app = FastAPI(title="order-service")

    if repo is None:
        repo = OrderRepository(os.environ["DATABASE_URL"])
        repo.init_schema()

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.post("/orders", status_code=201)
    def create_order(req: OrderRequest) -> Order:
        order = Order(order_id=uuid4(), sku=req.sku, quantity=req.quantity)
        event_id = str(uuid4())
        repo.save_with_event(
            order, event_id, ROUTING_KEY_ORDER_CREATED, order_created_message(order, event_id)
        )
        return order

    @app.get("/orders/{order_id}")
    def get_order(order_id: UUID) -> Order:
        order = repo.get(order_id)
        if order is None:
            raise HTTPException(status_code=404, detail="order not found")
        return order

    return app
