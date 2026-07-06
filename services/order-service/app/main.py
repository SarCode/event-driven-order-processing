import os
from uuid import uuid4

from fastapi import FastAPI

from .db import OrderRepository
from .events import RabbitPublisher
from .schemas import Order, OrderRequest


def create_app(repo=None, publisher=None) -> FastAPI:
    app = FastAPI(title="order-service")

    if repo is None:
        repo = OrderRepository(os.environ["DATABASE_URL"])
        repo.init_schema()
    if publisher is None:
        publisher = RabbitPublisher(os.environ["RABBITMQ_URL"])

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.post("/orders", status_code=201)
    def create_order(req: OrderRequest) -> Order:
        order = Order(order_id=uuid4(), sku=req.sku, quantity=req.quantity)
        repo.save(order)
        publisher.publish_order_created(order)
        return order

    return app
