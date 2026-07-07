from app.main import create_app
from fastapi.testclient import TestClient


class FakeRepo:
    def __init__(self):
        self.orders = {}

    def save_with_event(self, order, event_id, routing_key, body):
        self.orders[order.order_id] = order

    def get(self, order_id):
        return self.orders.get(order_id)


def test_metrics_endpoint_counts_created_orders():
    app = create_app(repo=FakeRepo())
    client = TestClient(app)
    before = client.get("/metrics").text
    client.post("/orders", json={"sku": "ABC-1", "quantity": 1})
    client.post("/orders", json={"sku": "ABC-1", "quantity": 1})
    after = client.get("/metrics").text
    assert "orders_created_total" in after

    def value(text):
        for line in text.splitlines():
            if line.startswith("orders_created_total"):
                return float(line.split()[-1])
        return 0.0

    assert value(after) - value(before) == 2.0
