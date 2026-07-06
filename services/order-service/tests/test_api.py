import json
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from app.main import create_app


class FakeRepo:
    def __init__(self):
        self.saved = []
        self.orders = {}

    def save_with_event(self, order, event_id, routing_key, body):
        self.saved.append((order, event_id, routing_key, body))
        self.orders[order.order_id] = order

    def get(self, order_id):
        return self.orders.get(order_id)


def make_client():
    repo = FakeRepo()
    app = create_app(repo=repo)
    return TestClient(app), repo


def test_healthz():
    client, _ = make_client()
    assert client.get("/healthz").json() == {"status": "ok"}


def test_create_order_returns_201_with_pending_order():
    client, _ = make_client()
    resp = client.post("/orders", json={"sku": "ABC-1", "quantity": 2})
    assert resp.status_code == 201
    body = resp.json()
    assert body["sku"] == "ABC-1"
    assert body["quantity"] == 2
    assert body["status"] == "pending"
    assert body["order_id"]


def test_create_order_writes_outbox_event_in_same_call():
    client, repo = make_client()
    resp = client.post("/orders", json={"sku": "ABC-1", "quantity": 2})
    assert len(repo.saved) == 1
    order, event_id, routing_key, body = repo.saved[0]
    assert routing_key == "order.created"
    payload = json.loads(body)
    assert payload["event_id"] == event_id
    assert payload["order_id"] == resp.json()["order_id"]
    assert payload["event_type"] == "order.created"


def test_get_order_returns_saved_order():
    client, _ = make_client()
    created = client.post("/orders", json={"sku": "ABC-1", "quantity": 2}).json()
    fetched = client.get(f"/orders/{created['order_id']}")
    assert fetched.status_code == 200
    assert fetched.json() == created


def test_get_unknown_order_returns_404():
    client, _ = make_client()
    assert client.get(f"/orders/{uuid4()}").status_code == 404


def test_rejects_invalid_quantity():
    client, _ = make_client()
    resp = client.post("/orders", json={"sku": "ABC-1", "quantity": 0})
    assert resp.status_code == 422
