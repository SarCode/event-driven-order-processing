from fastapi.testclient import TestClient

from app.main import create_app


class FakeRepo:
    def __init__(self):
        self.saved = []

    def save(self, order):
        self.saved.append(order)


class FakePublisher:
    def __init__(self):
        self.published = []

    def publish_order_created(self, order):
        self.published.append(order)


def make_client():
    repo, pub = FakeRepo(), FakePublisher()
    app = create_app(repo=repo, publisher=pub)
    return TestClient(app), repo, pub


def test_healthz():
    client, _, _ = make_client()
    assert client.get("/healthz").json() == {"status": "ok"}


def test_create_order_returns_201_with_pending_order():
    client, _, _ = make_client()
    resp = client.post("/orders", json={"sku": "ABC-1", "quantity": 2})
    assert resp.status_code == 201
    body = resp.json()
    assert body["sku"] == "ABC-1"
    assert body["quantity"] == 2
    assert body["status"] == "pending"
    assert body["order_id"]


def test_create_order_saves_and_publishes_same_order():
    client, repo, pub = make_client()
    client.post("/orders", json={"sku": "ABC-1", "quantity": 2})
    assert len(repo.saved) == 1
    assert len(pub.published) == 1
    assert repo.saved[0].order_id == pub.published[0].order_id


def test_rejects_invalid_quantity():
    client, _, _ = make_client()
    resp = client.post("/orders", json={"sku": "ABC-1", "quantity": 0})
    assert resp.status_code == 422
