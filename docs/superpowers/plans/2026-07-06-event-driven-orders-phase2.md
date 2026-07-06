# Event-Driven Order Processing - Phase 2 (Resilience: Outbox, Saga, DLQ, Idempotency) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the Phase 1 skeleton into a resilient event-driven system: transactional outbox with relay, order saga (inventory → payment → notification) with compensation, dead-letter queue for poison messages, idempotent consumers, Postgres-backed stock.

**Architecture:** order-service stops publishing directly; POST /orders writes the order row and an outbox row in one transaction, and a separate outbox-relay process publishes pending rows to RabbitMQ. A consolidated `workers` service (one image, three entrypoints) hosts inventory, payment, and notification consumers built on a shared runtime (connection retry, DLQ topology, processed-events idempotency). Saga: `order.created` → inventory reserves stock (Postgres) and emits `inventory.reserved`/`inventory.rejected` → payment mock-charges and emits `payment.completed`/`payment.failed` → notification logs; a status-consumer in the order-service image updates the order row (`confirmed`/`rejected`); on `payment.failed` the inventory consumer compensates by releasing stock.

**Tech Stack:** unchanged (Python 3.12, FastAPI, pika, psycopg 3, pytest, Docker/Compose, kind, Terraform). No Terraform changes this phase.

**Event contract v2** (all events, uniform):
```json
{"event_id": "<uuid>", "event_type": "<routing key>", "order_id": "<uuid>", "sku": "<str>", "quantity": <int>}
```
Routing keys: `order.created`, `inventory.reserved`, `inventory.rejected`, `payment.completed`, `payment.failed`. Exchange `orders` (topic). DLX `orders.dlx` (fanout) → queue `orders.dlq`. Queues: `inventory.events` (bindings order.created, payment.failed), `payment.events` (inventory.reserved), `notification.events` (inventory.rejected, payment.completed, payment.failed), `orders.status-events` (inventory.rejected, payment.completed, payment.failed).

**Saga outcomes:** quantity < 50 and stock available → `confirmed`. Stock insufficient → `rejected` (via inventory.rejected). quantity >= 50 → payment fails → `rejected` + stock released (compensation).

**Known accepted tradeoffs (document, don't fix here):** handler exception = immediate DLQ (no retry counter); publish + processed-mark not atomic (at-least-once, idempotency table absorbs duplicates); status-consumer duplicates a minimal consume loop because it lives in the order-service image and cannot import the workers runtime.

**Working directory:** repo root `/Users/sarthakagarwal/Documents/Claude/Projects/AWS Whitepaper`. Environment: docker CLI needs `export PATH="/usr/local/bin:$PATH"`; kind cluster "orders" already running (context kind-orders) with rabbitmq + postgres.

---

## File Structure

```
services/
  order-service/app/
    events.py          # MODIFIED: event_id param, str body, publisher removed
    db.py              # MODIFIED: outbox DDL + save_with_event/get/fetch_unpublished/mark_published
    main.py            # MODIFIED: outbox write instead of publish; GET /orders/{id}
    outbox_relay.py    # NEW: polling publisher (publish_batch pure-ish + main loop)
    status_consumer.py # NEW: saga status updates (decide_status pure + consume loop)
  order-service/tests/
    test_events.py     # MODIFIED
    test_api.py        # MODIFIED
    test_outbox_relay.py   # NEW
    test_status_consumer.py # NEW
  workers/             # NEW consolidated service (replaces services/inventory-worker)
    Dockerfile
    requirements.txt
    requirements-dev.txt
    app/
      __init__.py
      runtime.py       # shared: parse_event, connect_with_retry, ProcessedStore, declare_topology, run_consumer
      inventory.py     # PostgresInventoryStore + handler + entrypoint
      payment.py       # mock charge handler + entrypoint
      notification.py  # log handler + entrypoint
    tests/
      test_runtime.py
      test_inventory.py
      test_payment.py
      test_notification.py
  inventory-worker/    # DELETED in Task 10
deploy/compose/docker-compose.yaml  # MODIFIED: relay, status-consumer, 3 workers
deploy/k8s/            # MODIFIED: new deployments, inventory-worker.yaml replaced by workers.yaml
scripts/smoke.sh       # MODIFIED: saga-aware (poll status to confirmed/rejected)
Makefile               # MODIFIED: build/kind-load workers image
README.md              # MODIFIED: Phase 2 architecture + event table
```

---

### Task 1: order-service events v2

**Files:**
- Modify: `services/order-service/app/events.py`
- Modify: `services/order-service/tests/test_events.py`

- [ ] **Step 1: Rewrite the test** (replace file contents entirely)

`services/order-service/tests/test_events.py`:

```python
import json
from uuid import UUID

from app.events import EXCHANGE, ROUTING_KEY_ORDER_CREATED, order_created_message
from app.schemas import Order


def test_exchange_and_routing_key():
    assert EXCHANGE == "orders"
    assert ROUTING_KEY_ORDER_CREATED == "order.created"


def test_order_created_message_shape():
    order = Order(
        order_id=UUID("11111111-1111-1111-1111-111111111111"),
        sku="ABC-1",
        quantity=2,
    )
    payload = json.loads(order_created_message(order, "22222222-2222-2222-2222-222222222222"))
    assert payload == {
        "event_id": "22222222-2222-2222-2222-222222222222",
        "event_type": "order.created",
        "order_id": "11111111-1111-1111-1111-111111111111",
        "sku": "ABC-1",
        "quantity": 2,
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `services/order-service`): `.venv/bin/python -m pytest tests/test_events.py -v`
Expected: FAIL (`order_created_message` signature mismatch / missing ROUTING_KEY import at old location).

- [ ] **Step 3: Rewrite implementation** (replace file entirely; RabbitPublisher is deleted - the outbox relay publishes now)

`services/order-service/app/events.py`:

```python
import json

from .schemas import Order

EXCHANGE = "orders"
ROUTING_KEY_ORDER_CREATED = "order.created"


def order_created_message(order: Order, event_id: str) -> str:
    return json.dumps(
        {
            "event_id": event_id,
            "event_type": "order.created",
            "order_id": str(order.order_id),
            "sku": order.sku,
            "quantity": order.quantity,
        }
    )
```

- [ ] **Step 4: Run test file to verify it passes** (full suite will fail until Task 3 updates main.py/test_api.py - that is expected; run only this file)

Run: `.venv/bin/python -m pytest tests/test_events.py -v`
Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add services/order-service/app/events.py services/order-service/tests/test_events.py
git commit -m "feat(order-service): event contract v2 with event_id, publisher removed"
```

Note: main.py still imports RabbitPublisher at this commit and full suite is temporarily red; Task 3 restores green. Acceptable because Tasks 1-3 land as a rapid sequence.

---

### Task 2: order-service outbox repository

**Files:**
- Modify: `services/order-service/app/db.py`

- [ ] **Step 1: Replace db.py entirely** (I/O shell; smoke-verified in Task 10)

`services/order-service/app/db.py`:

```python
import psycopg

from .schemas import Order

DDL = """
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    status TEXT NOT NULL
)
"""

OUTBOX_DDL = """
CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    routing_key TEXT NOT NULL,
    body TEXT NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


class OrderRepository:
    def __init__(self, dsn: str):
        self._dsn = dsn

    def init_schema(self) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(DDL)
            conn.execute(OUTBOX_DDL)

    def save_with_event(self, order: Order, event_id: str, routing_key: str, body: str) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "INSERT INTO orders (order_id, sku, quantity, status)"
                " VALUES (%s, %s, %s, %s)",
                (order.order_id, order.sku, order.quantity, order.status),
            )
            conn.execute(
                "INSERT INTO outbox (event_id, routing_key, body) VALUES (%s, %s, %s)",
                (event_id, routing_key, body),
            )

    def get(self, order_id) -> Order | None:
        with psycopg.connect(self._dsn) as conn:
            row = conn.execute(
                "SELECT order_id, sku, quantity, status FROM orders WHERE order_id = %s",
                (order_id,),
            ).fetchone()
        if row is None:
            return None
        return Order(order_id=row[0], sku=row[1], quantity=row[2], status=row[3])

    def fetch_unpublished(self, limit: int = 50) -> list[tuple[int, str, str]]:
        with psycopg.connect(self._dsn) as conn:
            rows = conn.execute(
                "SELECT id, routing_key, body FROM outbox"
                " WHERE NOT published ORDER BY id LIMIT %s",
                (limit,),
            ).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]

    def mark_published(self, ids: list[int]) -> None:
        if not ids:
            return
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "UPDATE outbox SET published = TRUE WHERE id = ANY(%s)",
                (ids,),
            )
```

The old `save()` method is gone; `save_with_event` replaces it (both inserts share one connection context = one transaction, committed together on clean exit).

- [ ] **Step 2: Verify import**

Run (from `services/order-service`): `.venv/bin/python -c "from app.db import OrderRepository; print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add services/order-service/app/db.py
git commit -m "feat(order-service): transactional outbox repository with get and relay queries"
```

---

### Task 3: order-service API v2 (outbox write + GET endpoint)

**Files:**
- Modify: `services/order-service/app/main.py`
- Modify: `services/order-service/tests/test_api.py`

- [ ] **Step 1: Rewrite the test** (replace file entirely)

`services/order-service/tests/test_api.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_api.py -v`
Expected: FAIL (create_app signature/behavior mismatch).

- [ ] **Step 3: Rewrite implementation** (replace file entirely)

`services/order-service/app/main.py`:

```python
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
```

- [ ] **Step 4: Run full order-service suite**

Run: `.venv/bin/python -m pytest tests -v`
Expected: `12 passed` (4 schemas + 2 events + 6 api).

- [ ] **Step 5: Commit**

```bash
git add services/order-service/app/main.py services/order-service/tests/test_api.py
git commit -m "feat(order-service): outbox write on create, GET /orders/{id} for saga status"
```

---

### Task 4: outbox relay

**Files:**
- Create: `services/order-service/app/outbox_relay.py`
- Test: `services/order-service/tests/test_outbox_relay.py`

- [ ] **Step 1: Write the failing test**

`services/order-service/tests/test_outbox_relay.py`:

```python
from app.outbox_relay import publish_batch


class FakeRepo:
    def __init__(self, rows):
        self.rows = rows
        self.marked = []

    def fetch_unpublished(self, limit=50):
        return self.rows

    def mark_published(self, ids):
        self.marked.extend(ids)


class FakeChannel:
    def __init__(self):
        self.published = []

    def basic_publish(self, exchange, routing_key, body, properties):
        self.published.append((exchange, routing_key, body))


def test_publish_batch_empty_returns_zero():
    repo, ch = FakeRepo([]), FakeChannel()
    assert publish_batch(repo, ch) == 0
    assert ch.published == []
    assert repo.marked == []


def test_publish_batch_publishes_and_marks():
    rows = [(1, "order.created", '{"a": 1}'), (2, "order.created", '{"b": 2}')]
    repo, ch = FakeRepo(rows), FakeChannel()
    assert publish_batch(repo, ch) == 2
    assert ch.published == [
        ("orders", "order.created", b'{"a": 1}'),
        ("orders", "order.created", b'{"b": 2}'),
    ]
    assert repo.marked == [1, 2]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_outbox_relay.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.outbox_relay'`.

- [ ] **Step 3: Write implementation**

`services/order-service/app/outbox_relay.py`:

```python
import os
import time

import pika

from .db import OrderRepository
from .events import EXCHANGE


def publish_batch(repo, channel) -> int:
    rows = repo.fetch_unpublished()
    for _id, routing_key, body in rows:
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=body.encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
    repo.mark_published([row[0] for row in rows])
    return len(rows)


def main() -> None:
    repo = OrderRepository(os.environ["DATABASE_URL"])
    repo.init_schema()
    while True:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(os.environ["RABBITMQ_URL"]))
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            print("outbox-relay publishing", flush=True)
            while True:
                if publish_batch(repo, ch) == 0:
                    time.sleep(0.5)
        except pika.exceptions.AMQPError as exc:
            print(f"outbox-relay amqp error, reconnecting: {exc}", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/python -m pytest tests -v`
Expected: `14 passed`.

- [ ] **Step 5: Commit**

```bash
git add services/order-service/app/outbox_relay.py services/order-service/tests/test_outbox_relay.py
git commit -m "feat(order-service): outbox relay process"
```

---

### Task 5: workers service scaffold + shared runtime

**Files:**
- Create: `services/workers/requirements.txt`, `services/workers/requirements-dev.txt`
- Create: `services/workers/app/__init__.py` (empty)
- Create: `services/workers/app/runtime.py`
- Create: `services/workers/Dockerfile`
- Test: `services/workers/tests/test_runtime.py`

- [ ] **Step 1: Requirements + venv**

`services/workers/requirements.txt`:
```
pika>=1.3
psycopg[binary]>=3.1
```

`services/workers/requirements-dev.txt`:
```
-r requirements.txt
pytest>=8.0
```

```bash
cd services/workers
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
```

- [ ] **Step 2: Write the failing test**

`services/workers/tests/test_runtime.py`:

```python
import json

import pytest

from app.runtime import connect_with_retry, parse_event


def valid_event(**overrides):
    event = {
        "event_id": "e-1",
        "event_type": "order.created",
        "order_id": "o-1",
        "sku": "ABC-1",
        "quantity": 2,
    }
    event.update(overrides)
    return event


def test_parse_event_accepts_valid():
    body = json.dumps(valid_event()).encode()
    assert parse_event(body) == valid_event()


def test_parse_event_rejects_non_json():
    with pytest.raises(ValueError):
        parse_event(b"not json")


def test_parse_event_rejects_missing_keys():
    body = json.dumps({"event_type": "order.created"}).encode()
    with pytest.raises(ValueError):
        parse_event(body)


def test_connect_with_retry_returns_after_transient_failures():
    import pika

    attempts = []
    sleeps = []

    def connector(params):
        attempts.append(params)
        if len(attempts) < 3:
            raise pika.exceptions.AMQPConnectionError("not ready")
        return "connection"

    result = connect_with_retry(
        "amqp://guest:guest@localhost:5672/%2F",
        attempts=5,
        delay_seconds=1,
        sleep=sleeps.append,
        connector=connector,
    )
    assert result == "connection"
    assert len(attempts) == 3
    assert sleeps == [1, 1]


def test_connect_with_retry_raises_when_exhausted():
    import pika

    def connector(params):
        raise pika.exceptions.AMQPConnectionError("never ready")

    with pytest.raises(pika.exceptions.AMQPConnectionError):
        connect_with_retry(
            "amqp://guest:guest@localhost:5672/%2F",
            attempts=2,
            delay_seconds=1,
            sleep=lambda _s: None,
            connector=connector,
        )
```

- [ ] **Step 3: Run test to verify it fails**

Run (from `services/workers`): `.venv/bin/python -m pytest tests -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app'`.

- [ ] **Step 4: Write implementation**

Create empty `services/workers/app/__init__.py`, then `services/workers/app/runtime.py`:

```python
import json
import time

import pika
import psycopg

EXCHANGE = "orders"
DLX = "orders.dlx"
DLQ = "orders.dlq"

REQUIRED_KEYS = {"event_id", "event_type", "order_id", "sku", "quantity"}

PROCESSED_DDL = """
CREATE TABLE IF NOT EXISTS processed_events (
    consumer TEXT NOT NULL,
    event_id UUID NOT NULL,
    PRIMARY KEY (consumer, event_id)
)
"""


def parse_event(body: bytes) -> dict:
    try:
        event = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(f"malformed event body: {exc}") from exc
    if not isinstance(event, dict) or not REQUIRED_KEYS.issubset(event):
        raise ValueError(f"event missing required keys: {body!r}")
    return event


def connect_with_retry(
    url: str,
    attempts: int = 30,
    delay_seconds: float = 2,
    sleep=time.sleep,
    connector=pika.BlockingConnection,
):
    last_error = None
    for attempt in range(attempts):
        try:
            return connector(pika.URLParameters(url))
        except pika.exceptions.AMQPConnectionError as exc:
            last_error = exc
            if attempt < attempts - 1:
                sleep(delay_seconds)
    raise last_error


class ProcessedStore:
    """Idempotency ledger. mark() after handling makes consumers safe
    against at-least-once redelivery."""

    def __init__(self, dsn: str):
        self._dsn = dsn

    def init_schema(self) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(PROCESSED_DDL)

    def seen(self, consumer: str, event_id: str) -> bool:
        with psycopg.connect(self._dsn) as conn:
            row = conn.execute(
                "SELECT 1 FROM processed_events WHERE consumer = %s AND event_id = %s",
                (consumer, event_id),
            ).fetchone()
        return row is not None

    def mark(self, consumer: str, event_id: str) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "INSERT INTO processed_events (consumer, event_id) VALUES (%s, %s)"
                " ON CONFLICT DO NOTHING",
                (consumer, event_id),
            )


def declare_topology(channel, queue: str, bindings: tuple[str, ...]) -> None:
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=DLX, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=DLQ, durable=True)
    channel.queue_bind(queue=DLQ, exchange=DLX)
    channel.queue_declare(
        queue=queue,
        durable=True,
        arguments={"x-dead-letter-exchange": DLX},
    )
    for routing_key in bindings:
        channel.queue_bind(queue=queue, exchange=EXCHANGE, routing_key=routing_key)


def run_consumer(*, url: str, dsn: str, queue: str, bindings: tuple[str, ...], consumer_name: str, handler) -> None:
    """At-least-once consumer. Poison messages (parse or handler failure)
    are dead-lettered immediately; duplicates are skipped via ProcessedStore.
    handler(event) returns a list of {"routing_key": str, "body": dict} to publish."""
    processed = ProcessedStore(dsn)
    processed.init_schema()
    while True:
        try:
            conn = connect_with_retry(url)
            ch = conn.channel()
            declare_topology(ch, queue, bindings)

            def on_message(channel, method, properties, body):
                try:
                    event = parse_event(body)
                    if processed.seen(consumer_name, event["event_id"]):
                        channel.basic_ack(delivery_tag=method.delivery_tag)
                        return
                    results = handler(event)
                except Exception as exc:
                    print(f"{consumer_name} dead-lettering message: {exc}", flush=True)
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    return
                for out in results:
                    channel.basic_publish(
                        exchange=EXCHANGE,
                        routing_key=out["routing_key"],
                        body=json.dumps(out["body"]).encode(),
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                processed.mark(consumer_name, event["event_id"])
                channel.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=queue, on_message_callback=on_message)
            print(f"{consumer_name} consuming {queue}", flush=True)
            ch.start_consuming()
        except pika.exceptions.AMQPError as exc:
            print(f"{consumer_name} amqp error, reconnecting: {exc}", flush=True)
            time.sleep(2)
```

- [ ] **Step 5: Run tests**

Run: `.venv/bin/python -m pytest tests -v`
Expected: `5 passed`.

- [ ] **Step 6: Dockerfile** (entrypoint chosen per deployment via command override)

`services/workers/Dockerfile`:

```dockerfile
FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
CMD ["python", "-m", "app.inventory"]
```

- [ ] **Step 7: Commit**

```bash
git add services/workers
git commit -m "feat(workers): shared consumer runtime with retry, DLQ, idempotency"
```

---

### Task 6: inventory consumer (Postgres stock + compensation)

**Files:**
- Create: `services/workers/app/inventory.py`
- Test: `services/workers/tests/test_inventory.py`

- [ ] **Step 1: Write the failing test**

`services/workers/tests/test_inventory.py`:

```python
from app.inventory import make_handler


class FakeStore:
    def __init__(self, reserve_result=True):
        self.reserve_result = reserve_result
        self.reserved = []
        self.released = []

    def reserve(self, sku, quantity):
        self.reserved.append((sku, quantity))
        return self.reserve_result

    def release(self, sku, quantity):
        self.released.append((sku, quantity))


def order_created(quantity=2):
    return {
        "event_id": "e-1",
        "event_type": "order.created",
        "order_id": "o-1",
        "sku": "ABC-1",
        "quantity": quantity,
    }


def test_reserved_emits_inventory_reserved():
    store = FakeStore(reserve_result=True)
    results = make_handler(store)(order_created())
    assert len(results) == 1
    assert results[0]["routing_key"] == "inventory.reserved"
    body = results[0]["body"]
    assert body["event_type"] == "inventory.reserved"
    assert body["order_id"] == "o-1"
    assert body["sku"] == "ABC-1"
    assert body["quantity"] == 2
    assert body["event_id"] != "e-1"
    assert store.reserved == [("ABC-1", 2)]


def test_insufficient_stock_emits_inventory_rejected():
    store = FakeStore(reserve_result=False)
    results = make_handler(store)(order_created())
    assert results[0]["routing_key"] == "inventory.rejected"
    assert results[0]["body"]["event_type"] == "inventory.rejected"


def test_payment_failed_releases_stock_and_emits_nothing():
    store = FakeStore()
    event = order_created()
    event["event_type"] = "payment.failed"
    results = make_handler(store)(event)
    assert results == []
    assert store.released == [("ABC-1", 2)]
    assert store.reserved == []


def test_other_event_types_ignored():
    store = FakeStore()
    event = order_created()
    event["event_type"] = "payment.completed"
    assert make_handler(store)(event) == []
    assert store.reserved == []
    assert store.released == []
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `services/workers`): `.venv/bin/python -m pytest tests/test_inventory.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.inventory'`.

- [ ] **Step 3: Write implementation**

`services/workers/app/inventory.py`:

```python
import os
from uuid import uuid4

import psycopg

from .runtime import run_consumer

INVENTORY_DDL = """
CREATE TABLE IF NOT EXISTS inventory (
    sku TEXT PRIMARY KEY,
    available INTEGER NOT NULL
)
"""

SEED = {"ABC-1": 100, "XYZ-9": 10}


class PostgresInventoryStore:
    def __init__(self, dsn: str):
        self._dsn = dsn

    def init_schema(self) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(INVENTORY_DDL)
            for sku, available in SEED.items():
                conn.execute(
                    "INSERT INTO inventory (sku, available) VALUES (%s, %s)"
                    " ON CONFLICT DO NOTHING",
                    (sku, available),
                )

    def reserve(self, sku: str, quantity: int) -> bool:
        with psycopg.connect(self._dsn) as conn:
            row = conn.execute(
                "UPDATE inventory SET available = available - %s"
                " WHERE sku = %s AND available >= %s RETURNING available",
                (quantity, sku, quantity),
            ).fetchone()
        return row is not None

    def release(self, sku: str, quantity: int) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "UPDATE inventory SET available = available + %s WHERE sku = %s",
                (quantity, sku),
            )


def _saga_event(event: dict, event_type: str) -> dict:
    return {
        "routing_key": event_type,
        "body": {
            "event_id": str(uuid4()),
            "event_type": event_type,
            "order_id": event["order_id"],
            "sku": event["sku"],
            "quantity": event["quantity"],
        },
    }


def make_handler(store):
    def handle(event: dict) -> list[dict]:
        if event["event_type"] == "order.created":
            reserved = store.reserve(event["sku"], event["quantity"])
            return [_saga_event(event, "inventory.reserved" if reserved else "inventory.rejected")]
        if event["event_type"] == "payment.failed":
            store.release(event["sku"], event["quantity"])
            return []
        return []

    return handle


def main() -> None:
    store = PostgresInventoryStore(os.environ["DATABASE_URL"])
    store.init_schema()
    run_consumer(
        url=os.environ["RABBITMQ_URL"],
        dsn=os.environ["DATABASE_URL"],
        queue="inventory.events",
        bindings=("order.created", "payment.failed"),
        consumer_name="inventory",
        handler=make_handler(store),
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/python -m pytest tests -v`
Expected: `9 passed`.

- [ ] **Step 5: Commit**

```bash
git add services/workers/app/inventory.py services/workers/tests/test_inventory.py
git commit -m "feat(workers): inventory consumer with postgres stock and compensation"
```

---

### Task 7: payment consumer

**Files:**
- Create: `services/workers/app/payment.py`
- Test: `services/workers/tests/test_payment.py`

- [ ] **Step 1: Write the failing test**

`services/workers/tests/test_payment.py`:

```python
from app.payment import PAYMENT_FAILURE_THRESHOLD, handle_event


def inventory_reserved(quantity):
    return {
        "event_id": "e-1",
        "event_type": "inventory.reserved",
        "order_id": "o-1",
        "sku": "ABC-1",
        "quantity": quantity,
    }


def test_small_order_completes_payment():
    results = handle_event(inventory_reserved(quantity=2))
    assert len(results) == 1
    assert results[0]["routing_key"] == "payment.completed"
    assert results[0]["body"]["event_type"] == "payment.completed"
    assert results[0]["body"]["order_id"] == "o-1"


def test_order_at_threshold_fails_payment():
    results = handle_event(inventory_reserved(quantity=PAYMENT_FAILURE_THRESHOLD))
    assert results[0]["routing_key"] == "payment.failed"


def test_other_event_types_ignored():
    event = inventory_reserved(quantity=2)
    event["event_type"] = "order.created"
    assert handle_event(event) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_payment.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.payment'`.

- [ ] **Step 3: Write implementation**

`services/workers/app/payment.py`:

```python
import os
from uuid import uuid4

from .runtime import run_consumer

PAYMENT_FAILURE_THRESHOLD = 50
"""Mock charge rule: orders at or above this quantity fail payment.
Deterministic so the saga's failure path is exercisable from the API."""


def handle_event(event: dict) -> list[dict]:
    if event["event_type"] != "inventory.reserved":
        return []
    completed = event["quantity"] < PAYMENT_FAILURE_THRESHOLD
    event_type = "payment.completed" if completed else "payment.failed"
    return [
        {
            "routing_key": event_type,
            "body": {
                "event_id": str(uuid4()),
                "event_type": event_type,
                "order_id": event["order_id"],
                "sku": event["sku"],
                "quantity": event["quantity"],
            },
        }
    ]


def main() -> None:
    run_consumer(
        url=os.environ["RABBITMQ_URL"],
        dsn=os.environ["DATABASE_URL"],
        queue="payment.events",
        bindings=("inventory.reserved",),
        consumer_name="payment",
        handler=handle_event,
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/python -m pytest tests -v`
Expected: `12 passed`.

- [ ] **Step 5: Commit**

```bash
git add services/workers/app/payment.py services/workers/tests/test_payment.py
git commit -m "feat(workers): payment consumer with deterministic mock charge"
```

---

### Task 8: notification consumer

**Files:**
- Create: `services/workers/app/notification.py`
- Test: `services/workers/tests/test_notification.py`

- [ ] **Step 1: Write the failing test**

`services/workers/tests/test_notification.py`:

```python
from app.notification import handle_event


def test_notification_logs_and_emits_nothing(capsys):
    event = {
        "event_id": "e-1",
        "event_type": "payment.completed",
        "order_id": "o-1",
        "sku": "ABC-1",
        "quantity": 2,
    }
    assert handle_event(event) == []
    out = capsys.readouterr().out
    assert "o-1" in out
    assert "payment.completed" in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_notification.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.notification'`.

- [ ] **Step 3: Write implementation**

`services/workers/app/notification.py`:

```python
import os

from .runtime import run_consumer


def handle_event(event: dict) -> list[dict]:
    print(
        f"notification: order {event['order_id']} {event['event_type']}",
        flush=True,
    )
    return []


def main() -> None:
    run_consumer(
        url=os.environ["RABBITMQ_URL"],
        dsn=os.environ["DATABASE_URL"],
        queue="notification.events",
        bindings=("inventory.rejected", "payment.completed", "payment.failed"),
        consumer_name="notification",
        handler=handle_event,
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/python -m pytest tests -v`
Expected: `13 passed`.

- [ ] **Step 5: Commit**

```bash
git add services/workers/app/notification.py services/workers/tests/test_notification.py
git commit -m "feat(workers): notification consumer (mock)"
```

---

### Task 9: order status consumer

**Files:**
- Create: `services/order-service/app/status_consumer.py`
- Test: `services/order-service/tests/test_status_consumer.py`

- [ ] **Step 1: Write the failing test**

`services/order-service/tests/test_status_consumer.py`:

```python
from app.status_consumer import decide_status


def test_saga_outcome_mapping():
    assert decide_status("inventory.rejected") == "rejected"
    assert decide_status("payment.completed") == "confirmed"
    assert decide_status("payment.failed") == "rejected"


def test_unknown_events_map_to_none():
    assert decide_status("order.created") is None
    assert decide_status("inventory.reserved") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `services/order-service`): `.venv/bin/python -m pytest tests/test_status_consumer.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.status_consumer'`.

- [ ] **Step 3: Write implementation**

`services/order-service/app/status_consumer.py`:

```python
import json
import os
import time

import pika
import psycopg

from .events import EXCHANGE

QUEUE = "orders.status-events"
BINDINGS = ("inventory.rejected", "payment.completed", "payment.failed")

STATUS_BY_EVENT = {
    "inventory.rejected": "rejected",
    "payment.completed": "confirmed",
    "payment.failed": "rejected",
}


def decide_status(event_type: str) -> str | None:
    return STATUS_BY_EVENT.get(event_type)


def main() -> None:
    """Minimal consume loop, duplicated from the workers runtime because this
    process ships in the order-service image and cannot import it."""
    dsn = os.environ["DATABASE_URL"]
    while True:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(os.environ["RABBITMQ_URL"]))
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            ch.queue_declare(queue=QUEUE, durable=True)
            for routing_key in BINDINGS:
                ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=routing_key)

            def on_message(channel, method, properties, body):
                event = json.loads(body)
                status = decide_status(event["event_type"])
                if status is not None:
                    with psycopg.connect(dsn) as db:
                        db.execute(
                            "UPDATE orders SET status = %s WHERE order_id = %s",
                            (status, event["order_id"]),
                        )
                print(f"status: order {event['order_id']} -> {status}", flush=True)
                channel.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE, on_message_callback=on_message)
            print("status-consumer consuming", flush=True)
            ch.start_consuming()
        except pika.exceptions.AMQPError as exc:
            print(f"status-consumer amqp error, reconnecting: {exc}", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/python -m pytest tests -v`
Expected: `16 passed`.

- [ ] **Step 5: Commit**

```bash
git add services/order-service/app/status_consumer.py services/order-service/tests/test_status_consumer.py
git commit -m "feat(order-service): saga status consumer"
```

---

### Task 10: compose rewire + saga smoke test (end-to-end verification)

**Files:**
- Modify: `deploy/compose/docker-compose.yaml`
- Modify: `scripts/smoke.sh`
- Modify: `Makefile` (build/kind-load workers image)
- Delete: `services/inventory-worker/` (entire directory)

- [ ] **Step 1: Rewrite compose file**

`deploy/compose/docker-compose.yaml`:

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: orders
      POSTGRES_PASSWORD: orders
      POSTGRES_DB: orders
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U orders"]
      interval: 5s
      timeout: 3s
      retries: 10

  rabbitmq:
    image: rabbitmq:3.13-management
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      timeout: 5s
      retries: 10

  order-service:
    build: ../../services/order-service
    restart: on-failure
    environment:
      DATABASE_URL: postgresql://orders:orders@postgres:5432/orders
    ports:
      - "8000:8000"
    depends_on:
      postgres:
        condition: service_healthy

  outbox-relay:
    build: ../../services/order-service
    command: ["python", "-m", "app.outbox_relay"]
    restart: on-failure
    environment:
      DATABASE_URL: postgresql://orders:orders@postgres:5432/orders
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/%2F
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy

  status-consumer:
    build: ../../services/order-service
    command: ["python", "-m", "app.status_consumer"]
    restart: on-failure
    environment:
      DATABASE_URL: postgresql://orders:orders@postgres:5432/orders
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/%2F
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy

  inventory-worker:
    build: ../../services/workers
    command: ["python", "-m", "app.inventory"]
    restart: on-failure
    environment:
      DATABASE_URL: postgresql://orders:orders@postgres:5432/orders
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/%2F
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy

  payment-worker:
    build: ../../services/workers
    command: ["python", "-m", "app.payment"]
    restart: on-failure
    environment:
      DATABASE_URL: postgresql://orders:orders@postgres:5432/orders
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/%2F
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy

  notification-worker:
    build: ../../services/workers
    command: ["python", "-m", "app.notification"]
    restart: on-failure
    environment:
      DATABASE_URL: postgresql://orders:orders@postgres:5432/orders
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/%2F
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
```

Note: order-service no longer needs RABBITMQ_URL (outbox only).

- [ ] **Step 2: Rewrite smoke script**

`scripts/smoke.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"

wait_for_status() {
  order_id="$1"
  want="$2"
  status=""
  for _ in $(seq 1 30); do
    status=$(curl -sf "$BASE_URL/orders/$order_id" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
    if [ "$status" = "$want" ]; then
      echo "order $order_id reached $want"
      return 0
    fi
    sleep 1
  done
  echo "order $order_id stuck at '$status' (wanted $want)"
  return 1
}

create_order() {
  curl -sf -X POST "$BASE_URL/orders" \
    -H 'Content-Type: application/json' \
    -d "{\"sku\": \"$1\", \"quantity\": $2}" |
    grep -o '"order_id":"[^"]*"' | cut -d'"' -f4
}

echo "checking health..."
curl -sf "$BASE_URL/healthz" | grep -q '"status":"ok"'

echo "happy path: small order should confirm..."
id=$(create_order ABC-1 2)
wait_for_status "$id" confirmed

echo "payment failure path: big order should reject with compensation..."
id=$(create_order ABC-1 60)
wait_for_status "$id" rejected

echo "inventory failure path: order beyond stock should reject..."
id=$(create_order XYZ-9 99)
wait_for_status "$id" rejected

echo "smoke ok"
```

- [ ] **Step 3: Update Makefile build/kind-load targets** (only these two targets change; leave the rest untouched)

```makefile
build:
	docker build -t orders/order-service:dev services/order-service
	docker build -t orders/workers:dev services/workers

kind-load: build
	kind load docker-image orders/order-service:dev --name orders
	kind load docker-image orders/workers:dev --name orders
```

Also update the `test` target to cover workers instead of inventory-worker:

```makefile
test:
	cd services/order-service && .venv/bin/python -m pytest tests -v
	cd services/workers && .venv/bin/python -m pytest tests -v
```

- [ ] **Step 4: Delete migrated service**

```bash
git rm -r services/inventory-worker
```

- [ ] **Step 5: Run the stack and saga smoke test**

```bash
export PATH="/usr/local/bin:$PATH"
make down || true
make up
sleep 20
make smoke
```
Expected: all three saga paths pass, `smoke ok`.

- [ ] **Step 6: Verify compensation released stock**

```bash
docker compose -f deploy/compose/docker-compose.yaml exec postgres \
  psql -U orders -d orders -c "SELECT sku, available FROM inventory ORDER BY sku;"
```
Expected: `ABC-1 | 98` (100 minus the confirmed order's 2; the failed order's 60 was released) and `XYZ-9 | 10` (rejected order never reserved).

- [ ] **Step 7: Verify DLQ catches poison message**

```bash
docker compose -f deploy/compose/docker-compose.yaml exec rabbitmq \
  rabbitmqadmin -u guest -p guest publish exchange=orders routing_key=order.created payload='not json'
sleep 3
docker compose -f deploy/compose/docker-compose.yaml exec rabbitmq \
  rabbitmqctl list_queues name messages | grep orders.dlq
```
Expected: `orders.dlq` count >= 1. (If `rabbitmqadmin` is unavailable in the image, publish the poison message with a one-off python container using pika, e.g. `docker compose ... run --rm --entrypoint python outbox-relay -c "..."` - any working method is fine; report what you used.)

- [ ] **Step 8: Verify idempotency ledger populated**

```bash
docker compose -f deploy/compose/docker-compose.yaml exec postgres \
  psql -U orders -d orders -c "SELECT consumer, count(*) FROM processed_events GROUP BY consumer ORDER BY consumer;"
```
Expected: rows for inventory, payment, notification (counts > 0).

- [ ] **Step 9: Tear down and commit**

```bash
make down
git add -A
git commit -m "feat: saga end-to-end on compose with outbox relay, DLQ, and compensation"
```

---

### Task 11: Kubernetes manifests v2 + in-cluster verification

**Files:**
- Modify: `deploy/k8s/order-service.yaml` (remove RABBITMQ_URL env; API no longer publishes)
- Create: `deploy/k8s/order-workers.yaml` (outbox-relay + status-consumer, order-service image)
- Create: `deploy/k8s/workers.yaml` (inventory/payment/notification deployments, workers image)
- Delete: `deploy/k8s/inventory-worker.yaml`

- [ ] **Step 1: Update order-service.yaml env** (drop RABBITMQ_URL; rest unchanged)

Container env becomes:
```yaml
          env:
            - name: DATABASE_URL
              value: postgresql://orders:orders-dev-password@postgres-postgresql:5432/orders
```

- [ ] **Step 2: Create order-workers.yaml**

`deploy/k8s/order-workers.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: outbox-relay
  namespace: orders
spec:
  replicas: 1
  selector:
    matchLabels:
      app: outbox-relay
  template:
    metadata:
      labels:
        app: outbox-relay
    spec:
      containers:
        - name: outbox-relay
          image: orders/order-service:dev
          imagePullPolicy: Never
          command: ["python", "-m", "app.outbox_relay"]
          env:
            - name: DATABASE_URL
              value: postgresql://orders:orders-dev-password@postgres-postgresql:5432/orders
            - name: RABBITMQ_URL
              value: amqp://orders:orders-dev-password@rabbitmq:5672/%2F
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: status-consumer
  namespace: orders
spec:
  replicas: 1
  selector:
    matchLabels:
      app: status-consumer
  template:
    metadata:
      labels:
        app: status-consumer
    spec:
      containers:
        - name: status-consumer
          image: orders/order-service:dev
          imagePullPolicy: Never
          command: ["python", "-m", "app.status_consumer"]
          env:
            - name: DATABASE_URL
              value: postgresql://orders:orders-dev-password@postgres-postgresql:5432/orders
            - name: RABBITMQ_URL
              value: amqp://orders:orders-dev-password@rabbitmq:5672/%2F
```

- [ ] **Step 3: Create workers.yaml**

`deploy/k8s/workers.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: inventory-worker
  namespace: orders
spec:
  replicas: 1
  selector:
    matchLabels:
      app: inventory-worker
  template:
    metadata:
      labels:
        app: inventory-worker
    spec:
      containers:
        - name: inventory-worker
          image: orders/workers:dev
          imagePullPolicy: Never
          command: ["python", "-m", "app.inventory"]
          env:
            - name: DATABASE_URL
              value: postgresql://orders:orders-dev-password@postgres-postgresql:5432/orders
            - name: RABBITMQ_URL
              value: amqp://orders:orders-dev-password@rabbitmq:5672/%2F
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: payment-worker
  namespace: orders
spec:
  replicas: 1
  selector:
    matchLabels:
      app: payment-worker
  template:
    metadata:
      labels:
        app: payment-worker
    spec:
      containers:
        - name: payment-worker
          image: orders/workers:dev
          imagePullPolicy: Never
          command: ["python", "-m", "app.payment"]
          env:
            - name: DATABASE_URL
              value: postgresql://orders:orders-dev-password@postgres-postgresql:5432/orders
            - name: RABBITMQ_URL
              value: amqp://orders:orders-dev-password@rabbitmq:5672/%2F
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: notification-worker
  namespace: orders
spec:
  replicas: 1
  selector:
    matchLabels:
      app: notification-worker
  template:
    metadata:
      labels:
        app: notification-worker
    spec:
      containers:
        - name: notification-worker
          image: orders/workers:dev
          imagePullPolicy: Never
          command: ["python", "-m", "app.notification"]
          env:
            - name: DATABASE_URL
              value: postgresql://orders:orders-dev-password@postgres-postgresql:5432/orders
            - name: RABBITMQ_URL
              value: amqp://orders:orders-dev-password@rabbitmq:5672/%2F
```

- [ ] **Step 4: Delete old manifest and deploy**

```bash
git rm deploy/k8s/inventory-worker.yaml
export PATH="/usr/local/bin:$PATH"
make kind-load
kubectl --context kind-orders -n orders delete deploy inventory-worker --ignore-not-found
# Phase 1 left a queue bound to order.created with no consumer; without this
# cleanup every order.created event silently piles up there forever.
kubectl --context kind-orders -n orders exec rabbitmq-0 -- rabbitmqctl delete_queue inventory.order-created || true
make deploy-k8s
kubectl --context kind-orders -n orders get pods
```
Expected: order-service, outbox-relay, status-consumer, inventory-worker, payment-worker, notification-worker all Running 1/1 (plus rabbitmq-0, postgres-postgresql-0). Note: old app pods may need a restart to pick up new images: `kubectl --context kind-orders -n orders rollout restart deploy order-service` if it runs a stale image.

- [ ] **Step 5: In-cluster saga smoke**

```bash
make smoke-k8s
```
Expected: all three saga paths, `smoke ok`.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(k8s): saga deployments on kind"
```

---

### Task 12: README v2

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update Architecture section** (replace the existing diagram block and add the event table after it; leave other sections except Roadmap)

New architecture block:

```
POST /orders -> order-service -> Postgres (orders + outbox, one transaction)
                                     |
                     outbox-relay reads outbox -> RabbitMQ "orders" exchange
                                                        |
        order.created -> inventory-worker (reserve stock in Postgres)
                             |-> inventory.reserved -> payment-worker (mock charge)
                             |                            |-> payment.completed
                             |                            |-> payment.failed -> inventory-worker releases stock
                             |-> inventory.rejected
        inventory.rejected / payment.* -> notification-worker (log)
        inventory.rejected / payment.* -> status-consumer -> orders.status (confirmed/rejected)
        poison messages -> orders.dlx -> orders.dlq
```

Event table (add under the diagram):

```markdown
| Event | Producer | Consumers |
| --- | --- | --- |
| order.created | outbox-relay | inventory-worker |
| inventory.reserved | inventory-worker | payment-worker |
| inventory.rejected | inventory-worker | notification-worker, status-consumer |
| payment.completed | payment-worker | notification-worker, status-consumer |
| payment.failed | payment-worker | inventory-worker (compensation), notification-worker, status-consumer |
```

Add a short "Resilience patterns" section:

```markdown
## Resilience patterns

- **Transactional outbox**: order row and event row commit atomically; a relay
  process publishes pending events, so an order is never saved without its event.
- **Saga with compensation**: payment failure releases the reserved stock.
- **Idempotent consumers**: every consumer records processed event_ids in
  Postgres and skips duplicates (at-least-once delivery is safe).
- **Dead-letter queue**: malformed or poison messages are rejected to
  orders.dlx -> orders.dlq instead of crash-looping consumers.
- Mock payment rule: orders with quantity >= 50 fail payment (deterministic
  failure path for demos and tests).
```

Update Roadmap: remove the Phase 2 line, keep Phases 3-4.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: phase 2 README with saga architecture and resilience patterns"
```

---

## Verification checklist (end of Phase 2)

- [ ] `make test` → 16 order-service + 13 workers tests pass
- [ ] `make up && make smoke` → three saga paths pass; ABC-1 available = 98; DLQ >= 1 after poison; processed_events populated
- [ ] `make kind-load && make deploy-k8s && make smoke-k8s` → smoke ok in-cluster
- [ ] `services/inventory-worker/` deleted, `git log` one commit per task, tree clean

## Follow-up plans

1. Phase 3: `2026-07-XX-phase3-cicd.md` - public GitHub repo, Actions (lint/test → build → GHCR → ephemeral kind + smoke on PR → deploy on main), secrets, liveness probes + resource limits
2. Phase 4: `2026-07-XX-phase4-observability.md` - kube-prometheus-stack, app metrics, dashboards, alerts, k6, chaos, ADRs
