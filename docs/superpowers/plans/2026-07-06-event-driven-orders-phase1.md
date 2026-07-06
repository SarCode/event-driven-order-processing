# Event-Driven Order Processing - Phase 1 (End-to-End Skeleton) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a working end-to-end event-driven order system (order API → RabbitMQ → inventory worker → Postgres) running locally via Docker Compose and on a Terraform-provisioned kind Kubernetes cluster, with a passing smoke test.

**Architecture:** A FastAPI `order-service` persists orders to Postgres and publishes `order.created` events to a RabbitMQ topic exchange. A Python `inventory-worker` consumes those events and reserves stock (in-memory in Phase 1). Infrastructure (kind cluster, RabbitMQ, Postgres) is provisioned with Terraform; app workloads deploy via plain Kubernetes manifests and `kubectl`. This recreates the AWS event-driven architecture whitepaper pattern (API Gateway → SNS/SQS → Lambda consumers → RDS/DynamoDB) with open-source stand-ins.

**Tech Stack:** Python 3.12, FastAPI, pika (RabbitMQ client), psycopg 3, pytest, Docker, Docker Compose, kind, Terraform (tehcyx/kind + hashicorp/helm + hashicorp/kubernetes providers), Bitnami Helm charts (RabbitMQ, PostgreSQL).

**Out of scope (later phase plans):** outbox pattern, idempotent consumers, DLQ, saga, payment/notification workers (Phase 2); GitHub repo, GitHub Actions CI/CD, GHCR, ephemeral kind in CI (Phase 3); kube-prometheus-stack, dashboards, alerts, k6, chaos experiment, ADRs, full docs (Phase 4).

**Working directory for all commands:** repo root `/Users/sarthakagarwal/Documents/Claude/Projects/AWS Whitepaper` unless a step says otherwise.

---

## File Structure

```
.
├── .gitignore
├── Makefile
├── README.md
├── docs/superpowers/plans/          (this plan)
├── services/
│   ├── order-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── requirements-dev.txt
│   │   ├── app/
│   │   │   ├── __init__.py
│   │   │   ├── schemas.py       # Pydantic models (request + order)
│   │   │   ├── events.py        # event serialization + RabbitMQ publisher
│   │   │   ├── db.py            # Postgres order repository
│   │   │   └── main.py          # FastAPI app factory
│   │   └── tests/
│   │       ├── test_schemas.py
│   │       ├── test_events.py
│   │       └── test_api.py
│   └── inventory-worker/
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── requirements-dev.txt
│       ├── app/
│       │   ├── __init__.py
│       │   ├── handler.py       # pure message-handling logic (testable)
│       │   └── consumer.py      # pika consume loop (thin, untested shell)
│       └── tests/
│           └── test_handler.py
├── deploy/
│   ├── compose/
│   │   └── docker-compose.yaml
│   └── k8s/
│       ├── order-service.yaml
│       └── inventory-worker.yaml
├── terraform/
│   ├── versions.tf
│   ├── main.tf
│   └── outputs.tf
└── scripts/
    └── smoke.sh
```

Design rules locked in here: business logic lives in pure functions/classes (`handler.py`, `events.py` serialization) that unit tests cover with fakes; I/O shells (`consumer.py`, pika/psycopg wiring) stay thin and are exercised by the smoke test against real infra.

---

### Task 1: Repo scaffold

**Files:**
- Create: `.gitignore`
- Create: `README.md`

- [ ] **Step 1: Initialize git repo**

```bash
git init
git branch -m main
```

Expected: `Initialized empty Git repository`.

- [ ] **Step 2: Create `.gitignore`**

```gitignore
__pycache__/
*.pyc
.pytest_cache/
.venv/
venv/
.terraform/
.terraform.lock.hcl
terraform.tfstate
terraform.tfstate.backup
*.tfvars
.env
.DS_Store
```

- [ ] **Step 3: Create minimal `README.md`** (expanded in Task 10)

```markdown
# Event-Driven Order Processing

Local recreation of the AWS event-driven architecture pattern (API → queue → workers → database) using FastAPI, RabbitMQ, Postgres, Kubernetes (kind), and Terraform.

Work in progress. See docs/superpowers/plans/ for the implementation plan.
```

- [ ] **Step 4: Commit**

```bash
git add .gitignore README.md docs/
git commit -m "chore: repo scaffold with plan"
```

---

### Task 2: Order service - schemas

**Files:**
- Create: `services/order-service/requirements.txt`
- Create: `services/order-service/requirements-dev.txt`
- Create: `services/order-service/app/__init__.py` (empty)
- Create: `services/order-service/app/schemas.py`
- Test: `services/order-service/tests/test_schemas.py`

- [ ] **Step 1: Create requirements files and venv**

`services/order-service/requirements.txt`:

```
fastapi>=0.111
uvicorn[standard]>=0.30
pydantic>=2.7
psycopg[binary]>=3.1
pika>=1.3
```

`services/order-service/requirements-dev.txt`:

```
-r requirements.txt
pytest>=8.0
httpx>=0.27
```

```bash
cd services/order-service
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
```

Expected: pip finishes with `Successfully installed ...`.

- [ ] **Step 2: Write the failing test**

`services/order-service/tests/test_schemas.py`:

```python
import pytest
from pydantic import ValidationError

from app.schemas import Order, OrderRequest


def test_valid_order_request():
    req = OrderRequest(sku="ABC-1", quantity=2)
    assert req.sku == "ABC-1"
    assert req.quantity == 2


def test_rejects_zero_quantity():
    with pytest.raises(ValidationError):
        OrderRequest(sku="ABC-1", quantity=0)


def test_rejects_empty_sku():
    with pytest.raises(ValidationError):
        OrderRequest(sku="", quantity=1)


def test_order_defaults_to_pending():
    order = Order(order_id="11111111-1111-1111-1111-111111111111", sku="ABC-1", quantity=2)
    assert order.status == "pending"
```

- [ ] **Step 3: Run test to verify it fails**

Run (from `services/order-service`): `.venv/bin/python -m pytest tests/test_schemas.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app'` or `ImportError`.

- [ ] **Step 4: Write minimal implementation**

Create empty `services/order-service/app/__init__.py`, then `services/order-service/app/schemas.py`:

```python
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
```

- [ ] **Step 5: Run test to verify it passes**

Run (from `services/order-service`): `.venv/bin/python -m pytest tests/test_schemas.py -v`
Expected: `4 passed`.

- [ ] **Step 6: Commit**

```bash
git add services/order-service
git commit -m "feat(order-service): request and order schemas"
```

---

### Task 3: Order service - event serialization and publisher

**Files:**
- Create: `services/order-service/app/events.py`
- Test: `services/order-service/tests/test_events.py`

- [ ] **Step 1: Write the failing test**

`services/order-service/tests/test_events.py`:

```python
import json
from uuid import UUID

from app.events import EXCHANGE, order_created_message
from app.schemas import Order


def test_exchange_name():
    assert EXCHANGE == "orders"


def test_order_created_message_shape():
    order = Order(
        order_id=UUID("11111111-1111-1111-1111-111111111111"),
        sku="ABC-1",
        quantity=2,
    )
    payload = json.loads(order_created_message(order))
    assert payload == {
        "event_type": "order.created",
        "order_id": "11111111-1111-1111-1111-111111111111",
        "sku": "ABC-1",
        "quantity": 2,
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `services/order-service`): `.venv/bin/python -m pytest tests/test_events.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.events'`.

- [ ] **Step 3: Write implementation**

`services/order-service/app/events.py`:

```python
import json

import pika

from .schemas import Order

EXCHANGE = "orders"
ROUTING_KEY_ORDER_CREATED = "order.created"


def order_created_message(order: Order) -> bytes:
    return json.dumps(
        {
            "event_type": "order.created",
            "order_id": str(order.order_id),
            "sku": order.sku,
            "quantity": order.quantity,
        }
    ).encode()


class RabbitPublisher:
    """Opens a connection per publish. Simple and correct for Phase 1;
    Phase 2 replaces this with the outbox pattern."""

    def __init__(self, url: str):
        self._url = url

    def publish_order_created(self, order: Order) -> None:
        conn = pika.BlockingConnection(pika.URLParameters(self._url))
        try:
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            ch.basic_publish(
                exchange=EXCHANGE,
                routing_key=ROUTING_KEY_ORDER_CREATED,
                body=order_created_message(order),
                properties=pika.BasicProperties(delivery_mode=2),
            )
        finally:
            conn.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run (from `services/order-service`): `.venv/bin/python -m pytest tests/test_events.py -v`
Expected: `2 passed`. (`RabbitPublisher` is I/O shell; smoke test in Task 7 covers it.)

- [ ] **Step 5: Commit**

```bash
git add services/order-service/app/events.py services/order-service/tests/test_events.py
git commit -m "feat(order-service): order.created event serialization and publisher"
```

---

### Task 4: Order service - Postgres repository

**Files:**
- Create: `services/order-service/app/db.py`

- [ ] **Step 1: Write implementation** (I/O shell, no unit test; covered by smoke test in Task 7)

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


class OrderRepository:
    def __init__(self, dsn: str):
        self._dsn = dsn

    def init_schema(self) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(DDL)

    def save(self, order: Order) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "INSERT INTO orders (order_id, sku, quantity, status)"
                " VALUES (%s, %s, %s, %s)",
                (order.order_id, order.sku, order.quantity, order.status),
            )
```

- [ ] **Step 2: Verify it imports cleanly**

Run (from `services/order-service`): `.venv/bin/python -c "from app.db import OrderRepository; print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add services/order-service/app/db.py
git commit -m "feat(order-service): postgres order repository"
```

---

### Task 5: Order service - API

**Files:**
- Create: `services/order-service/app/main.py`
- Test: `services/order-service/tests/test_api.py`

- [ ] **Step 1: Write the failing test**

`services/order-service/tests/test_api.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `services/order-service`): `.venv/bin/python -m pytest tests/test_api.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.main'`.

- [ ] **Step 3: Write implementation**

`services/order-service/app/main.py`:

```python
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
```

- [ ] **Step 4: Run all order-service tests**

Run (from `services/order-service`): `.venv/bin/python -m pytest tests -v`
Expected: `10 passed`.

- [ ] **Step 5: Commit**

```bash
git add services/order-service/app/main.py services/order-service/tests/test_api.py
git commit -m "feat(order-service): POST /orders endpoint with app factory"
```

---

### Task 6: Inventory worker

**Files:**
- Create: `services/inventory-worker/requirements.txt`
- Create: `services/inventory-worker/requirements-dev.txt`
- Create: `services/inventory-worker/app/__init__.py` (empty)
- Create: `services/inventory-worker/app/handler.py`
- Create: `services/inventory-worker/app/consumer.py`
- Test: `services/inventory-worker/tests/test_handler.py`

- [ ] **Step 1: Create requirements files and venv**

`services/inventory-worker/requirements.txt`:

```
pika>=1.3
```

`services/inventory-worker/requirements-dev.txt`:

```
-r requirements.txt
pytest>=8.0
```

```bash
cd services/inventory-worker
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
```

- [ ] **Step 2: Write the failing test**

`services/inventory-worker/tests/test_handler.py`:

```python
import json

from app.handler import InventoryStore, handle_order_created


def make_body(order_id="o-1", sku="ABC-1", quantity=2):
    return json.dumps(
        {
            "event_type": "order.created",
            "order_id": order_id,
            "sku": sku,
            "quantity": quantity,
        }
    ).encode()


def test_reserves_stock_when_available():
    store = InventoryStore({"ABC-1": 5})
    result = handle_order_created(make_body(quantity=2), store)
    assert result == {"order_id": "o-1", "sku": "ABC-1", "reserved": True}
    assert store.available("ABC-1") == 3


def test_rejects_when_insufficient_stock():
    store = InventoryStore({"ABC-1": 1})
    result = handle_order_created(make_body(quantity=2), store)
    assert result["reserved"] is False
    assert store.available("ABC-1") == 1


def test_unknown_sku_has_zero_stock():
    store = InventoryStore({})
    result = handle_order_created(make_body(sku="NOPE"), store)
    assert result["reserved"] is False
```

- [ ] **Step 3: Run test to verify it fails**

Run (from `services/inventory-worker`): `.venv/bin/python -m pytest tests -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app'`.

- [ ] **Step 4: Write implementation**

Create empty `services/inventory-worker/app/__init__.py`, then `services/inventory-worker/app/handler.py`:

```python
import json


class InventoryStore:
    """In-memory stock for Phase 1. Phase 2 moves this to Postgres."""

    def __init__(self, initial: dict[str, int]):
        self._stock = dict(initial)

    def available(self, sku: str) -> int:
        return self._stock.get(sku, 0)

    def reserve(self, sku: str, quantity: int) -> bool:
        if self.available(sku) < quantity:
            return False
        self._stock[sku] -= quantity
        return True


def handle_order_created(body: bytes, store: InventoryStore) -> dict:
    event = json.loads(body)
    reserved = store.reserve(event["sku"], event["quantity"])
    return {
        "order_id": event["order_id"],
        "sku": event["sku"],
        "reserved": reserved,
    }
```

- [ ] **Step 5: Run test to verify it passes**

Run (from `services/inventory-worker`): `.venv/bin/python -m pytest tests -v`
Expected: `3 passed`.

- [ ] **Step 6: Write the consume loop** (I/O shell, covered by smoke test)

`services/inventory-worker/app/consumer.py`:

```python
import os

import pika

from .handler import InventoryStore, handle_order_created

EXCHANGE = "orders"
QUEUE = "inventory.order-created"
ROUTING_KEY = "order.created"


def main() -> None:
    store = InventoryStore({"ABC-1": 100, "XYZ-9": 10})
    conn = pika.BlockingConnection(pika.URLParameters(os.environ["RABBITMQ_URL"]))
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=ROUTING_KEY)

    def on_message(channel, method, properties, body):
        result = handle_order_created(body, store)
        print(f"processed {result}", flush=True)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE, on_message_callback=on_message)
    print("inventory-worker consuming", flush=True)
    ch.start_consuming()


if __name__ == "__main__":
    main()
```

- [ ] **Step 7: Commit**

```bash
git add services/inventory-worker
git commit -m "feat(inventory-worker): order.created consumer with stock reservation"
```

---

### Task 7: Dockerfiles, Compose stack, smoke test

**Files:**
- Create: `services/order-service/Dockerfile`
- Create: `services/inventory-worker/Dockerfile`
- Create: `deploy/compose/docker-compose.yaml`
- Create: `scripts/smoke.sh`
- Create: `Makefile`

- [ ] **Step 1: Write order-service Dockerfile**

`services/order-service/Dockerfile`:

```dockerfile
FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
EXPOSE 8000
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 2: Write inventory-worker Dockerfile**

`services/inventory-worker/Dockerfile`:

```dockerfile
FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
CMD ["python", "-m", "app.consumer"]
```

- [ ] **Step 3: Write Compose file**

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
    environment:
      DATABASE_URL: postgresql://orders:orders@postgres:5432/orders
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/%2F
    ports:
      - "8000:8000"
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy

  inventory-worker:
    build: ../../services/inventory-worker
    environment:
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/%2F
    depends_on:
      rabbitmq:
        condition: service_healthy
```

- [ ] **Step 4: Write smoke test script**

`scripts/smoke.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"

echo "checking health..."
curl -sf "$BASE_URL/healthz" | grep -q '"status":"ok"'

echo "creating order..."
resp=$(curl -sf -X POST "$BASE_URL/orders" \
  -H 'Content-Type: application/json' \
  -d '{"sku": "ABC-1", "quantity": 2}')
echo "$resp"
echo "$resp" | grep -q '"status":"pending"'

echo "smoke ok"
```

```bash
chmod +x scripts/smoke.sh
```

- [ ] **Step 5: Write Makefile**

`Makefile`:

```makefile
.PHONY: test up down smoke build kind-load deploy-k8s smoke-k8s

test:
	cd services/order-service && .venv/bin/python -m pytest tests -v
	cd services/inventory-worker && .venv/bin/python -m pytest tests -v

up:
	docker compose -f deploy/compose/docker-compose.yaml up --build -d

down:
	docker compose -f deploy/compose/docker-compose.yaml down -v

smoke:
	./scripts/smoke.sh

build:
	docker build -t orders/order-service:dev services/order-service
	docker build -t orders/inventory-worker:dev services/inventory-worker

kind-load: build
	kind load docker-image orders/order-service:dev --name orders
	kind load docker-image orders/inventory-worker:dev --name orders

deploy-k8s:
	kubectl apply -f deploy/k8s/

smoke-k8s:
	kubectl -n orders rollout status deploy/order-service --timeout=120s
	kubectl -n orders port-forward svc/order-service 8000:8000 & \
	sleep 3 && ./scripts/smoke.sh; \
	kill %1
```

- [ ] **Step 6: Run the stack and smoke test**

```bash
make up
sleep 15
make smoke
```

Expected: JSON order response printed, then `smoke ok`.

- [ ] **Step 7: Verify the event was consumed**

```bash
docker compose -f deploy/compose/docker-compose.yaml logs inventory-worker | grep processed
```

Expected: line containing `'reserved': True` for `ABC-1`.

- [ ] **Step 8: Verify the order row exists in Postgres**

```bash
docker compose -f deploy/compose/docker-compose.yaml exec postgres \
  psql -U orders -d orders -c "SELECT sku, quantity, status FROM orders;"
```

Expected: one row `ABC-1 | 2 | pending`.

- [ ] **Step 9: Tear down and commit**

```bash
make down
git add services/order-service/Dockerfile services/inventory-worker/Dockerfile deploy/compose scripts/smoke.sh Makefile
git commit -m "feat: dockerize services with compose stack and smoke test"
```

---

### Task 8: Terraform - kind cluster, RabbitMQ, Postgres

**Files:**
- Create: `terraform/versions.tf`
- Create: `terraform/main.tf`
- Create: `terraform/outputs.tf`

Prerequisites: `terraform`, `kind`, `kubectl`, Docker running. Install if missing: `brew install terraform kind kubectl`.

- [ ] **Step 1: Write provider config**

`terraform/versions.tf`:

```hcl
terraform {
  required_version = ">= 1.7.0"

  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.5"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }
}
```

- [ ] **Step 2: Write main config**

`terraform/main.tf`:

```hcl
provider "kind" {}

resource "kind_cluster" "orders" {
  name           = "orders"
  wait_for_ready = true
}

provider "kubernetes" {
  host                   = kind_cluster.orders.endpoint
  client_certificate     = kind_cluster.orders.client_certificate
  client_key             = kind_cluster.orders.client_key
  cluster_ca_certificate = kind_cluster.orders.cluster_ca_certificate
}

provider "helm" {
  kubernetes {
    host                   = kind_cluster.orders.endpoint
    client_certificate     = kind_cluster.orders.client_certificate
    client_key             = kind_cluster.orders.client_key
    cluster_ca_certificate = kind_cluster.orders.cluster_ca_certificate
  }
}

resource "kubernetes_namespace" "orders" {
  metadata {
    name = "orders"
  }
}

resource "helm_release" "rabbitmq" {
  name       = "rabbitmq"
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "rabbitmq"
  namespace  = kubernetes_namespace.orders.metadata[0].name

  set {
    name  = "auth.username"
    value = "orders"
  }
  set {
    name  = "auth.password"
    value = "orders-dev-password"
  }
}

resource "helm_release" "postgresql" {
  name       = "postgres"
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "postgresql"
  namespace  = kubernetes_namespace.orders.metadata[0].name

  set {
    name  = "auth.username"
    value = "orders"
  }
  set {
    name  = "auth.password"
    value = "orders-dev-password"
  }
  set {
    name  = "auth.database"
    value = "orders"
  }
}
```

Dev-only credentials, intentionally in code for Phase 1; Phase 3 moves them to proper secret handling. Note for the executor: Bitnami changed its free image distribution in late 2025. If a Bitnami chart pod fails with `ImagePullBackOff`, add to that release: `set { name = "global.imageRegistry" value = "docker.io" }` and check `kubectl -n orders describe pod <pod>` for the exact image ref; fallback is pinning `image.repository` to `bitnamilegacy/<name>`. Record whatever was needed in the README troubleshooting section (Task 10).

- [ ] **Step 3: Write outputs**

`terraform/outputs.tf`:

```hcl
output "cluster_name" {
  value = kind_cluster.orders.name
}

output "kubeconfig_context" {
  value = "kind-${kind_cluster.orders.name}"
}
```

- [ ] **Step 4: Init and validate**

```bash
cd terraform
terraform init
terraform validate
```

Expected: `Terraform has been successfully initialized!` then `Success! The configuration is valid.`

- [ ] **Step 5: Apply**

```bash
terraform apply -auto-approve
```

Expected: `Apply complete! Resources: 4 added` (cluster, namespace, 2 helm releases). Takes several minutes.

- [ ] **Step 6: Verify infra pods are running**

```bash
kubectl --context kind-orders -n orders get pods
```

Expected: `rabbitmq-0` and `postgres-postgresql-0` in `Running` state (wait/retry up to ~3 minutes).

- [ ] **Step 7: Commit**

```bash
git add terraform/
git commit -m "feat(terraform): kind cluster with rabbitmq and postgres helm releases"
```

---

### Task 9: Kubernetes manifests for app services + deploy

**Files:**
- Create: `deploy/k8s/order-service.yaml`
- Create: `deploy/k8s/inventory-worker.yaml`

- [ ] **Step 1: Write order-service manifest**

`deploy/k8s/order-service.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
  namespace: orders
spec:
  replicas: 1
  selector:
    matchLabels:
      app: order-service
  template:
    metadata:
      labels:
        app: order-service
    spec:
      containers:
        - name: order-service
          image: orders/order-service:dev
          imagePullPolicy: Never
          ports:
            - containerPort: 8000
          env:
            - name: DATABASE_URL
              value: postgresql://orders:orders-dev-password@postgres-postgresql:5432/orders
            - name: RABBITMQ_URL
              value: amqp://orders:orders-dev-password@rabbitmq:5672/%2F
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8000
            initialDelaySeconds: 3
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: order-service
  namespace: orders
spec:
  selector:
    app: order-service
  ports:
    - port: 8000
      targetPort: 8000
```

- [ ] **Step 2: Write inventory-worker manifest**

`deploy/k8s/inventory-worker.yaml`:

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
          image: orders/inventory-worker:dev
          imagePullPolicy: Never
          env:
            - name: RABBITMQ_URL
              value: amqp://orders:orders-dev-password@rabbitmq:5672/%2F
```

- [ ] **Step 3: Build images, load into kind, deploy**

```bash
make kind-load
make deploy-k8s
kubectl --context kind-orders -n orders get pods
```

Expected: `order-service-...` and `inventory-worker-...` pods reach `Running` with `READY 1/1`.

- [ ] **Step 4: Run smoke test against the cluster**

```bash
make smoke-k8s
```

Expected: `smoke ok`.

- [ ] **Step 5: Verify worker consumed the event in-cluster**

```bash
kubectl --context kind-orders -n orders logs deploy/inventory-worker | grep processed
```

Expected: line containing `'reserved': True`.

- [ ] **Step 6: Commit**

```bash
git add deploy/k8s
git commit -m "feat(k8s): app manifests deployed to terraform-provisioned kind cluster"
```

---

### Task 10: README for Phase 1

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace README.md with full Phase 1 docs**

```markdown
# Event-Driven Order Processing

Local recreation of the AWS event-driven architecture whitepaper pattern
(API Gateway → SNS/SQS → Lambda consumers → RDS), built with open-source
stand-ins and deployed to a Terraform-provisioned Kubernetes cluster.

| AWS reference          | This project                    |
| ---------------------- | ------------------------------- |
| API Gateway + Lambda   | FastAPI order-service           |
| SNS / SQS              | RabbitMQ topic exchange + queue |
| Lambda consumer        | inventory-worker (Python)       |
| RDS                    | PostgreSQL                      |
| CloudFormation         | Terraform                       |
| EKS                    | kind (local Kubernetes)         |

## Architecture

```
POST /orders → order-service → Postgres (orders table)
                     └→ RabbitMQ exchange "orders" (order.created)
                              └→ queue inventory.order-created → inventory-worker
```

## Run locally (Docker Compose)

    make up      # build and start postgres, rabbitmq, order-service, inventory-worker
    make smoke   # POST an order, verify response
    make down    # tear down

## Run on Kubernetes (kind + Terraform)

Prerequisites: docker, terraform, kind, kubectl.

    cd terraform && terraform apply   # kind cluster + rabbitmq + postgres (helm)
    cd .. && make kind-load           # build images, load into kind
    make deploy-k8s                   # deploy app manifests
    make smoke-k8s                    # port-forward + smoke test

Tear down: `cd terraform && terraform destroy`.

## Tests

    make test

Unit tests cover schemas, event serialization, API behavior (with fake
repo/publisher), and inventory reservation logic. The smoke test covers
the real integration path end to end.

## Roadmap

- Phase 2: outbox pattern, idempotent consumers, dead-letter queue, saga,
  payment and notification workers
- Phase 3: GitHub Actions CI/CD, images to GHCR, ephemeral kind cluster per PR
- Phase 4: kube-prometheus-stack, custom Grafana dashboards, alert rules,
  k6 load tests, chaos experiment, ADRs

## Troubleshooting

(Record any Bitnami image pull fixes or platform quirks encountered during
setup here.)
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: phase 1 README with architecture, run instructions, roadmap"
```

---

## Verification checklist (end of Phase 1)

- [ ] `make test` → all unit tests pass (10 order-service + 3 inventory-worker)
- [ ] `make up && make smoke` → `smoke ok`; worker log shows `'reserved': True`; Postgres has the order row
- [ ] `terraform apply` from scratch → cluster + infra pods `Running`
- [ ] `make kind-load && make deploy-k8s && make smoke-k8s` → `smoke ok` in-cluster
- [ ] `git log` shows one commit per task, working tree clean

## Follow-up plans (write after Phase 1 ships)

1. `2026-07-XX-phase2-resilience.md` — outbox table + relay, idempotent consumers (processed-events table), DLQ with retry policy, order saga (inventory → payment → notification), stock moved to Postgres
2. `2026-07-XX-phase3-cicd.md` — public GitHub repo, GitHub Actions (lint/test → build → push GHCR → ephemeral kind deploy + smoke on PR → deploy on main), secret handling
3. `2026-07-XX-phase4-observability.md` — kube-prometheus-stack via Terraform, app metrics endpoints, Grafana dashboards (queue depth, consumer lag, order latency p95, DLQ size), alert rules, k6 load test, chaos experiment (kill worker mid-flow), ADRs
