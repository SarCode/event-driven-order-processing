# Event-Driven Order Processing - Phase 4 (Observability) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Full observability on the kind cluster: kube-prometheus-stack, application metrics from every service, RabbitMQ metrics, a Grafana saga dashboard, alert rules, a k6 load test, a chaos experiment proving saga recovery, and ADRs documenting the architecture decisions.

**Architecture:** kube-prometheus-stack installed by the same Terraform config (prometheus-community chart repo is unaffected by the Bitnami breakage). Services expose Prometheus metrics: order-service adds `/metrics` (prometheus-client), workers/relay/status-consumer each start a metrics HTTP server on port 9464. PodMonitors scrape them; the Bitnami RabbitMQ chart enables its Prometheus plugin + ServiceMonitor. One Grafana dashboard (provisioned via configmap sidecar) shows order rate, saga outcomes, queue depth, DLQ size, and API latency. PrometheusRules alert on DLQ growth, dead consumers, and API p95 latency. k6 drives load; a chaos script kills the inventory worker mid-saga and asserts every order still reaches a terminal status with no stock leak.

**Metric names (canonical, used across tasks):**
- order-service: `orders_created_total` (Counter), `http_request_duration_seconds` (Histogram, labels: path) - both via prometheus-client
- workers runtime: `events_processed_total{consumer}`, `events_deadlettered_total{consumer}` (Counters)
- outbox relay: `outbox_published_total` (Counter)
- status consumer: `order_status_updates_total{status}` (Counter)
- All Python metrics servers listen on port 9464 (`start_http_server`), pods get `metrics: "9464"` label

**Known constraints:** kube-prometheus-stack is heavy (~2 min install, ~1GB memory on the kind node). Dashboard JSON: the plan specifies exact panels + PromQL; the implementer assembles the standard Grafana JSON envelope around them (mechanical schema, deviation from full-verbatim-code accepted for this one artifact). k6 installed via `brew install k6`.

**Working directory:** repo root. Environment: docker via `export PATH="/usr/local/bin:$PATH"`; kind/terraform/gh at /opt/homebrew/bin; cluster "orders" live with Phase 3 stack (secret-based creds, probes, resources).

---

## File Structure

```
terraform/main.tf                      # MODIFIED: kube-prometheus-stack release, rabbitmq metrics values
deploy/k8s/monitoring/podmonitors.yaml # NEW: PodMonitors for app services
deploy/k8s/monitoring/dashboard.yaml   # NEW: ConfigMap with Grafana dashboard JSON
deploy/k8s/monitoring/alerts.yaml      # NEW: PrometheusRule
services/order-service/app/metrics.py  # NEW + main.py wiring
services/workers/app/runtime.py        # MODIFIED: counters + metrics server
services/order-service/app/outbox_relay.py    # MODIFIED: counter + metrics server
services/order-service/app/status_consumer.py # MODIFIED: counter + metrics server
scripts/load.js                        # NEW: k6 scenario
scripts/chaos.sh                       # NEW: kill-worker experiment
docs/adr/0001..0004                    # NEW: ADRs
README.md                              # MODIFIED: observability section
Makefile                               # MODIFIED: monitoring targets (grafana port-forward, load, chaos)
```

---

### Task 1: kube-prometheus-stack + RabbitMQ metrics via Terraform

**Files:** Modify `terraform/main.tf`

- [ ] Add namespace + release:

```hcl
resource "kubernetes_namespace" "monitoring" {
  metadata {
    name = "monitoring"
  }
}

resource "helm_release" "kube_prometheus_stack" {
  name       = "kps"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  version    = "62.7.0"
  namespace  = kubernetes_namespace.monitoring.metadata[0].name
  timeout    = 900

  set {
    name  = "grafana.adminPassword"
    value = var.app_password
  }
  # Scrape PodMonitors/ServiceMonitors from all namespaces without label gating
  set {
    name  = "prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues"
    value = "false"
  }
  set {
    name  = "prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues"
    value = "false"
  }
  set {
    name  = "prometheus.prometheusSpec.ruleSelectorNilUsesHelmValues"
    value = "false"
  }
}
```

- [ ] Enable RabbitMQ metrics on the existing rabbitmq helm_release (add set blocks):

```hcl
  set {
    name  = "metrics.enabled"
    value = "true"
  }
  set {
    name  = "metrics.serviceMonitor.enabled"
    value = "true"
  }
```
(ServiceMonitor CRD must exist first: add `depends_on = [helm_release.kube_prometheus_stack]`? NO - circular ordering trap. Instead: apply in two passes. Pass 1: kube-prometheus-stack only. Pass 2: add rabbitmq metrics sets. The task steps below sequence this.)

- [ ] Apply pass 1 (stack), verify: `kubectl --context kind-orders -n monitoring get pods` → prometheus/grafana/operator Running. If chart version 62.7.0 unavailable, pick nearest available 6x version, pin it, report.
- [ ] Apply pass 2 (rabbitmq metrics sets), verify rabbitmq pod restarts with metrics sidecar/plugin and ServiceMonitor exists: `kubectl -n orders get servicemonitor`
- [ ] Makefile: add target

```makefile
grafana:
	kubectl -n monitoring port-forward svc/kps-grafana 3000:80
```
(.PHONY too.)

- [ ] Commit: `feat(terraform): kube-prometheus-stack and rabbitmq metrics`

---

### Task 2: order-service metrics (TDD)

**Files:** Create `services/order-service/app/metrics.py`, modify `app/main.py`, test `tests/test_metrics.py`

- [ ] Failing test first:

```python
from fastapi.testclient import TestClient

from app.main import create_app


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
```

- [ ] `app/metrics.py`:

```python
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

ORDERS_CREATED = Counter("orders_created", "Orders accepted by the API")

REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration",
    ["path"],
)


def render() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
```

- [ ] Wire into `main.py`: import `metrics`, `time`; add `/metrics` route returning `Response(content=body, media_type=content_type)`; in `create_order` call `metrics.ORDERS_CREATED.inc()` after save; add FastAPI middleware timing requests into `REQUEST_DURATION.labels(path=request.url.path).observe(...)`. Add `prometheus-client>=0.20` to requirements.txt.
- [ ] Suite green (20 + new = 21), commit: `feat(order-service): prometheus metrics endpoint`

---

### Task 3: workers + relay + status-consumer metrics

**Files:** Modify `services/workers/app/runtime.py`, `services/order-service/app/outbox_relay.py`, `services/order-service/app/status_consumer.py`; tests.

- [ ] runtime.py: add prometheus-client to workers requirements.txt. Define:

```python
from prometheus_client import Counter, start_http_server

EVENTS_PROCESSED = Counter("events_processed", "Events handled", ["consumer"])
EVENTS_DEADLETTERED = Counter("events_deadlettered", "Events sent to DLQ", ["consumer"])
```

In `run_consumer`: call `start_http_server(9464)` once before the while loop; `EVENTS_PROCESSED.labels(consumer=consumer_name).inc()` right after `processed.mark(...)`; `EVENTS_DEADLETTERED.labels(consumer=consumer_name).inc()` in the dead-letter branch. TDD the counter increments by refactoring on_message's body into a testable function ONLY if trivially possible - otherwise test via the existing pure handlers plus a unit test asserting the Counter objects exist and increment when called directly (metrics wiring is I/O-adjacent; smoke verifies live).
- [ ] outbox_relay.py: `OUTBOX_PUBLISHED = Counter("outbox_published", "Outbox rows published")`; `start_http_server(9464)` in main() before loop; inc per row in publish_batch (add counter increment inside the row loop; update test_outbox_relay to assert counter grew by 2 after publish_batch - prometheus_client counters expose `._value.get()` for tests, acceptable here).
- [ ] status_consumer.py: `ORDER_STATUS_UPDATES = Counter("order_status_updates", "Status updates applied", ["status"])`; `start_http_server(9464)` in main(); inc with status label after UPDATE.
- [ ] Both suites green, commit: `feat: prometheus counters and metrics servers in all consumers`

---

### Task 4: PodMonitors + rebuild/redeploy + scrape verification

**Files:** Create `deploy/k8s/monitoring/podmonitors.yaml`

- [ ] Add `metrics: "9464"`-style port. Simplest robust shape: one PodMonitor per app label, all in namespace orders:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: orders-apps
  namespace: orders
spec:
  namespaceSelector:
    matchNames: [orders]
  selector:
    matchExpressions:
      - key: app
        operator: In
        values: [order-service, outbox-relay, status-consumer, inventory-worker, payment-worker, notification-worker]
  podMetricsEndpoints:
    - port: metrics
```
Requires a named container port. Add to every deployment container spec:

```yaml
          ports:
            - name: metrics
              containerPort: 9464
```
(order-service keeps its 8000 port entry too; its Prometheus metrics come from /metrics on 8000, so for order-service the PodMonitor port should be its http port with path /metrics. Split: give order-service its own PodMonitor with `path: /metrics`, port name `http` (name the 8000 port `http`), and keep the shared one for the five consumers.)
- [ ] `make kind-load`, `make deploy-k8s`, rollout restart all six, `make smoke-k8s` still green
- [ ] Verify scraping: port-forward prometheus (`kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 9090:9090 &`), query `curl -s 'localhost:9090/api/v1/query?query=up{namespace="orders"}'` → targets up; `orders_created_total` present after a smoke order; `rabbitmq_queue_messages` present (from rabbitmq plugin)
- [ ] Commit: `feat(k8s): podmonitors and metrics ports for all app services`

---

### Task 5: Grafana dashboard

**Files:** Create `deploy/k8s/monitoring/dashboard.yaml` (ConfigMap, label `grafana_dashboard: "1"` so the kps sidecar imports it)

Panels (PromQL is canon; assemble standard Grafana v11 JSON envelope around them, one row, 7 panels, datasource uid `prometheus`):
1. Order rate: `rate(orders_created_total[5m])`
2. Saga outcomes: `sum by (status) (rate(order_status_updates_total[5m]))`
3. Events processed by consumer: `sum by (consumer) (rate(events_processed_total[5m]))`
4. DLQ size: `rabbitmq_queue_messages{queue="orders.dlq"}`
5. Queue depth (all queues): `sum by (queue) (rabbitmq_queue_messages)`
6. API p95 latency: `histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{path="/orders"}[5m])))`
7. Dead-lettered events: `sum by (consumer) (rate(events_deadlettered_total[5m]))`

- [ ] Apply, verify import: grafana port-forward, `curl -s -u admin:$TF_VAR_app_password_or_default localhost:3000/api/search?query=Order` shows the dashboard (password = orders-dev-password unless overridden)
- [ ] Commit: `feat(monitoring): grafana saga dashboard`

---

### Task 6: Alert rules

**Files:** Create `deploy/k8s/monitoring/alerts.yaml`

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: orders-alerts
  namespace: orders
spec:
  groups:
    - name: orders
      rules:
        - alert: DeadLetterQueueGrowing
          expr: rabbitmq_queue_messages{queue="orders.dlq"} > 0
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: Messages are sitting in the dead letter queue
        - alert: ConsumerDown
          expr: up{namespace="orders"} == 0
          for: 3m
          labels:
            severity: critical
          annotations:
            summary: An orders service scrape target is down
        - alert: ApiHighLatency
          expr: histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{path="/orders"}[10m]))) > 0.5
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: POST /orders p95 latency above 500ms
```

- [ ] Apply, verify rules loaded: prometheus API `/api/v1/rules` contains group `orders`
- [ ] Commit: `feat(monitoring): alert rules for dlq, dead consumers, api latency`

---

### Task 7: k6 load test

**Files:** Create `scripts/load.js`, Makefile `load` target

```javascript
import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  stages: [
    { duration: "30s", target: 10 },
    { duration: "1m", target: 10 },
    { duration: "15s", target: 0 },
  ],
  thresholds: {
    http_req_duration: ["p(95)<500"],
    checks: ["rate>0.99"],
  },
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";

export default function () {
  const quantity = Math.floor(Math.random() * 3) + 1;
  const res = http.post(
    `${BASE_URL}/orders`,
    JSON.stringify({ sku: "ABC-1", quantity: quantity }),
    { headers: { "Content-Type": "application/json" } },
  );
  check(res, { "status 201": (r) => r.status === 201 });
  sleep(1);
}
```

Makefile:
```makefile
load:
	kubectl -n orders port-forward svc/order-service 8000:8000 & \
	sleep 3 && k6 run scripts/load.js; \
	kill %1
```

- [ ] `brew install k6` if missing. Run `make load` against the cluster. Thresholds must pass. NOTE: stock depletion - ~600 orders × avg 2 units = ~1200 units > seeded 100. Before the run, top up stock: `kubectl -n orders exec postgres-postgresql-0 -- env PGPASSWORD=orders-dev-password psql -U orders -d orders -c "UPDATE inventory SET available = 100000 WHERE sku = 'ABC-1';"` and record this in the task report. After run, check Grafana/Prometheus reflect the load (orders_created_total jumped).
- [ ] Commit: `feat: k6 load test with latency thresholds`

---

### Task 8: Chaos experiment

**Files:** Create `scripts/chaos.sh` (executable), Makefile `chaos` target

Script contract:
1. Port-forward order-service (background, like smoke-k8s)
2. POST 10 orders (sku ABC-1, quantity 1), collect ids
3. Immediately `kubectl -n orders delete pod -l app=inventory-worker` (kills the consumer mid-stream; deployment recreates it)
4. Poll each order until terminal status (confirmed/rejected), timeout 120s
5. Assert: all 10 terminal; print stock before/after math: available must equal before minus units of confirmed orders (rejected orders release or never reserve)
6. Exit nonzero on any violation; print a summary table

Makefile target `chaos` runs it. Write clean bash: `set -euo pipefail`, reuse the `|| true` poll pattern from smoke.sh.

- [ ] Run `make chaos` against cluster. All orders must reach terminal state (idempotency + redelivery must hold through the pod kill). Record output. If a real defect surfaces (order stuck forever), STOP and report with logs - that is a finding, not a script bug.
- [ ] Commit: `feat: chaos experiment proving saga recovery through worker loss`

---

### Task 9: ADRs + README v4

**Files:** Create `docs/adr/0001-event-driven-architecture-with-rabbitmq.md`, `0002-transactional-outbox.md`, `0003-local-kind-over-cloud.md`, `0004-bitnami-oci-workaround.md`; modify README.md

ADR format (each ~15-25 lines): Status / Context / Decision / Consequences. Content sources: plan docs + README history. 0001: why event-driven + RabbitMQ as SQS/SNS stand-in. 0002: why outbox over direct publish (atomicity), accepted at-least-once + idempotency ledger. 0003: local kind vs EKS (cost, CI parity via terraform), tradeoff: no real cloud IAM/networking story. 0004: Bitnami/Broadcom breakage, bitnamilegacy stopgap, risks.

README: add "Observability" section (stack components, dashboard screenshot placeholder text, `make grafana`, alert list, `make load`, `make chaos` with one-line results from Tasks 7-8), link ADRs, roadmap cleared (all phases done - replace Roadmap with "Ideas / future work" list: real cloud deploy, OpenTelemetry tracing, schema registry).

- [ ] Commit: `docs: adrs and observability documentation`

---

## Verification checklist (end of Phase 4)

- [ ] monitoring namespace: prometheus, grafana, operator Running; rabbitmq ServiceMonitor live
- [ ] All app metrics scraped (up == 1 for 7 targets: 6 pods + rabbitmq)
- [ ] Dashboard imported and populated during load test
- [ ] Alert rules loaded (3 rules in group orders)
- [ ] k6 thresholds passed; chaos experiment: 10/10 terminal, stock math exact
- [ ] ADRs present; README observability section accurate
- [ ] `make lint` + `make test` still green
