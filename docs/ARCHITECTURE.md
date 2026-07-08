# Architecture Deep Dive

This document explains what this project is, why it is shaped the way it is,
and how each piece works internally. The [README](../README.md) is the quick
tour; this is the long version. The four ADRs in [docs/adr](adr) record the
individual decisions; this ties them together.

## 1. The basis: an AWS whitepaper pattern, rebuilt from parts you can run

AWS's event-driven architecture guidance describes a serverless order
pipeline: API Gateway accepts a request, a Lambda writes it and emits an
event through SNS/SQS, downstream Lambdas react (reserve inventory, charge
payment, notify), and state lands in RDS/DynamoDB. The managed services hide
the hard parts: message durability, retries, duplicate delivery, poison
messages, and what happens when a consumer dies mid-work.

The point of this project is to rebuild that pattern with open-source
stand-ins so the hard parts become visible and testable:

| AWS managed piece      | Stand-in here                     | What that exposes                       |
| ---------------------- | --------------------------------- | --------------------------------------- |
| API Gateway + Lambda   | FastAPI `order-service`           | request handling, connection management |
| SNS/SQS                | RabbitMQ topic exchange + queues  | routing, durability, dead-lettering     |
| Lambda consumers       | Python worker processes           | acks, retries, idempotency, crash recovery |
| RDS                    | PostgreSQL                        | transactions, the outbox pattern        |
| CloudFormation         | Terraform                         | reproducible provisioning               |
| EKS                    | kind (local Kubernetes)           | manifests, probes, limits, restarts     |
| CloudWatch             | Prometheus + Grafana              | metrics, dashboards, alert rules        |

Everything runs on one laptop and, identically, inside a GitHub Actions
runner. That "identically" is deliberate: the same Terraform configuration
provisions both, so CI is not a simulation of the environment, it is the
environment.

## 2. The domain: one order, five events, three outcomes

A single business flow drives everything. `POST /orders` creates an order in
status `pending`. From there the saga runs asynchronously:

```
order.created ──> inventory-worker reserves stock
                    ├─ not enough stock ──> inventory.rejected ──> order: rejected
                    └─ reserved ──> inventory.reserved ──> payment-worker (mock charge)
                                       ├─ quantity < 50 ──> payment.completed ──> order: confirmed
                                       └─ quantity >= 50 ─> payment.failed
                                                              ├─> inventory-worker RELEASES the stock (compensation)
                                                              └─> order: rejected
```

Every event carries the same envelope:

```json
{"event_id": "<uuid>", "event_type": "<routing key>", "order_id": "<uuid>", "sku": "...", "quantity": n}
```

The mock payment rule (fail at quantity >= 50) is deterministic on purpose:
every saga branch, including compensation, can be triggered from a plain
HTTP request, which is what the smoke test and chaos experiment do.

## 3. Implementation logic, component by component

### order-service: never lose an event (the transactional outbox)

The naive implementation writes the order to Postgres and then publishes to
RabbitMQ. That is two systems and no transaction across them: crash between
the two and you get an order without an event (stuck forever) or an event
without an order (phantom work). This is the dual-write problem.

The fix (ADR 0002): `POST /orders` writes **two rows in one Postgres
transaction**: the order, and the serialized event into an `outbox` table.
Either both commit or neither does. The API never talks to RabbitMQ at all.

A separate process, the **outbox-relay**, polls the outbox
(`SELECT ... WHERE NOT published ORDER BY id LIMIT 50`), publishes each row
to the `orders` topic exchange with persistent delivery, then marks the
batch published. Publish-then-mark ordering means a crash in between
republishes the batch: **at-least-once** delivery, by choice. Exactly-once
is not achievable across two systems without heavier machinery; instead,
duplicates are made harmless downstream (see the runtime).

Two performance notes baked into the code: the repository uses a lazily
created `psycopg_pool.ConnectionPool` behind a double-checked lock, because
the load test proved a fresh connection per request costs ~1.2s of p95
latency; and the relay assumes a single replica (no row locks), documented
at the call site because idempotent consumers absorb the double-publish that
a second replica would cause.

### The workers runtime: one loop that makes consumers safe

All three workers (inventory, payment, notification) run on a shared
`run_consumer` loop in one image, selected by container command. The loop
encodes the consumer contract:

1. **Validate**: `parse_event` rejects non-JSON and missing-key messages
   with `ValueError`.
2. **Deduplicate**: a `processed_events(consumer, event_id)` table in
   Postgres is checked before handling. Seen it before → ack and skip.
   This is what makes at-least-once delivery safe: redelivered messages are
   recognized by `event_id` and dropped.
3. **Handle**: the business handler is a pure function
   `event -> [events to publish]`, unit-tested with fakes, no I/O inside.
4. **Publish results, mark processed, ack** - in that order. Mark-before-ack
   means a crash between the two causes a redelivery that the ledger
   swallows; no double side effects.
5. **Poison messages** (validation or handler exceptions) are
   `basic_nack(requeue=False)`-ed into a dead-letter exchange
   (`orders.dlx` → `orders.dlq`) instead of crash-looping the consumer.
   There is deliberately no retry counter: transient failures are handled
   by reconnect, not redelivery.
6. **Survive infrastructure**: connection setup retries with backoff, and
   the outer loop catches both AMQP and Postgres errors and reconnects
   rather than dying (an unhandled DB blip would otherwise kill the process
   through pika's callback stack).

### The three handlers

- **inventory** (binds `order.created` and `payment.failed`): stock lives in
  Postgres. Reservation is one race-safe statement:
  `UPDATE inventory SET available = available - n WHERE sku = ? AND available >= n RETURNING ...` -
  concurrent consumers serialize on the row lock, so overselling is
  impossible without a read-modify-write gap. On `payment.failed` it adds
  the stock back: that is the saga's compensation step. A comment marks the
  topology assumption: releases trust that `payment.failed` only ever
  follows a successful reservation.
- **payment** (binds `inventory.reserved`): the deterministic mock charge.
  Emits `payment.completed` or `payment.failed` with a fresh `event_id`.
- **notification** (binds all terminal events): logs a "notification sent"
  line. Deliberately trivial; it exists to show fan-out consumption.

### status-consumer: closing the loop

The order row's status must reflect the saga outcome. A small consumer in
the order-service image maps terminal events to status updates:
`inventory.rejected`/`payment.failed` → `rejected`, `payment.completed` →
`confirmed`, applied as an idempotent `UPDATE`. The mapping is safe without
ordering guards because the saga's branches are mutually exclusive: exactly
one terminal event exists per order. Malformed messages are logged and
dropped (acked) rather than crash-looping, mirroring the workers' hardening.

### Why this set of guarantees

The system chooses **at-least-once delivery + idempotent consumers +
compensation** over distributed transactions. The failure matrix:

| Failure | What happens | Why it's safe |
| --- | --- | --- |
| API crashes mid-request | Transaction rolls back | No order without event, no event without order |
| Relay crashes mid-batch | Whole batch republishes | Ledger drops duplicates by event_id |
| Worker crashes mid-message | Message redelivered (unacked) | Ledger, or not-yet-marked → reprocess is the same pure handler |
| Worker killed entirely | Deployment restarts pod, queue holds messages | Proven by the chaos experiment: 10/10 orders reached terminal state through a kill |
| Payment fails after reservation | `payment.failed` triggers stock release | Compensation, verified by exact stock accounting |
| Garbage message arrives | Dead-lettered to `orders.dlq` | Alert fires if the DLQ has messages for 5 minutes |

## 4. Infrastructure logic

Terraform owns everything stateful: the kind cluster itself (via the
`tehcyx/kind` provider), the RabbitMQ and PostgreSQL Helm releases, the
`kube-prometheus-stack`, and a Kubernetes Secret holding the connection
URLs (single `app_password` variable, dev default, overridable). App
workloads are plain manifests applied with `kubectl` - six Deployments, all
reading credentials via `envFrom` the secret, all with resource
requests/limits, the API with readiness+liveness probes (consumers have no
HTTP surface; their failure mode is process exit + pod restart, documented
in the manifest).

Two provisioning scars worth knowing (ADR 0004 and the troubleshooting
section): Bitnami's chart/image distribution moved behind Broadcom's paid
registry in 2025, so charts come from the OCI registry with pinned versions
and `bitnamilegacy` image overrides; and RabbitMQ's per-queue metrics
(`rabbitmq_queue_messages{queue=...}`) only exist when
`prometheus.return_per_object_metrics = true` is set - without it the DLQ
dashboard panel and alert silently query a metric shape that never exists.

## 5. CI/CD logic

`ci.yml` runs four gates on every push/PR: ruff lint, pytest for both
services, a Compose end-to-end job (fast path), and the flagship
**e2e-kind** job: the runner installs Terraform and kind, runs the same
`terraform apply` used locally to provision a fresh cluster + brokers +
monitoring CRDs, builds and loads the images, deploys the manifests, and
runs the full three-path saga smoke test in-cluster. `cd.yml` publishes
both images to GHCR (`sha` + `latest` tags) on main.

Getting e2e-kind green surfaced three fresh-environment bugs that local
work had masked: Helm resources that need operator CRDs require explicit
`depends_on` when applied from empty state (local two-pass applies hid the
ordering); the kind CLI version must match the cluster the Terraform
provider creates (containerd snapshotter detection fails on skew); and
`kill %1` job-control in Makefiles neither works under dash (CI's shell)
nor preserves the test's exit code. All three fixes are in the history with
their reasoning.

## 6. Observability logic

Every process exports Prometheus metrics: the API serves `/metrics` on its
HTTP port (request histogram labeled by **route template**, not raw path -
labeling by raw path would mint a new time series per order UUID);
relay/status/workers each run a metrics server on :9464 with counters for
events processed, dead-lettered, published, and status updates. PodMonitors
scrape all of them plus RabbitMQ's per-queue gauges - 8 targets total.

The "Order Saga" Grafana dashboard shows the system's pulse: order rate,
saga outcomes, per-consumer throughput, queue depths, DLQ size, API p95.
Three alerts encode "what should never happen": messages sitting in the
DLQ, a dead scrape target, API p95 over 500ms.

The observability stack earned its keep immediately: the k6 load test
(10 VUs, ~10 rps) failed its 500ms p95 threshold at 1.39s, the metrics
pointed at per-request connection churn rather than CPU (zero throttling,
latency inside the request path), and the connection pool fix brought p95
to 199ms. The chaos experiment then killed the inventory worker with ten
orders in flight: all ten reached terminal status and stock accounting was
exact - the queue, the restart policy, and the idempotency ledger doing
precisely what sections 3 and 4 claim.

## 7. Repository map

```
services/order-service/   FastAPI app, outbox repository, relay, status consumer, tests
services/workers/          shared consumer runtime + inventory/payment/notification, tests
deploy/compose/            7-service local stack
deploy/k8s/                app manifests + monitoring (podmonitors, dashboard, alerts)
terraform/                 cluster, brokers, monitoring stack, secret
scripts/                   smoke.sh (saga paths), load.js (k6), chaos.sh
.github/workflows/         ci.yml (4 gates), cd.yml (GHCR)
docs/adr/                  four architecture decision records
docs/superpowers/plans/    the phase-by-phase implementation plans this was built from
```
