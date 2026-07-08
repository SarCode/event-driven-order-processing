# Event-Driven Order Processing

![ci](https://github.com/SarCode/event-driven-order-processing/actions/workflows/ci.yml/badge.svg)
![cd](https://github.com/SarCode/event-driven-order-processing/actions/workflows/cd.yml/badge.svg)

Local recreation of the AWS event-driven architecture whitepaper pattern
(API Gateway -> SNS/SQS -> Lambda consumers -> RDS), built with open-source
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

| Event | Producer | Consumers |
| --- | --- | --- |
| order.created | outbox-relay | inventory-worker |
| inventory.reserved | inventory-worker | payment-worker |
| inventory.rejected | inventory-worker | notification-worker, status-consumer |
| payment.completed | payment-worker | notification-worker, status-consumer |
| payment.failed | payment-worker | inventory-worker (compensation), notification-worker, status-consumer |

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

## Run locally (Docker Compose)

    make up      # build and start the full stack: postgres, rabbitmq,
                 # order-service, outbox-relay, status-consumer, and the
                 # inventory/payment/notification workers
    make smoke   # run all three saga paths, verify final statuses
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

Unit tests cover schemas, event serialization, API behavior (with a fake
repository), the outbox relay batch logic, saga status mapping, consumer
runtime validation and retry, and all three worker handlers. The smoke
test covers the real integration path end to end.

## CI/CD

Every push and pull request runs [ci.yml](.github/workflows/ci.yml):

1. **lint** - ruff over both services
2. **test** - pytest matrix (order-service, workers)
3. **e2e-compose** - full stack via Docker Compose + saga smoke test
4. **e2e-kind** - the same Terraform config used locally provisions an
   ephemeral kind cluster inside the runner; images are built, loaded, and
   the saga smoke test runs in-cluster

On main, [cd.yml](.github/workflows/cd.yml) publishes both images to GHCR
(`ghcr.io/sarcode/event-driven-order-processing/{order-service,workers}`)
tagged with the commit SHA and `latest`.

## Observability

The kind cluster runs a full monitoring stack alongside the app, provisioned
by the same Terraform config: `kube-prometheus-stack` (Prometheus, Alertmanager,
Grafana, and the Prometheus Operator) plus RabbitMQ's Prometheus plugin.
Every service exposes metrics: `order-service` serves `/metrics` on its
existing port via `prometheus-client`; the outbox relay, status consumer, and
all three workers each run a metrics HTTP server on port 9464. PodMonitors
scrape all of them into Prometheus.

**Dashboard.** A provisioned Grafana dashboard, "Order Saga" (uid
`orders-saga`), has 7 panels: order rate, saga outcomes by status, events
processed by consumer, DLQ size, queue depth across all queues, API p95
latency, and dead-lettered events by consumer.

    make grafana   # port-forward kps-grafana to localhost:3000

Log in at `http://localhost:3000` with `admin` / the dev password
(`orders-dev-password` unless overridden via `TF_VAR_app_password`).

**Alert rules** (Prometheus `PrometheusRule`, group `orders`):

| Alert | Severity | Condition |
| --- | --- | --- |
| DeadLetterQueueGrowing | warning | `orders.dlq` has messages for 5m |
| ConsumerDown | critical | a scrape target in `orders` namespace is down for 3m |
| ApiHighLatency | warning | POST /orders p95 > 500ms for 10m |

**Load test.** `make load` runs a k6 scenario (10 virtual users sustained,
~10 rps) against `order-service`. The first run exposed a real bottleneck: a
fresh Postgres connection opened per request pushed p95 to 1.39s against the
500ms threshold. Pooling connections with `psycopg_pool` (commit `5417fb0`)
fixed it: p95 199ms, avg 79ms, 777 requests, 0 failures, all thresholds pass.

**Chaos experiment.** `make chaos` posts 10 orders, then kills the
`inventory-worker` pod mid-saga while those orders are in flight, and polls
until every order reaches a terminal status. All 10 orders reached a
terminal status (10 confirmed) and stock accounting was exact
(98991 -> 98981, matching the expected 98981) - the deployment's pod restart,
combined with idempotent consumers and at-least-once redelivery, recovers
the saga through a worker loss with no stock leak.

**Secrets note.** Terraform state (gitignored, never committed) contains the
dev password in plaintext; treat any copy of `terraform.tfstate` as sensitive
even though the repo itself does not track it. Docker Compose intentionally
uses simpler inline dev credentials (`orders`/`orders`, `guest`/`guest`)
rather than the Kubernetes path's Terraform-managed secret - a deliberate
simplification for the fastest local loop, not a pattern to copy for anything
shared.

See [docs/adr](docs/adr) for the architecture decisions behind this stack:
[0001](docs/adr/0001-event-driven-architecture-with-rabbitmq.md) (RabbitMQ as
the SNS/SQS stand-in),
[0002](docs/adr/0002-transactional-outbox.md) (transactional outbox),
[0003](docs/adr/0003-local-kind-over-cloud.md) (local kind over cloud EKS),
[0004](docs/adr/0004-bitnami-oci-workaround.md) (Bitnami OCI/image workaround).

## Ideas / future work

- Real cloud deployment (EKS, RDS, managed SNS/SQS) to validate the IAM,
  networking, and load-balancer story that kind cannot exercise
- OpenTelemetry tracing across the saga, to replace log-reading with a
  single trace per order
- A schema registry for event payloads instead of implicit versioning
- Connection pooling for the workers (order-service already has it; the
  workers still open connections per operation)
- DLQ replay tooling, so dead-lettered messages can be inspected and
  reprocessed instead of only alerted on

## Troubleshooting

**Bitnami charts and images (Aug 2025 distribution change).** Bitnami moved
chart hosting to OCI and put current images behind Broadcom's paid registry.
This repo's Terraform therefore pulls charts from
`oci://registry-1.docker.io/bitnamicharts` and overrides images to the free
`bitnamilegacy/*` mirrors with `global.security.allowInsecureImages=true`.
Chart versions are pinned in `terraform/main.tf`. The `bitnamilegacy` images
are a stopgap with no update guarantee; revisit before any non-local use.

**Worker or API crash-loops briefly at startup.** RabbitMQ's healthcheck can
pass before its AMQP listener accepts connections, and the services connect
eagerly at boot. Compose uses `restart: on-failure` and Kubernetes restarts
crashed pods by default, so both recover on their own within seconds.
