# Event-Driven Order Processing

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

- Phase 3: GitHub Actions CI/CD, images to GHCR, ephemeral kind cluster per PR,
  secrets management, liveness probes and resource limits on app manifests
- Phase 4: kube-prometheus-stack, custom Grafana dashboards, alert rules,
  k6 load tests, chaos experiment, ADRs

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
