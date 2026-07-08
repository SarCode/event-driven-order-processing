# 0001: Event-driven architecture with RabbitMQ standing in for SNS/SQS

## Status

Accepted (Phase 1)

## Context

The project's goal is to recreate the AWS whitepaper pattern for event-driven
order processing (API Gateway -> SNS/SQS -> Lambda consumers -> RDS) with
open-source components that run locally, without an AWS account or ongoing
cloud cost. We need a message broker that offers topic-based fan-out (one
event, multiple independent consumers) and durable per-consumer queues, since
that is the structural property that makes SNS/SQS useful in the reference
architecture - not any AWS-specific API.

## Decision

Use RabbitMQ as the local stand-in for SNS/SQS: a single topic exchange
(`orders`) that events are published to, with one durable queue per consumer
bound by routing key. This mirrors SNS fan-out (one topic, many
subscriptions) plus SQS's per-consumer buffering and redelivery, using a
single broker instead of two managed AWS services. FastAPI's `order-service`
plays the API Gateway + Lambda role for the write path; small Python
processes (`inventory-worker`, `payment-worker`, `notification-worker`,
`status-consumer`) play the Lambda consumer role.

## Consequences

- Local development and CI need no AWS credentials or account; the whole
  stack runs in Docker Compose or kind.
- Routing-key-based topic exchange gives the same fan-out semantics as SNS
  topics/subscriptions, so the saga (Task 1-2 of Phase 2) maps cleanly onto
  it later.
- We give up SNS/SQS-specific managed features (server-side encryption at
  rest, IAM-scoped access per queue, CloudWatch-native metrics) and take on
  operating a broker ourselves, including its own failure modes (see ADR
  0004 for the Bitnami image/chart complication this introduces).
- Because RabbitMQ is a single broker rather than two decoupled services,
  broker downtime affects both publish and consume paths simultaneously,
  which SNS/SQS's separation would partially insulate against.
