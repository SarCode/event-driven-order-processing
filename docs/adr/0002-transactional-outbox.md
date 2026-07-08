# 0002: Transactional outbox instead of direct publish

## Status

Accepted (Phase 2)

## Context

Phase 1's `order-service` wrote the order row to Postgres and then published
`order.created` to RabbitMQ as a second, separate step. That is a dual-write:
if the process crashes, or the publish call fails, after the database commit
but before (or during) the publish, the order exists with no event ever sent,
and the saga never starts. There is no way to make "write row" and "publish
message" atomic across two different systems directly.

## Decision

Adopt the transactional outbox pattern. `create_order` writes the order row
and an outbox row (event id, routing key, serialized body) in the same
Postgres transaction, so both commit or neither does. A separate
`outbox-relay` process polls the outbox table for unpublished rows, publishes
them to RabbitMQ, and marks them published. This decouples "did we durably
record the intent to emit this event" (guaranteed by the DB transaction)
from "did we get it onto the broker" (best-effort, retried by the relay).

Because the relay can crash or retry after a publish that RabbitMQ already
accepted, delivery is at-least-once, not exactly-once. Every consumer
therefore maintains an idempotency ledger (processed event_ids in Postgres)
and skips events it has already handled. Messages a consumer cannot process
(bad schema, unknown routing key) are rejected to a dead-letter exchange/queue
(`orders.dlx` / `orders.dlq`) instead of being retried forever or crashing the
consumer.

## Consequences

- Orders are never silently lost: an order row implies its event will
  eventually be published, even across a relay restart.
- Extra moving part (the relay process) and extra latency between order
  creation and event publish (bounded by the relay's poll interval).
- Every consumer must be written idempotently; this is now a hard
  requirement, not an optimization, and is tested per-consumer.
- The DLQ needs monitoring (ADR-adjacent: the `DeadLetterQueueGrowing` alert
  in Phase 4) since poison messages are now invisible unless watched.
