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


# CREATE TABLE IF NOT EXISTS is not atomic against concurrent creators: two
# sessions racing on a fresh database can both pass the existence check and
# collide in the catalogs, surfacing UniqueViolation (pg_type/pg_class index)
# or DuplicateTable. The winner's commit makes a retry see the table and no-op.
SCHEMA_RACE_ERRORS = (psycopg.errors.UniqueViolation, psycopg.errors.DuplicateTable)


def create_schema_racing(apply, attempts: int = 3) -> None:
    for attempt in range(attempts):
        try:
            return apply()
        except SCHEMA_RACE_ERRORS:
            if attempt == attempts - 1:
                raise


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
        def create():
            with psycopg.connect(self._dsn) as conn:
                conn.execute(PROCESSED_DDL)

        create_schema_racing(create)

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


def run_consumer(
    *, url: str, dsn: str, queue: str, bindings: tuple[str, ...], consumer_name: str, handler
) -> None:
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
        # psycopg errors escape on_message via start_consuming; treat them like
        # broker hiccups: reconnect and let redelivery plus the idempotency ledger
        # sort it out. A crash between publish and mark still means the handler
        # reruns with fresh event_ids, an accepted at-least-once tradeoff here.
        except (pika.exceptions.AMQPError, psycopg.Error) as exc:
            print(f"{consumer_name} amqp error, reconnecting: {exc}", flush=True)
            time.sleep(2)
