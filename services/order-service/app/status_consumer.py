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
