import os
import time

import pika

from .db import OrderRepository
from .events import EXCHANGE


def publish_batch(repo, channel) -> int:
    # Single relay replica assumed: fetch_unpublished takes no row locks,
    # so a second replica would double-publish. Consumers are idempotent anyway.
    # A crash between publish and mark_published republishes the whole batch,
    # not just the in-flight row; same idempotency guarantee absorbs it.
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
