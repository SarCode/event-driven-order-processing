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
