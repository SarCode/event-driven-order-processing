import json

import pika

from .schemas import Order

EXCHANGE = "orders"
ROUTING_KEY_ORDER_CREATED = "order.created"


def order_created_message(order: Order) -> bytes:
    return json.dumps(
        {
            "event_type": "order.created",
            "order_id": str(order.order_id),
            "sku": order.sku,
            "quantity": order.quantity,
        }
    ).encode()


class RabbitPublisher:
    """Opens a connection per publish. Simple and correct for Phase 1;
    Phase 2 replaces this with the outbox pattern."""

    def __init__(self, url: str):
        self._url = url

    def publish_order_created(self, order: Order) -> None:
        conn = pika.BlockingConnection(pika.URLParameters(self._url))
        try:
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            ch.basic_publish(
                exchange=EXCHANGE,
                routing_key=ROUTING_KEY_ORDER_CREATED,
                body=order_created_message(order),
                properties=pika.BasicProperties(delivery_mode=2),
            )
        finally:
            conn.close()
