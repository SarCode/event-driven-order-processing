import json

from .schemas import Order

EXCHANGE = "orders"
ROUTING_KEY_ORDER_CREATED = "order.created"


def order_created_message(order: Order, event_id: str) -> str:
    return json.dumps(
        {
            "event_id": event_id,
            "event_type": "order.created",
            "order_id": str(order.order_id),
            "sku": order.sku,
            "quantity": order.quantity,
        }
    )
