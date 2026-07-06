import json
from uuid import UUID

from app.events import EXCHANGE, ROUTING_KEY_ORDER_CREATED, order_created_message
from app.schemas import Order


def test_exchange_and_routing_key():
    assert EXCHANGE == "orders"
    assert ROUTING_KEY_ORDER_CREATED == "order.created"


def test_order_created_message_shape():
    order = Order(
        order_id=UUID("11111111-1111-1111-1111-111111111111"),
        sku="ABC-1",
        quantity=2,
    )
    payload = json.loads(order_created_message(order, "22222222-2222-2222-2222-222222222222"))
    assert payload == {
        "event_id": "22222222-2222-2222-2222-222222222222",
        "event_type": "order.created",
        "order_id": "11111111-1111-1111-1111-111111111111",
        "sku": "ABC-1",
        "quantity": 2,
    }
