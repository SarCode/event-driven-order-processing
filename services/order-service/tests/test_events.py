import json
from uuid import UUID

from app.events import EXCHANGE, order_created_message
from app.schemas import Order


def test_exchange_name():
    assert EXCHANGE == "orders"


def test_order_created_message_shape():
    order = Order(
        order_id=UUID("11111111-1111-1111-1111-111111111111"),
        sku="ABC-1",
        quantity=2,
    )
    payload = json.loads(order_created_message(order))
    assert payload == {
        "event_type": "order.created",
        "order_id": "11111111-1111-1111-1111-111111111111",
        "sku": "ABC-1",
        "quantity": 2,
    }
