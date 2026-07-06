import json

from app.handler import InventoryStore, handle_order_created


def make_body(order_id="o-1", sku="ABC-1", quantity=2):
    return json.dumps(
        {
            "event_type": "order.created",
            "order_id": order_id,
            "sku": sku,
            "quantity": quantity,
        }
    ).encode()


def test_reserves_stock_when_available():
    store = InventoryStore({"ABC-1": 5})
    result = handle_order_created(make_body(quantity=2), store)
    assert result == {"order_id": "o-1", "sku": "ABC-1", "reserved": True}
    assert store.available("ABC-1") == 3


def test_rejects_when_insufficient_stock():
    store = InventoryStore({"ABC-1": 1})
    result = handle_order_created(make_body(quantity=2), store)
    assert result["reserved"] is False
    assert store.available("ABC-1") == 1


def test_unknown_sku_has_zero_stock():
    store = InventoryStore({})
    result = handle_order_created(make_body(sku="NOPE"), store)
    assert result["reserved"] is False
