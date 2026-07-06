from app.payment import PAYMENT_FAILURE_THRESHOLD, handle_event


def inventory_reserved(quantity):
    return {
        "event_id": "e-1",
        "event_type": "inventory.reserved",
        "order_id": "o-1",
        "sku": "ABC-1",
        "quantity": quantity,
    }


def test_small_order_completes_payment():
    results = handle_event(inventory_reserved(quantity=2))
    assert len(results) == 1
    assert results[0]["routing_key"] == "payment.completed"
    assert results[0]["body"]["event_type"] == "payment.completed"
    assert results[0]["body"]["order_id"] == "o-1"


def test_order_at_threshold_fails_payment():
    results = handle_event(inventory_reserved(quantity=PAYMENT_FAILURE_THRESHOLD))
    assert results[0]["routing_key"] == "payment.failed"


def test_other_event_types_ignored():
    event = inventory_reserved(quantity=2)
    event["event_type"] = "order.created"
    assert handle_event(event) == []
