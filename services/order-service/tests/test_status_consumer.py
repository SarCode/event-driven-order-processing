from app.status_consumer import ORDER_STATUS_UPDATES, decide_status


def test_saga_outcome_mapping():
    assert decide_status("inventory.rejected") == "rejected"
    assert decide_status("payment.completed") == "confirmed"
    assert decide_status("payment.failed") == "rejected"


def test_unknown_events_map_to_none():
    assert decide_status("order.created") is None
    assert decide_status("inventory.reserved") is None


def test_order_status_updates_counter_increments_per_status():
    before = ORDER_STATUS_UPDATES.labels(status="confirmed")._value.get()
    ORDER_STATUS_UPDATES.labels(status="confirmed").inc()
    after = ORDER_STATUS_UPDATES.labels(status="confirmed")._value.get()
    assert after - before == 1.0
