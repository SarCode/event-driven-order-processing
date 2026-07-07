from app.status_consumer import decide_status


def test_saga_outcome_mapping():
    assert decide_status("inventory.rejected") == "rejected"
    assert decide_status("payment.completed") == "confirmed"
    assert decide_status("payment.failed") == "rejected"


def test_unknown_events_map_to_none():
    assert decide_status("order.created") is None
    assert decide_status("inventory.reserved") is None
