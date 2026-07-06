from app.notification import handle_event


def test_notification_logs_and_emits_nothing(capsys):
    event = {
        "event_id": "e-1",
        "event_type": "payment.completed",
        "order_id": "o-1",
        "sku": "ABC-1",
        "quantity": 2,
    }
    assert handle_event(event) == []
    out = capsys.readouterr().out
    assert "o-1" in out
    assert "payment.completed" in out
