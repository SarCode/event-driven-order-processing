import json

import pytest

from app.runtime import connect_with_retry, parse_event


def valid_event(**overrides):
    event = {
        "event_id": "e-1",
        "event_type": "order.created",
        "order_id": "o-1",
        "sku": "ABC-1",
        "quantity": 2,
    }
    event.update(overrides)
    return event


def test_parse_event_accepts_valid():
    body = json.dumps(valid_event()).encode()
    assert parse_event(body) == valid_event()


def test_parse_event_rejects_non_json():
    with pytest.raises(ValueError):
        parse_event(b"not json")


def test_parse_event_rejects_missing_keys():
    body = json.dumps({"event_type": "order.created"}).encode()
    with pytest.raises(ValueError):
        parse_event(body)


def test_connect_with_retry_returns_after_transient_failures():
    import pika

    attempts = []
    sleeps = []

    def connector(params):
        attempts.append(params)
        if len(attempts) < 3:
            raise pika.exceptions.AMQPConnectionError("not ready")
        return "connection"

    result = connect_with_retry(
        "amqp://guest:guest@localhost:5672/%2F",
        attempts=5,
        delay_seconds=1,
        sleep=sleeps.append,
        connector=connector,
    )
    assert result == "connection"
    assert len(attempts) == 3
    assert sleeps == [1, 1]


def test_connect_with_retry_raises_when_exhausted():
    import pika

    def connector(params):
        raise pika.exceptions.AMQPConnectionError("never ready")

    with pytest.raises(pika.exceptions.AMQPConnectionError):
        connect_with_retry(
            "amqp://guest:guest@localhost:5672/%2F",
            attempts=2,
            delay_seconds=1,
            sleep=lambda _s: None,
            connector=connector,
        )
