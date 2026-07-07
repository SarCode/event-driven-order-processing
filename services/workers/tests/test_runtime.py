import json

import psycopg
import pytest
from app.runtime import (
    EVENTS_DEADLETTERED,
    EVENTS_PROCESSED,
    ProcessedStore,
    connect_with_retry,
    parse_event,
)


class FakeSchemaConn:
    """Stands in for a psycopg connection during init_schema. Optionally raises
    a concurrent-DDL race error on execute."""

    def __init__(self, error=None):
        self.error = error
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append(sql)
        if self.error is not None:
            raise self.error


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


def make_connect(monkeypatch, conns):
    """Patch psycopg.connect to hand out fake connections in order."""
    remaining = list(conns)
    calls = []

    def fake_connect(dsn):
        calls.append(dsn)
        return remaining.pop(0)

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    return calls


def test_init_schema_retries_after_concurrent_create_race(monkeypatch):
    race = psycopg.errors.UniqueViolation(
        'duplicate key value violates unique constraint "pg_type_typname_nsp_index"'
    )
    loser, winner = FakeSchemaConn(error=race), FakeSchemaConn()
    calls = make_connect(monkeypatch, [loser, winner])

    ProcessedStore("postgresql://ignored").init_schema()

    assert len(calls) == 2
    assert winner.executed


def test_init_schema_retries_after_duplicate_table(monkeypatch):
    race = psycopg.errors.DuplicateTable('relation "processed_events" already exists')
    calls = make_connect(monkeypatch, [FakeSchemaConn(error=race), FakeSchemaConn()])

    ProcessedStore("postgresql://ignored").init_schema()

    assert len(calls) == 2


def test_init_schema_raises_when_race_never_resolves(monkeypatch):
    race = psycopg.errors.UniqueViolation("duplicate key value")
    conns = [FakeSchemaConn(error=race) for _ in range(10)]
    calls = make_connect(monkeypatch, conns)

    with pytest.raises(psycopg.errors.UniqueViolation):
        ProcessedStore("postgresql://ignored").init_schema()

    assert 1 < len(calls) < 10


def test_init_schema_propagates_unrelated_errors_without_retry(monkeypatch):
    boom = psycopg.OperationalError("connection refused")
    conns = [FakeSchemaConn(error=boom) for _ in range(10)]
    calls = make_connect(monkeypatch, conns)

    with pytest.raises(psycopg.OperationalError):
        ProcessedStore("postgresql://ignored").init_schema()

    assert len(calls) == 1


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


def test_events_processed_counter_increments_per_consumer():
    before = EVENTS_PROCESSED.labels(consumer="test-consumer")._value.get()
    EVENTS_PROCESSED.labels(consumer="test-consumer").inc()
    after = EVENTS_PROCESSED.labels(consumer="test-consumer")._value.get()
    assert after - before == 1.0


def test_events_deadlettered_counter_increments_per_consumer():
    before = EVENTS_DEADLETTERED.labels(consumer="test-consumer")._value.get()
    EVENTS_DEADLETTERED.labels(consumer="test-consumer").inc()
    after = EVENTS_DEADLETTERED.labels(consumer="test-consumer")._value.get()
    assert after - before == 1.0
