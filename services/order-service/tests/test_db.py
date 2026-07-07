import psycopg
import pytest
from app.db import OrderRepository


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
        if self.error is not None:
            raise self.error
        self.executed.append(sql)


def make_connect(monkeypatch, conns):
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

    OrderRepository("postgresql://ignored").init_schema()

    assert len(calls) == 2
    assert len(winner.executed) == 2


def test_init_schema_retries_after_duplicate_table(monkeypatch):
    race = psycopg.errors.DuplicateTable('relation "orders" already exists')
    calls = make_connect(monkeypatch, [FakeSchemaConn(error=race), FakeSchemaConn()])

    OrderRepository("postgresql://ignored").init_schema()

    assert len(calls) == 2


def test_init_schema_raises_when_race_never_resolves(monkeypatch):
    race = psycopg.errors.UniqueViolation("duplicate key value")
    conns = [FakeSchemaConn(error=race) for _ in range(10)]
    calls = make_connect(monkeypatch, conns)

    with pytest.raises(psycopg.errors.UniqueViolation):
        OrderRepository("postgresql://ignored").init_schema()

    assert 1 < len(calls) < 10


def test_init_schema_propagates_unrelated_errors_without_retry(monkeypatch):
    boom = psycopg.OperationalError("connection refused")
    conns = [FakeSchemaConn(error=boom) for _ in range(10)]
    calls = make_connect(monkeypatch, conns)

    with pytest.raises(psycopg.OperationalError):
        OrderRepository("postgresql://ignored").init_schema()

    assert len(calls) == 1
