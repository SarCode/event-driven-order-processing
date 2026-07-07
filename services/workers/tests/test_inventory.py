import psycopg

from app.inventory import PostgresInventoryStore, make_handler


class FakeSchemaConn:
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


def test_init_schema_retries_after_concurrent_create_race(monkeypatch):
    race = psycopg.errors.UniqueViolation(
        'duplicate key value violates unique constraint "pg_type_typname_nsp_index"'
    )
    conns = [FakeSchemaConn(error=race), FakeSchemaConn()]
    monkeypatch.setattr(psycopg, "connect", lambda dsn: conns.pop(0))

    PostgresInventoryStore("postgresql://ignored").init_schema()

    # winner connection ran DDL plus one seed insert per sku
    assert conns == []


class FakeStore:
    def __init__(self, reserve_result=True):
        self.reserve_result = reserve_result
        self.reserved = []
        self.released = []

    def reserve(self, sku, quantity):
        self.reserved.append((sku, quantity))
        return self.reserve_result

    def release(self, sku, quantity):
        self.released.append((sku, quantity))


def order_created(quantity=2):
    return {
        "event_id": "e-1",
        "event_type": "order.created",
        "order_id": "o-1",
        "sku": "ABC-1",
        "quantity": quantity,
    }


def test_reserved_emits_inventory_reserved():
    store = FakeStore(reserve_result=True)
    results = make_handler(store)(order_created())
    assert len(results) == 1
    assert results[0]["routing_key"] == "inventory.reserved"
    body = results[0]["body"]
    assert body["event_type"] == "inventory.reserved"
    assert body["order_id"] == "o-1"
    assert body["sku"] == "ABC-1"
    assert body["quantity"] == 2
    assert body["event_id"] != "e-1"
    assert store.reserved == [("ABC-1", 2)]


def test_insufficient_stock_emits_inventory_rejected():
    store = FakeStore(reserve_result=False)
    results = make_handler(store)(order_created())
    assert results[0]["routing_key"] == "inventory.rejected"
    assert results[0]["body"]["event_type"] == "inventory.rejected"


def test_payment_failed_releases_stock_and_emits_nothing():
    store = FakeStore()
    event = order_created()
    event["event_type"] = "payment.failed"
    results = make_handler(store)(event)
    assert results == []
    assert store.released == [("ABC-1", 2)]
    assert store.reserved == []


def test_other_event_types_ignored():
    store = FakeStore()
    event = order_created()
    event["event_type"] = "payment.completed"
    assert make_handler(store)(event) == []
    assert store.reserved == []
    assert store.released == []
