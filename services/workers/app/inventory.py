import os
from uuid import uuid4

import psycopg

from .runtime import create_schema_racing, run_consumer

INVENTORY_DDL = """
CREATE TABLE IF NOT EXISTS inventory (
    sku TEXT PRIMARY KEY,
    available INTEGER NOT NULL
)
"""

SEED = {"ABC-1": 100, "XYZ-9": 10}


class PostgresInventoryStore:
    def __init__(self, dsn: str):
        self._dsn = dsn

    def init_schema(self) -> None:
        def create():
            with psycopg.connect(self._dsn) as conn:
                conn.execute(INVENTORY_DDL)
                for sku, available in SEED.items():
                    conn.execute(
                        "INSERT INTO inventory (sku, available) VALUES (%s, %s)"
                        " ON CONFLICT DO NOTHING",
                        (sku, available),
                    )

        create_schema_racing(create)

    def reserve(self, sku: str, quantity: int) -> bool:
        with psycopg.connect(self._dsn) as conn:
            row = conn.execute(
                "UPDATE inventory SET available = available - %s"
                " WHERE sku = %s AND available >= %s RETURNING available",
                (quantity, sku, quantity),
            ).fetchone()
        return row is not None

    def release(self, sku: str, quantity: int) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "UPDATE inventory SET available = available + %s WHERE sku = %s",
                (quantity, sku),
            )


def _saga_event(event: dict, event_type: str) -> dict:
    return {
        "routing_key": event_type,
        "body": {
            "event_id": str(uuid4()),
            "event_type": event_type,
            "order_id": event["order_id"],
            "sku": event["sku"],
            "quantity": event["quantity"],
        },
    }


def make_handler(store):
    def handle(event: dict) -> list[dict]:
        if event["event_type"] == "order.created":
            reserved = store.reserve(event["sku"], event["quantity"])
            return [_saga_event(event, "inventory.reserved" if reserved else "inventory.rejected")]
        if event["event_type"] == "payment.failed":
            # No reservation ledger: release trusts that payment.failed only ever
            # follows inventory.reserved (current topology guarantees it). A manual
            # DLQ requeue or new payment.failed producer could inflate stock.
            store.release(event["sku"], event["quantity"])
            return []
        return []

    return handle


def main() -> None:
    store = PostgresInventoryStore(os.environ["DATABASE_URL"])
    store.init_schema()
    run_consumer(
        url=os.environ["RABBITMQ_URL"],
        dsn=os.environ["DATABASE_URL"],
        queue="inventory.events",
        bindings=("order.created", "payment.failed"),
        consumer_name="inventory",
        handler=make_handler(store),
    )


if __name__ == "__main__":
    main()
