import psycopg

from .schemas import Order

DDL = """
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    status TEXT NOT NULL
)
"""


class OrderRepository:
    def __init__(self, dsn: str):
        self._dsn = dsn

    def init_schema(self) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(DDL)

    def save(self, order: Order) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "INSERT INTO orders (order_id, sku, quantity, status)"
                " VALUES (%s, %s, %s, %s)",
                (order.order_id, order.sku, order.quantity, order.status),
            )
