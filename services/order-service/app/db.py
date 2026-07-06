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

OUTBOX_DDL = """
CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    routing_key TEXT NOT NULL,
    body TEXT NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


class OrderRepository:
    def __init__(self, dsn: str):
        self._dsn = dsn

    def init_schema(self) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(DDL)
            conn.execute(OUTBOX_DDL)

    def save_with_event(self, order: Order, event_id: str, routing_key: str, body: str) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "INSERT INTO orders (order_id, sku, quantity, status)"
                " VALUES (%s, %s, %s, %s)",
                (order.order_id, order.sku, order.quantity, order.status),
            )
            conn.execute(
                "INSERT INTO outbox (event_id, routing_key, body) VALUES (%s, %s, %s)",
                (event_id, routing_key, body),
            )

    def get(self, order_id) -> Order | None:
        with psycopg.connect(self._dsn) as conn:
            row = conn.execute(
                "SELECT order_id, sku, quantity, status FROM orders WHERE order_id = %s",
                (order_id,),
            ).fetchone()
        if row is None:
            return None
        return Order(order_id=row[0], sku=row[1], quantity=row[2], status=row[3])

    def fetch_unpublished(self, limit: int = 50) -> list[tuple[int, str, str]]:
        with psycopg.connect(self._dsn) as conn:
            rows = conn.execute(
                "SELECT id, routing_key, body FROM outbox"
                " WHERE NOT published ORDER BY id LIMIT %s",
                (limit,),
            ).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]

    def mark_published(self, ids: list[int]) -> None:
        if not ids:
            return
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "UPDATE outbox SET published = TRUE WHERE id = ANY(%s)",
                (ids,),
            )
