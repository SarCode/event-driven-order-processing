import json


class InventoryStore:
    """In-memory stock for Phase 1. Phase 2 moves this to Postgres."""

    def __init__(self, initial: dict[str, int]):
        self._stock = dict(initial)

    def available(self, sku: str) -> int:
        return self._stock.get(sku, 0)

    def reserve(self, sku: str, quantity: int) -> bool:
        if self.available(sku) < quantity:
            return False
        self._stock[sku] -= quantity
        return True


def handle_order_created(body: bytes, store: InventoryStore) -> dict:
    event = json.loads(body)
    reserved = store.reserve(event["sku"], event["quantity"])
    return {
        "order_id": event["order_id"],
        "sku": event["sku"],
        "reserved": reserved,
    }
