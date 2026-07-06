import os
from uuid import uuid4

from .runtime import run_consumer

PAYMENT_FAILURE_THRESHOLD = 50
"""Mock charge rule: orders at or above this quantity fail payment.
Deterministic so the saga's failure path is exercisable from the API."""


def handle_event(event: dict) -> list[dict]:
    if event["event_type"] != "inventory.reserved":
        return []
    completed = event["quantity"] < PAYMENT_FAILURE_THRESHOLD
    event_type = "payment.completed" if completed else "payment.failed"
    return [
        {
            "routing_key": event_type,
            "body": {
                "event_id": str(uuid4()),
                "event_type": event_type,
                "order_id": event["order_id"],
                "sku": event["sku"],
                "quantity": event["quantity"],
            },
        }
    ]


def main() -> None:
    run_consumer(
        url=os.environ["RABBITMQ_URL"],
        dsn=os.environ["DATABASE_URL"],
        queue="payment.events",
        bindings=("inventory.reserved",),
        consumer_name="payment",
        handler=handle_event,
    )


if __name__ == "__main__":
    main()
