import os

from .runtime import run_consumer


def handle_event(event: dict) -> list[dict]:
    print(
        f"notification: order {event['order_id']} {event['event_type']}",
        flush=True,
    )
    return []


def main() -> None:
    run_consumer(
        url=os.environ["RABBITMQ_URL"],
        dsn=os.environ["DATABASE_URL"],
        queue="notification.events",
        bindings=("inventory.rejected", "payment.completed", "payment.failed"),
        consumer_name="notification",
        handler=handle_event,
    )


if __name__ == "__main__":
    main()
