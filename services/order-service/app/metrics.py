from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

ORDERS_CREATED = Counter("orders_created", "Orders accepted by the API")

REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration",
    ["path"],
)


def render() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
