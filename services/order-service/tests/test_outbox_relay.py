from app.outbox_relay import OUTBOX_PUBLISHED, publish_batch


class FakeRepo:
    def __init__(self, rows):
        self.rows = rows
        self.marked = []

    def fetch_unpublished(self, limit=50):
        return self.rows

    def mark_published(self, ids):
        self.marked.extend(ids)


class FakeChannel:
    def __init__(self):
        self.published = []

    def basic_publish(self, exchange, routing_key, body, properties):
        self.published.append((exchange, routing_key, body))


def test_publish_batch_empty_returns_zero():
    repo, ch = FakeRepo([]), FakeChannel()
    assert publish_batch(repo, ch) == 0
    assert ch.published == []
    assert repo.marked == []


def test_publish_batch_publishes_and_marks():
    rows = [(1, "order.created", '{"a": 1}'), (2, "order.created", '{"b": 2}')]
    repo, ch = FakeRepo(rows), FakeChannel()
    before = OUTBOX_PUBLISHED._value.get()
    assert publish_batch(repo, ch) == 2
    assert ch.published == [
        ("orders", "order.created", b'{"a": 1}'),
        ("orders", "order.created", b'{"b": 2}'),
    ]
    assert repo.marked == [1, 2]
    assert OUTBOX_PUBLISHED._value.get() - before == 2.0
