import pytest
from app.schemas import Order, OrderRequest
from pydantic import ValidationError


def test_valid_order_request():
    req = OrderRequest(sku="ABC-1", quantity=2)
    assert req.sku == "ABC-1"
    assert req.quantity == 2


def test_rejects_zero_quantity():
    with pytest.raises(ValidationError):
        OrderRequest(sku="ABC-1", quantity=0)


def test_rejects_empty_sku():
    with pytest.raises(ValidationError):
        OrderRequest(sku="", quantity=1)


def test_order_defaults_to_pending():
    order = Order(order_id="11111111-1111-1111-1111-111111111111", sku="ABC-1", quantity=2)
    assert order.status == "pending"
