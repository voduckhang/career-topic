"""Hồi quy: lỗi nào thì settle outcome nào (ACCEPTED / FAILED / REJECTED). Không cần DB hay broker."""
import json

import pytest

import notification_service as NS
import order_processor as OP
import Order_publisher as P
from tests.conftest import ACCEPTED, FAILED, REJECTED

REPEAT = 25  # kịch bản có yếu tố ngẫu nhiên (normal, flaky) thì chạy nhiều lần

# Các kịch bản của orders worker hỏng TRƯỚC khi chạm DB nên không cần PostgreSQL
ORDERS_WITHOUT_DB = {
    "transient": {FAILED},
    "permanent": {REJECTED},
    "bad-json": {REJECTED},
    "missing-field": {REJECTED},
    "unknown-event": {REJECTED},
}

# Notification service độc lập: chỉ lỗi của chính nó (notify-down) mới làm nó FAILED
NOTIFICATION = {
    "normal": {ACCEPTED},
    "transient": {ACCEPTED},
    "permanent": {ACCEPTED},
    "flaky": {ACCEPTED},
    "txn-flaky": {ACCEPTED},
    "duplicate": {ACCEPTED},
    "notify-down": {FAILED},
    "bad-json": {REJECTED},
    "missing-field": {REJECTED},
    "unknown-event": {REJECTED},
}


def outcomes(deliver, handler, scenario):
    seen = set()
    for _ in range(REPEAT):
        NS.SENT.clear()
        event, payload, _ = P.build_event(scenario)
        seen.add(deliver(handler, event, payload))
    return seen


@pytest.mark.parametrize("scenario,expected", ORDERS_WITHOUT_DB.items())
def test_orders_worker_outcome(deliver, scenario, expected):
    assert outcomes(deliver, OP.handle, scenario) == expected


@pytest.mark.parametrize("scenario,expected", NOTIFICATION.items())
def test_notification_worker_outcome(deliver, scenario, expected):
    assert outcomes(deliver, NS.handle, scenario) == expected


def test_fanout_isolation_matrix(deliver):
    """Cùng một payload: lỗi của service này không được lan sang service kia."""
    event, payload, _ = P.build_event("transient")  # chỉ orders-worker đọc _simulate
    assert deliver(OP.handle, event, payload) == FAILED
    assert deliver(NS.handle, event, payload) == ACCEPTED

    event, payload, _ = P.build_event("notify-down")  # chỉ notification đọc _simulate_notify
    assert deliver(NS.handle, event, payload) == FAILED


def test_bad_data_is_rejected_by_both_services(deliver):
    """Dữ liệu hỏng thì mỗi service tự từ chối: cả hai DMQ cùng có message."""
    for scenario in ("bad-json", "missing-field", "unknown-event"):
        event, payload, _ = P.build_event(scenario)
        assert deliver(OP.handle, event, payload) == REJECTED
        assert deliver(NS.handle, event, payload) == REJECTED


def test_unexpected_exception_is_treated_as_transient(deliver):
    """Bug chưa lường trước (KeyError...) phải thành FAILED chứ không làm sập worker."""
    def buggy(topic, payload):
        return {}["khong_co"]

    assert deliver(buggy, "created", "{}") == FAILED


def test_non_utf8_payload_is_rejected(deliver):
    assert deliver(OP.handle, "created", None, raw=b"\xff\xfe\x00") == REJECTED


def test_missing_payload_is_rejected(deliver):
    assert deliver(OP.handle, "created", None, raw=None) == REJECTED


def test_redelivered_message_is_logged(deliver, capsys):
    deliver(NS.handle, "created", json.dumps({"order_id": "R1", "customer": "An", "product": "Balo"}),
            redelivered=True)
    assert "[GIAO LẠI]" in capsys.readouterr().out


def test_notification_skips_duplicate_without_resending(deliver, capsys):
    payload = json.dumps({"order_id": "N1", "customer": "An", "product": "Balo"})
    assert deliver(NS.handle, "created", payload) == ACCEPTED
    assert deliver(NS.handle, "created", payload) == ACCEPTED  # bản trùng vẫn ACCEPTED để broker xóa
    out = capsys.readouterr().out
    assert out.count("EMAIL cho") == 1 and "BỎ QUA" in out