"""Hồi quy cho Order_publisher.py: nội dung từng kịch bản, thuộc tính message và sổ cái."""
import ctypes
import json
import types

import pytest
import solace
from solace.messaging.publisher._impl._outbound_message import _OutboundMessageBuilder

import ledger
import Order_publisher as P


class FakeService:
    def message_builder(self):
        return _OutboundMessageBuilder()

    def create_persistent_message_publisher_builder(self):
        return types.SimpleNamespace(build=lambda: self.publisher)

    def disconnect(self):
        pass


class FakePublisher:
    """Publisher giả: ghi lại thứ nhận được và nội dung sổ cái TẠI THỜI ĐIỂM publish."""

    def __init__(self):
        self.sent = []
        self.ledger_at_publish = []

    def start(self):
        pass

    def set_message_publish_receipt_listener(self, listener):
        self.listener = listener

    def publish(self, message, topic, user_context=None):
        self.sent.append((message, topic.get_name(), user_context))
        entries, _ = ledger.load()
        self.ledger_at_publish.append(user_context and
                                      ledger.message_key(user_context["order_id"], user_context["event"]) in entries)

    def terminate(self):
        pass


@pytest.fixture
def run_main(monkeypatch):
    def _run(count, scenario="normal", ttl_ms=None, dmq_eligible=True):
        service, publisher = FakeService(), FakePublisher()
        service.publisher = publisher
        monkeypatch.setattr(P, "build_service", lambda: service)
        monkeypatch.setattr(P, "time", types.SimpleNamespace(sleep=lambda s: None))
        P.main(count, scenario, ttl_ms, dmq_eligible)
        return publisher
    return _run


def readback(message):
    """Đọc lại TTL và cờ DMQ thật trên message (qua thư viện lõi); bỏ qua nếu API nội bộ đổi."""
    try:
        pointer = message._solace_message._msg_p
        ttl = ctypes.c_int64(-1)
        solace.CORE_LIB.solClient_msg_getTimeToLive(pointer, ctypes.byref(ttl))
        return ttl.value, bool(solace.CORE_LIB.solClient_msg_isDMQEligible(pointer))
    except AttributeError:
        pytest.skip("API nội bộ của thư viện Solace đã đổi, không đọc lại được thuộc tính message")


# ---------- build_event ----------

def test_normal_orders_are_valid_and_clean():
    seen_events = set()
    for _ in range(60):
        event, payload, order_id = P.build_event("normal")
        order = json.loads(payload)
        seen_events.add(event)
        assert {"order_id", "customer", "product", "quantity", "total"} <= order.keys()
        assert order["order_id"] == order_id
        assert not any(k.startswith("_simulate") for k in order)
    assert seen_events == {"created", "cancelled"}


@pytest.mark.parametrize("scenario", ["transient", "flaky", "permanent", "txn-flaky"])
def test_orders_worker_failure_scenarios_set_simulate(scenario):
    event, payload, _ = P.build_event(scenario)
    order = json.loads(payload)
    assert event == "created" and order["_simulate"] == scenario and "_simulate_notify" not in order


def test_notify_down_only_targets_notification_service():
    _, payload, _ = P.build_event("notify-down")
    order = json.loads(payload)
    assert order["_simulate_notify"] == "transient" and "_simulate" not in order


def test_bad_json_is_unparsable_but_still_unique_per_order():
    payloads = set()
    for _ in range(5):
        _, payload, order_id = P.build_event("bad-json")
        with pytest.raises(json.JSONDecodeError):
            json.loads(payload)
        assert order_id in payload
        payloads.add(payload)
    assert len(payloads) == 5


def test_missing_field_has_no_product():
    event, payload, _ = P.build_event("missing-field")
    assert event == "created" and "product" not in json.loads(payload)


def test_unknown_event_uses_unsupported_topic_suffix():
    event, payload, _ = P.build_event("unknown-event")
    assert event == "refunded" and "product" in json.loads(payload)


def test_unknown_scenario_is_rejected():
    with pytest.raises(ValueError):
        P.build_event("khong-co")


# ---------- build_message ----------

@pytest.mark.parametrize("ttl,dmq,expected", [(None, True, (0, True)), (5000, True, (5000, True)),
                                              (None, False, (0, False)), (7000, False, (7000, False))])
def test_ttl_and_dmq_flag_reach_the_message(ttl, dmq, expected):
    message = P.build_message(FakeService(), "{}", ttl, dmq)
    assert readback(message) == expected


def test_application_message_id_is_attached():
    message = P.build_message(FakeService(), "{}", None, True, message_id="ab12cd34:created")
    assert message.get_application_message_id() == "ab12cd34:created"


# ---------- main() và sổ cái ----------

def test_duplicate_scenario_sends_each_order_twice_with_same_identity(run_main):
    publisher = run_main(3, "duplicate")
    keys = [m.get_application_message_id() for m, _, _ in publisher.sent]
    assert len(keys) == 6 and len(set(keys)) == 3
    assert all(keys.count(k) == 2 for k in keys)
    entries, bad = ledger.load()
    assert bad == 0 and sorted(e.attempts for e in entries.values()) == [2, 2, 2]


def test_every_send_is_recorded_in_the_ledger_before_publishing(run_main):
    publisher = run_main(4, "normal")
    assert len(publisher.sent) == 4 and all(publisher.ledger_at_publish)


def test_ledger_captures_ttl_and_dmq_flag(run_main):
    run_main(2, "permanent", ttl_ms=5000, dmq_eligible=False)
    entries, _ = ledger.load()
    assert all(e.ttl_ms == 5000 and e.dmq_eligible is False and e.scenarios == {"permanent"}
               for e in entries.values())


def test_receipts_flow_into_the_ledger(run_main):
    publisher = run_main(2, "normal")
    contexts = [c for _, _, c in publisher.sent]
    ok = types.SimpleNamespace(exception=None, user_context=contexts[0])
    bad = types.SimpleNamespace(exception=RuntimeError("broker đầy"), user_context=contexts[1])
    publisher.listener.on_publish_receipt(ok)
    publisher.listener.on_publish_receipt(bad)
    entries, _ = ledger.load()
    first = entries[ledger.message_key(contexts[0]["order_id"], contexts[0]["event"])]
    second = entries[ledger.message_key(contexts[1]["order_id"], contexts[1]["event"])]
    assert (first.acked, first.nacked) == (1, 0) and (second.acked, second.nacked) == (0, 1)