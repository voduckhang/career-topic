"""Hồi quy cho common.run_consumer: cách bind queue và xử lý lỗi khởi động."""
import pytest
from solace.messaging.config.missing_resources_creation_configuration import MissingResourcesCreationStrategy as Strategy
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError

import common
from tests.conftest import FAILED, REJECTED


class Recorder:
    """Dựng chuỗi builder giả và ghi lại những gì worker đã yêu cầu."""

    def __init__(self, fail_start=False, messages=()):
        self.calls, self.fail_start, self.messages = {}, fail_start, list(messages)
        self.disconnected = self.terminated = False

    # builder
    def create_persistent_message_receiver_builder(self):
        return self

    def with_missing_resources_creation_strategy(self, strategy):
        self.calls["strategy"] = strategy
        return self

    def with_message_client_acknowledgement(self):
        self.calls["client_ack"] = True
        return self

    def with_required_message_outcome_support(self, *outcomes):
        self.calls["outcomes"] = set(outcomes)
        return self

    def build(self, queue):
        self.calls["queue"] = queue.get_name()
        return self

    # receiver
    def start(self):
        if self.fail_start:
            raise PubSubPlusClientError("queue not found")

    def add_subscription(self, topic):
        pass

    def receive_message(self, timeout=0):
        if self.messages:
            return self.messages.pop(0)
        raise KeyboardInterrupt

    def settle(self, msg, outcome):
        self.calls.setdefault("settled", []).append(outcome)

    def terminate(self):
        self.terminated = True

    def disconnect(self):
        self.disconnected = True


@pytest.fixture
def broker(monkeypatch):
    def _make(**kw):
        rec = Recorder(**kw)
        monkeypatch.setattr(common, "build_service", lambda: rec)
        return rec
    return _make


def test_worker_binds_to_existing_queue_instead_of_provisioning(broker):
    rec = broker()
    common.run_consumer("notifications-queue", [], lambda t, p: None, exclusive=False)
    # DO_NOT_CREATE: tránh ENDPOINT_PROPERTY_MISMATCH khi queue do setup_queues.py (SEMP) tạo
    assert rec.calls["strategy"] == Strategy.DO_NOT_CREATE
    assert rec.calls["queue"] == "notifications-queue"


def test_worker_requests_settlement_outcomes_and_client_ack(broker):
    rec = broker()
    common.run_consumer("orders-queue", [], lambda t, p: None, exclusive=False)
    assert FAILED in rec.calls["outcomes"] and REJECTED in rec.calls["outcomes"]
    assert rec.calls["client_ack"] is True


def test_worker_cleans_up_on_ctrl_c(broker):
    rec = broker()
    common.run_consumer("orders-queue", [], lambda t, p: None, exclusive=False)
    assert rec.terminated and rec.disconnected


def test_missing_queue_exits_cleanly_with_hint(broker):
    rec = broker(fail_start=True)
    with pytest.raises(SystemExit) as exc:
        common.run_consumer("orders-queue", [], lambda t, p: None, exclusive=False)
    assert "setup_queues.py" in str(exc.value) and rec.disconnected


def test_message_loop_settles_what_the_handler_decides(broker):
    from tests.conftest import FakeMsg
    from errors import PermanentError

    def handler(topic, payload):
        raise PermanentError("hỏng")

    rec = broker(messages=[FakeMsg("shop/orders/created", "{}")])
    common.run_consumer("orders-queue", [], handler, exclusive=False)
    assert rec.calls["settled"] == [REJECTED]