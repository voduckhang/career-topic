"""Fixture dùng chung cho bộ test hồi quy.

AN TOÀN: test dùng database riêng (mặc định solace_test), KHÔNG đụng vào dữ liệu demo solace_demo.
Đổi bằng biến môi trường TEST_DATABASE_URL (tên database bắt buộc chứa chữ "test").
"""
import os
import types

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", "postgresql://solace:solace@localhost:5432/solace_test")
# Phải đặt TRƯỚC khi bất kỳ module nào import db (db đọc DATABASE_URL lúc import)
os.environ["DATABASE_URL"] = TEST_DATABASE_URL

from solace.messaging.config.message_acknowledgement_configuration import Outcome  # noqa: E402

ACCEPTED, FAILED, REJECTED = Outcome.ACCEPTED, Outcome.FAILED, Outcome.REJECTED

# Tiến trình con in tiếng Việt ra pipe: trên Windows stdout bị chuyển hướng dùng bảng mã hệ thống
# (không phải UTF-8) nên sẽ lỗi UnicodeEncodeError. Mọi test chạy subprocess phải dùng env này.
UTF8_ENV = {"PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}


class FakeMsg:
    """Message giả đủ dùng cho common.process_message."""

    def __init__(self, topic, payload, redelivered=False, raw=None):
        self.topic, self.payload, self.redelivered, self.raw = topic, payload, redelivered, raw

    def get_destination_name(self):
        return self.topic

    def get_payload_as_string(self):
        return self.payload

    def get_payload_as_bytes(self):
        return self.raw if self.raw is not None else (
            None if self.payload is None else self.payload.encode())

    def is_redelivered(self):
        return self.redelivered


class FakeReceiver:
    outcome = None

    def settle(self, msg, outcome):
        self.outcome = outcome


@pytest.fixture(autouse=True)
def isolated_state(monkeypatch, tmp_path):
    """Mỗi test: bỏ giả lập chờ 1.5s, xóa trạng thái trong RAM, trỏ sổ cái vào thư mục tạm."""
    import ledger
    import notification_service
    import order_processor

    stub = types.SimpleNamespace(sleep=lambda s: None)
    monkeypatch.setattr(order_processor, "time", stub)
    monkeypatch.setattr(notification_service, "time", stub)
    notification_service.SENT.clear()
    monkeypatch.setattr(ledger, "LEDGER_PATH", str(tmp_path / "ledger.jsonl"))


@pytest.fixture
def deliver():
    """deliver(handler, event, payload) -> Outcome mà worker sẽ settle với broker."""
    import common

    def _deliver(handler, event, payload, redelivered=False, raw=None):
        receiver = FakeReceiver()
        common.process_message(receiver, FakeMsg(f"shop/orders/{event}", payload, redelivered, raw), handler)
        return receiver.outcome

    return _deliver


@pytest.fixture(scope="session")
def pg_ready():
    import psycopg
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    info = conninfo_to_dict(TEST_DATABASE_URL)
    dbname = info["dbname"]
    if "test" not in dbname:
        pytest.fail(f"Từ chối chạy test trên database '{dbname}': tên phải chứa 'test' để khỏi xóa nhầm dữ liệu thật")
    try:
        admin = psycopg.connect(make_conninfo(**{**info, "dbname": "postgres"}),
                                autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as e:
        pytest.skip(f"Không kết nối được PostgreSQL ({e}); bỏ qua các test cần DB")
    with admin:
        if not admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)).fetchone():
            admin.execute(f'CREATE DATABASE "{dbname}"')


@pytest.fixture
def clean_db(pg_ready):
    """Database test sạch: schema có sẵn, kho đầy, không có đơn nào."""
    import db

    db.reset_conn()
    db.reset_data()
    yield db
    db.reset_conn()


def make_order(order_id, product="Balo", qty=2, **extra):
    import json
    return json.dumps({"order_id": order_id, "customer": "An", "product": product,
                       "quantity": qty, "total": 800_000, **extra}, ensure_ascii=False)