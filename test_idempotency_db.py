"""Hồi quy idempotency và xử lý lỗi DB trên PostgreSQL thật (database solace_test)."""
import json
import os
import random
import subprocess
import sys
import threading
import time
from pathlib import Path

import psycopg
import pytest
from psycopg import errors as pgerr

import db
import order_processor as OP
import Order_publisher as P
from errors import PermanentError, TransientError
from tests.conftest import ACCEPTED, FAILED, REJECTED, make_order

pytestmark = pytest.mark.db
ROOT = Path(__file__).resolve().parent.parent


def q(sql, *args):
    with db.connect() as conn:
        return conn.execute(sql, args).fetchall()


def stock(product="Balo"):
    return q("SELECT stock FROM inventory WHERE product = %s", product)[0][0]


def no_trace(order_id):
    """Không được còn dấu vết nửa vời của đơn này ở bất kỳ bảng nào."""
    return (q("SELECT count(*) FROM orders WHERE order_id = %s", order_id)[0][0] == 0
            and q("SELECT count(*) FROM processed_events WHERE order_id = %s", order_id)[0][0] == 0)


# ---------- Luồng cơ bản và bản trùng ----------

def test_created_deducts_stock_once(clean_db, deliver):
    assert deliver(OP.handle, "created", make_order("T1", qty=2)) == ACCEPTED
    assert stock() == db.INITIAL_STOCK - 2


def test_duplicate_created_is_absorbed(clean_db, deliver):
    payload = make_order("T1", qty=2)
    assert deliver(OP.handle, "created", payload) == ACCEPTED
    assert deliver(OP.handle, "created", payload) == ACCEPTED  # vẫn ACCEPTED để broker xóa bản trùng
    assert stock() == db.INITIAL_STOCK - 2                    # kho KHÔNG bị trừ lần hai
    assert q("SELECT dup_count FROM processed_events WHERE order_id='T1' AND event='created'")[0][0] == 1


def test_cancel_restores_stock_exactly_once(clean_db, deliver):
    deliver(OP.handle, "created", make_order("T1", qty=2))
    cancel = json.dumps({"order_id": "T1"})
    assert deliver(OP.handle, "cancelled", cancel) == ACCEPTED
    assert stock() == db.INITIAL_STOCK
    assert deliver(OP.handle, "cancelled", cancel) == ACCEPTED  # bản trùng
    assert stock() == db.INITIAL_STOCK                          # không hoàn kho lần hai


def test_cancel_of_unknown_order_is_noop(clean_db, deliver):
    assert deliver(OP.handle, "cancelled", json.dumps({"order_id": "GHOST"})) == ACCEPTED
    assert stock() == db.INITIAL_STOCK


def test_same_order_different_events_are_independent(clean_db, deliver):
    """Khóa idempotency là (order_id, event): created và cancelled của cùng đơn không chặn nhau."""
    deliver(OP.handle, "created", make_order("T1", qty=3))
    deliver(OP.handle, "cancelled", json.dumps({"order_id": "T1"}))
    assert q("SELECT count(*) FROM processed_events WHERE order_id = 'T1'")[0][0] == 2


def test_eight_processes_same_event_applied_once(clean_db):
    """Nhiều TIẾN TRÌNH (như nhiều worker thật) cùng nhận một sự kiện: chỉ một bên được ghi."""
    code = ("import sys, types, order_processor as OP;"
            "OP.time = types.SimpleNamespace(sleep=lambda s: None);"
            "OP.handle('shop/orders/created', sys.argv[1])")
    payload = make_order("MP1", qty=3)
    env = {**os.environ, "DATABASE_URL": os.environ["DATABASE_URL"]}
    procs = [subprocess.Popen([sys.executable, "-c", code, payload], cwd=ROOT, env=env,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(8)]
    results = [p.communicate() for p in procs]
    assert all(p.returncode == 0 for p in procs), [r[1] for r in results]
    assert stock() == db.INITIAL_STOCK - 3
    assert sum("XỬ LÝ đơn" in out for out, _ in results) == 1
    assert sum("bản trùng" in out for out, _ in results) == 7
    assert q("SELECT count(*), sum(dup_count) FROM processed_events") == [(1, 7)]


# ---------- Lỗi giữa transaction: ROLLBACK, không để lại nửa vời ----------

def test_failure_inside_transaction_rolls_back_everything(clean_db, deliver, monkeypatch):
    monkeypatch.setattr(random, "random", lambda: 0.0)  # luôn rơi vào nhánh lỗi của txn-flaky
    assert deliver(OP.handle, "created", make_order("TX1", qty=1, _simulate="txn-flaky")) == FAILED
    assert no_trace("TX1")
    assert stock() == db.INITIAL_STOCK


def test_redelivery_after_rollback_succeeds(clean_db, deliver, monkeypatch):
    payload = make_order("TX1", qty=1, _simulate="txn-flaky")
    monkeypatch.setattr(random, "random", lambda: 0.0)
    assert deliver(OP.handle, "created", payload) == FAILED
    monkeypatch.setattr(random, "random", lambda: 0.99)  # lần giao lại thì không lỗi
    assert deliver(OP.handle, "created", payload) == ACCEPTED
    assert stock() == db.INITIAL_STOCK - 1  # đúng một lần dù đã thử hai lần


def test_flaky_stream_keeps_inventory_invariant(clean_db, deliver):
    random.seed(7)
    for i in range(12):
        payload = make_order(f"TX{i}", qty=1, _simulate="txn-flaky")
        while deliver(OP.handle, "created", payload) != ACCEPTED:
            assert no_trace(f"TX{i}")  # mỗi lần ROLLBACK phải sạch
    with db.connect() as conn:
        assert db.inventory_mismatches(db.inventory_rows(conn)) == []
        assert db.orphan_orders(conn) == []
    assert q("SELECT count(*) FROM orders")[0][0] == 12


# ---------- Dữ liệu xấu: REJECTED và không để lại dấu vết ----------

BAD_ORDERS = {
    "sản phẩm lạ (khóa ngoại)": dict(product="Xe hơi"),
    "vượt tồn kho (CHECK stock >= 0)": dict(qty=db.INITIAL_STOCK + 1),
    "quantity = 0": dict(qty=0),
    "quantity âm": dict(qty=-1),
    "quantity là chuỗi": dict(qty="3"),
    "quantity là true": dict(qty=True),
    "total là chuỗi": dict(total="abc"),
}


@pytest.mark.parametrize("name", BAD_ORDERS)
def test_bad_order_is_rejected_without_trace(clean_db, deliver, name):
    kwargs = dict(BAD_ORDERS[name])
    payload = json.loads(make_order("BAD1"))
    payload.update({"product": kwargs.get("product", "Balo"),
                    "quantity": kwargs.get("qty", 1),
                    "total": kwargs.get("total", 100)})
    assert deliver(OP.handle, "created", json.dumps(payload)) == REJECTED
    assert no_trace("BAD1")  # claim cũng bị rollback: sửa dữ liệu rồi gửi lại vẫn xử lý được
    assert stock() == db.INITIAL_STOCK


def test_rejected_order_can_be_reprocessed_after_fix(clean_db, deliver):
    """Vì claim bị rollback, message sửa lại đúng (cùng order_id) vẫn được xử lý, không bị coi là trùng."""
    assert deliver(OP.handle, "created", make_order("FIX1", product="Xe hơi")) == REJECTED
    assert deliver(OP.handle, "created", make_order("FIX1", product="Balo")) == ACCEPTED


# ---------- Lỗi hạ tầng DB: FAILED (retry được) ----------

@pytest.mark.slow
def test_locked_row_times_out_as_transient(clean_db, deliver):
    blocker = db.connect()
    blocker.autocommit = False
    blocker.execute("SELECT 1 FROM inventory WHERE product = 'Tai nghe' FOR UPDATE")
    try:
        started = time.time()
        outcome = deliver(OP.handle, "created", make_order("L1", product="Tai nghe"))
        elapsed = time.time() - started
    finally:
        blocker.rollback()
        blocker.close()
    assert outcome == FAILED
    assert 2.5 < elapsed < 8  # lock_timeout 3s: không treo vô hạn
    assert no_trace("L1")
    assert deliver(OP.handle, "created", make_order("L1", product="Tai nghe")) == ACCEPTED  # hết khóa là xong


@pytest.mark.slow
def test_postgres_really_detects_deadlock(clean_db):
    """Bằng chứng Postgres thật sự ném DeadlockDetected trong tình huống hai bên khóa chéo."""
    c1, c2 = db.connect(), db.connect()
    c1.autocommit = c2.autocommit = False
    c1.execute("UPDATE inventory SET stock = stock WHERE product = 'Balo'")
    c2.execute("UPDATE inventory SET stock = stock WHERE product = 'Áo thun'")
    errors = {}

    def side1():
        try:
            c1.execute("UPDATE inventory SET stock = stock WHERE product = 'Áo thun'")
            c1.commit()
        except Exception as e:
            errors[1] = e
            c1.rollback()

    t = threading.Thread(target=side1)
    t.start()
    time.sleep(0.5)
    try:
        c2.execute("UPDATE inventory SET stock = stock WHERE product = 'Balo'")
        c2.commit()
    except Exception as e:
        errors[2] = e
        c2.rollback()
    t.join()
    c1.close()
    c2.close()
    assert any(isinstance(e, pgerr.DeadlockDetected) for e in errors.values()), errors


@pytest.mark.parametrize("exc,expected", [
    (pgerr.DeadlockDetected("x"), TransientError),
    (pgerr.SerializationFailure("x"), TransientError),
    (pgerr.LockNotAvailable("x"), TransientError),
    (pgerr.QueryCanceled("x"), TransientError),
    (pgerr.CheckViolation("x"), PermanentError),
    (pgerr.ForeignKeyViolation("x"), PermanentError),
    (pgerr.InvalidTextRepresentation("x"), PermanentError),
])
def test_postgres_errors_are_classified(clean_db, exc, expected):
    with pytest.raises(expected):
        with db.transaction():
            raise exc


@pytest.mark.parametrize("exc", [pgerr.DeadlockDetected("x"), pgerr.SerializationFailure("x"),
                                 pgerr.LockNotAvailable("x"), pgerr.QueryCanceled("x")])
def test_lock_conflicts_do_not_drop_a_healthy_connection(clean_db, exc):
    """Deadlock/timeout là lỗi tranh chấp chứ không phải mất kết nối: không được vứt kết nối đang khỏe
    và log không được nói dối là 'mất kết nối'."""
    conn = db.get_conn()
    with pytest.raises(TransientError) as caught:
        with db.transaction():
            raise exc
    assert "Mất kết nối" not in str(caught.value)
    assert db.get_conn() is conn


def test_real_connection_loss_resets_the_connection(clean_db):
    db.get_conn()
    with pytest.raises(TransientError) as caught:
        with db.transaction():
            raise psycopg.OperationalError("server closed the connection")
    assert "Mất kết nối DB" in str(caught.value)
    assert db._conn is None  # lần giao lại sẽ mở kết nối mới


def test_database_down_is_transient_then_recovers(clean_db, deliver, monkeypatch):
    good_url = db.DATABASE_URL
    monkeypatch.setattr(db, "DATABASE_URL", "postgresql://solace:solace@localhost:5999/solace_test")
    db.reset_conn()
    assert deliver(OP.handle, "created", make_order("C1")) == FAILED  # DB không với tới
    monkeypatch.setattr(db, "DATABASE_URL", good_url)
    assert deliver(OP.handle, "created", make_order("C1")) == ACCEPTED  # DB trở lại: giao lại thành công


# ---------- Các kịch bản của publisher đi qua orders worker ----------

def test_duplicate_scenario_deducts_once(clean_db, deliver):
    event, payload, _ = P.build_event("duplicate")
    for _ in range(2):  # main() gửi đơn này 2 lần
        assert deliver(OP.handle, event, payload) == ACCEPTED
    qty = json.loads(payload)["quantity"]
    product = json.loads(payload)["product"]
    assert stock(product) == db.INITIAL_STOCK - qty


def test_normal_stream_with_cancels_keeps_invariant(clean_db, deliver):
    random.seed(1)
    for _ in range(40):
        event, payload, _ = P.build_event("normal")
        assert deliver(OP.handle, event, payload) == ACCEPTED
    with db.connect() as conn:
        assert db.inventory_mismatches(db.inventory_rows(conn)) == []