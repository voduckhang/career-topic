"""Hồi quy cho sổ cái (ledger.py) và đối chiếu (reconcile.py)."""
import json
import time
import types
from collections import Counter

import pytest

import db
import ledger
import order_processor as OP
import Order_publisher as P
import reconcile
from reconcile import (IN_DMQ, IN_QUEUE, MISSING, PROCESSED, PUBLISH_NACK, UNVERIFIED,
                       BrokerSnapshot, classify)
from tests.conftest import FAILED, REJECTED, make_order


def entry(order_id="a1", event="created", attempts=1, acked=1, nacked=0, dmq=True, ttl=None):
    return ledger.Entry(ledger.message_key(order_id, event), order_id, event,
                        attempts, acked, nacked, dmq, ttl)


def snap(dmq=(), queue=(), **kw):
    return BrokerSnapshot(Counter(dmq), Counter(queue), **kw)


def classify_one(e, processed=None, broker=None):
    rep = classify({e.key: e}, processed or {}, broker or snap())
    return rep.status[e.key], rep


# ====================== ledger.py ======================

def test_ledger_aggregates_attempts_and_receipts():
    ledger.record_sent("a1", "created", 0, "duplicate", True, None)
    ledger.record_sent("a1", "created", 1, "duplicate", True, None)
    ledger.record_receipt("a1", "created", 0, True)
    ledger.record_receipt("a1", "created", 1, False, "broker từ chối")
    entries, bad = ledger.load()
    e = entries["a1:created"]
    assert (e.attempts, e.acked, e.nacked, e.unconfirmed, bad) == (2, 1, 1, 0, 0)


def test_ledger_dmq_flag_is_sticky_false():
    ledger.record_sent("a1", "created", 0, "permanent", True, None)
    ledger.record_sent("a1", "created", 1, "permanent", False, None)
    assert ledger.load()[0]["a1:created"].dmq_eligible is False


def test_ledger_tolerates_truncated_and_unknown_lines_without_phantom_events():
    ledger.record_sent("a1", "created", 0, "normal", True, None)
    with open(ledger.LEDGER_PATH, "a", encoding="utf-8") as f:
        f.write('{"type": "sent", "order_id": "a2", "ev')                   # ghi dở vì tiến trình chết
        f.write('\n{"type": "la-la", "order_id": "x", "event": "y"}\n')    # loại dòng không biết
        f.write("\n")                                                       # dòng trống: bỏ qua, không tính là hỏng
    entries, bad = ledger.load()
    assert list(entries) == ["a1:created"]  # tuyệt đối không có "sự kiện ma" x:y hay a2:created
    assert bad == 2


def test_orphan_receipt_is_counted_bad_and_creates_no_event():
    ledger.record_receipt("lost", "created", 0, True)  # biên nhận mà không có dòng "sent"
    entries, bad = ledger.load()
    assert entries == {} and bad == 1


def test_ledger_missing_file_is_empty():
    assert ledger.load("/khong/ton/tai.jsonl") == ({}, 0)


def test_ledger_reset_removes_file():
    ledger.record_sent("a1", "created", 0, "normal", True, None)
    ledger.reset()
    assert ledger.load() == ({}, 0)


# ====================== classify (hàm thuần) ======================

def test_processed():
    st, _ = classify_one(entry(), processed={"a1:created": 0})
    assert st == PROCESSED


def test_processed_needs_no_broker():
    """Đã có dấu trong DB thì đủ kết luận, kể cả khi không đọc được broker."""
    st, _ = classify_one(entry(), processed={"a1:created": 0}, broker=BrokerSnapshot(None, None, error="x"))
    assert st == PROCESSED


def test_in_dmq():
    assert classify_one(entry(), broker=snap(dmq=["a1:created"]))[0] == IN_DMQ


def test_in_queue():
    assert classify_one(entry(), broker=snap(queue=["a1:created"]))[0] == IN_QUEUE


def test_dmq_wins_over_queue():
    assert classify_one(entry(), broker=snap(dmq=["a1:created"], queue=["a1:created"]))[0] == IN_DMQ


def test_processed_wins_over_queue():
    st, _ = classify_one(entry(), processed={"a1:created": 1}, broker=snap(queue=["a1:created"]))
    assert st == PROCESSED


def test_missing_when_nowhere():
    st, rep = classify_one(entry())
    assert st == MISSING and "điều tra" in rep.notes["a1:created"]


def test_missing_explains_dmq_ineligible():
    st, rep = classify_one(entry(dmq=False))
    assert st == MISSING and "DMQ Eligible = false" in rep.notes["a1:created"]


def test_missing_explains_no_receipt():
    st, rep = classify_one(entry(acked=0))
    assert st == MISSING and "biên nhận" in rep.notes["a1:created"]


def test_missing_mentions_ttl():
    st, rep = classify_one(entry(ttl=5000))
    assert st == MISSING and "TTL 5000" in rep.notes["a1:created"]


def test_publish_nack():
    st, _ = classify_one(entry(acked=0, nacked=1))
    assert st == PUBLISH_NACK


def test_partial_nack_is_judged_by_where_the_acked_copy_went():
    e = entry(attempts=2, acked=1, nacked=1)
    assert classify_one(e, processed={e.key: 0})[0] == PROCESSED
    assert classify_one(e)[0] == MISSING


def test_unverified_when_broker_unreadable():
    st, _ = classify_one(entry(), broker=BrokerSnapshot(None, None, error="timeout"))
    assert st == UNVERIFIED


def test_stale_dmq_copy_of_processed_event_is_flagged():
    _, rep = classify_one(entry(), processed={"a1:created": 0}, broker=snap(dmq=["a1:created"]))
    assert rep.dmq_stale == ["a1:created"]


def test_event_in_db_but_not_in_ledger_is_unexpected():
    rep = classify({}, {"zz:created": 0}, snap())
    assert rep.unexpected == ["zz:created"]


def test_foreign_broker_messages_are_counted():
    rep = classify({entry().key: entry()}, {}, snap(dmq=["old1:created", "old1:created"], queue=["old2:created"]))
    assert (rep.foreign_dmq, rep.foreign_queue) == (2, 1)


def test_duplicate_counters():
    e = entry(attempts=3, acked=3)
    rep = classify({e.key: e}, {e.key: 2}, snap())
    assert (rep.dup_sent, rep.dup_absorbed) == (2, 2)


# ====================== reconcile đầu-cuối (DB thật + broker giả) ======================

class FakeBroker:
    """Broker giả: trả lần lượt các snapshot; hook(n) chạy trước lần đọc thứ n (để mô phỏng worker làm việc)."""

    def __init__(self, *snapshots, hooks=None):
        self.snapshots, self.hooks, self.reads = list(snapshots), hooks or {}, 0

    def read(self):
        self.reads += 1
        if self.reads in self.hooks:
            self.hooks[self.reads]()
        return self.snapshots[min(self.reads, len(self.snapshots)) - 1]


def simulate_run(scenario, count, deliver, dmq_eligible=True):
    """Mô phỏng publisher + worker + broker. Trả về Counter các message mà DMQ của broker sẽ giữ."""
    dmq = Counter()
    for _ in range(count):
        event, payload, order_id = P.build_event(scenario)
        key = ledger.message_key(order_id, event)
        for attempt in range(2 if scenario == "duplicate" else 1):
            ledger.record_sent(order_id, event, attempt, scenario, dmq_eligible, None)
            ledger.record_receipt(order_id, event, attempt, True)
            outcome = deliver(OP.handle, event, payload)
            if outcome in (REJECTED, FAILED) and dmq_eligible:  # FAILED mãi thì hết lượt giao lại -> DMQ
                dmq[key] += 1
    return dmq


def run_reconcile(broker, capsys, *argv):
    code = reconcile.main(list(argv), broker=broker)
    return code, capsys.readouterr().out


@pytest.mark.db
def test_clean_run_reconciles_to_zero_findings(clean_db, deliver, capsys):
    simulate_run("normal", 15, deliver)
    code, out = run_reconcile(FakeBroker(snap()), capsys)
    assert code == 0 and "KHỚP" in out
    assert "!!" not in out  # không có mục cảnh báo nghiêm trọng nào


@pytest.mark.db
@pytest.mark.parametrize("scenario", ["permanent", "transient", "bad-json", "missing-field", "unknown-event"])
def test_bad_events_land_in_dmq_and_that_is_not_data_loss(clean_db, deliver, capsys, scenario):
    dmq = simulate_run(scenario, 3, deliver)
    code, out = run_reconcile(FakeBroker(snap(dmq=dmq)), capsys)
    assert code == 0 and "IN_DMQ" in out
    entries, _ = ledger.load()
    assert classify(entries, {}, snap(dmq=dmq)).counts()[IN_DMQ] == 3


@pytest.mark.db
def test_silent_loss_without_dmq_flag_is_caught(clean_db, deliver, capsys):
    """Bài học --no-dmq: worker REJECTED nhưng message bị broker xóa, reconcile phải bắt được."""
    dmq = simulate_run("permanent", 2, deliver, dmq_eligible=False)
    assert not dmq  # broker không giữ gì cả
    code, out = run_reconcile(FakeBroker(snap()), capsys)
    assert code == 1 and "MẤT DỮ LIỆU IM LẶNG" in out and "DMQ Eligible = false" in out


@pytest.mark.db
def test_duplicate_scenario_reports_absorbed_duplicates(clean_db, deliver, capsys):
    simulate_run("duplicate", 4, deliver)
    code, out = run_reconcile(FakeBroker(snap()), capsys)
    assert code == 0
    assert "4 bản trùng" in out and "4 bản trùng bị chặn" in out


@pytest.mark.db
def test_publish_nack_is_reported(clean_db, capsys):
    ledger.record_sent("n1", "created", 0, "normal", True, None)
    ledger.record_receipt("n1", "created", 0, False, "broker đầy")
    code, out = run_reconcile(FakeBroker(snap()), capsys)
    assert code == 1 and "PUBLISH_NACK" in out and "n1:created" in out


@pytest.mark.db
def test_unreadable_broker_gives_unverified_not_false_alarm(clean_db, deliver, capsys):
    ledger.record_sent("u1", "created", 0, "normal", True, None)
    ledger.record_receipt("u1", "created", 0, True)
    code, out = run_reconcile(FakeBroker(BrokerSnapshot(None, None, error="mất kết nối")), capsys)
    assert code == 2 and "UNVERIFIED" in out and "MẤT DỮ LIỆU IM LẶNG:" not in out


@pytest.mark.db
def test_no_broker_flag(clean_db, deliver, capsys):
    simulate_run("normal", 5, deliver)
    code, out = run_reconcile(None, capsys, "--no-broker")
    assert code == 0 and "PROCESSED" in out


@pytest.mark.db
def test_event_in_db_but_not_in_ledger_is_reported_as_extra(clean_db, deliver, capsys):
    simulate_run("normal", 2, deliver)
    deliver(OP.handle, "created", make_order("ZZ99"))  # xử lý một đơn mà publisher không hề ghi sổ
    code, out = run_reconcile(FakeBroker(snap()), capsys)
    assert code == 0 and "ZZ99:created" in out and "thừa" in out


@pytest.mark.db
def test_orphan_order_is_a_hard_failure(clean_db, deliver, capsys):
    simulate_run("normal", 2, deliver)
    with db.connect() as conn:  # ghi thẳng vào orders, lách qua cơ chế claim
        conn.execute("INSERT INTO orders (order_id, product, quantity, status) VALUES ('GHOST', 'Balo', 1, 'created')")
    code, out = run_reconcile(FakeBroker(snap()), capsys)
    assert code == 1 and "Đơn mồ côi" in out and "GHOST" in out


@pytest.mark.db
def test_inventory_tampering_is_a_hard_failure(clean_db, deliver, capsys):
    simulate_run("normal", 3, deliver)
    with db.connect() as conn:
        conn.execute("UPDATE inventory SET stock = stock - 5 WHERE product = 'Balo'")
    code, out = run_reconcile(FakeBroker(snap()), capsys)
    assert code == 1 and "Tồn kho LỆCH" in out


@pytest.mark.db
def test_in_flight_messages_are_not_failures(clean_db, capsys):
    ledger.record_sent("q1", "created", 0, "normal", True, None)
    ledger.record_receipt("q1", "created", 0, True)
    code, out = run_reconcile(FakeBroker(snap(queue=["q1:created"])), capsys)
    assert code == 0 and "chưa có sai lệch" in out and "IN_QUEUE" in out


@pytest.mark.db
def test_wait_polls_until_queue_drains(clean_db, deliver, capsys, monkeypatch):
    monkeypatch.setattr(reconcile, "time", types.SimpleNamespace(time=time.time, sleep=lambda s: None))
    event, payload, order_id = P.build_event("normal")
    ledger.record_sent(order_id, event, 0, "normal", True, None)
    ledger.record_receipt(order_id, event, 0, True)
    key = ledger.message_key(order_id, event)
    broker = FakeBroker(snap(queue=[key]), snap(),
                        hooks={2: lambda: deliver(OP.handle, event, payload)})  # worker xử lý giữa hai lần đọc
    code, out = run_reconcile(broker, capsys, "--wait", "30")
    assert broker.reads == 2 and code == 0 and "KHỚP" in out


def test_empty_ledger(capsys, pg_ready):
    code, out = run_reconcile(FakeBroker(snap()), capsys)
    assert code == 0 and "trống" in out


# ====================== browse_app_ids (Queue Browser giả) ======================

class FakeInbound:
    def __init__(self, app_id):
        self.app_id = app_id

    def get_application_message_id(self):
        return self.app_id


class FakeBrowser:
    def __init__(self, messages):
        self.messages, self.started, self.terminated = list(messages), False, False

    def start(self):
        self.started = True

    def receive_message(self, timeout=None):
        return self.messages.pop(0) if self.messages else None

    def terminate(self):
        self.terminated = True


class FakeService:
    def __init__(self, browser):
        self.browser = browser

    def create_message_queue_browser_builder(self):
        return types.SimpleNamespace(build=lambda queue: self.browser)


def test_browse_counts_ids_and_unattributed_and_cleans_up():
    browser = FakeBrowser([FakeInbound("a:created"), FakeInbound("a:created"), FakeInbound("b:cancelled"),
                           FakeInbound(None), FakeInbound("")])
    ids, unattributed = reconcile.browse_app_ids(FakeService(browser), "orders-dmq", exclusive=True)
    assert ids == Counter({"a:created": 2, "b:cancelled": 1}) and unattributed == 2
    assert browser.started and browser.terminated


def test_browse_respects_max_msgs():
    browser = FakeBrowser([FakeInbound(f"{i}:created") for i in range(50)])
    ids, _ = reconcile.browse_app_ids(FakeService(browser), "orders-queue", exclusive=False, max_msgs=10)
    assert sum(ids.values()) == 10


def test_solace_broker_failure_becomes_unreadable_snapshot():
    b = reconcile.SolaceBroker("orders-queue", "orders-dmq")
    b.service = types.SimpleNamespace(create_message_queue_browser_builder=lambda: (_ for _ in ()).throw(RuntimeError("no browser")))
    snapshot = b.read()
    assert snapshot.dmq is None and "RuntimeError" in snapshot.error