"""Đối chiếu (reconciliation) ba nguồn: sổ publisher  <->  PostgreSQL  <->  broker.

    python reconcile.py                 # đối chiếu một lần (nên chạy khi worker đã rảnh)
    python reconcile.py --wait 30       # đợi tối đa 30 giây cho message còn trong queue được xử lý hết
    python reconcile.py --no-broker     # chỉ đối chiếu sổ với DB (không đọc broker)

Mỗi sự kiện publisher đã gửi phải kết thúc ở ĐÚNG MỘT trong các trạng thái sau:
  PROCESSED     đã xử lý, có dấu trong processed_events
  IN_DMQ        đang nằm trong DMQ, chờ người xử lý (có chủ đích, không phải mất dữ liệu)
  IN_QUEUE      còn trong orders-queue (đang chờ hoặc đang retry), chưa kết luận được
  PUBLISH_NACK  broker từ chối mọi lần gửi: sự kiện chưa hề vào hệ thống
  MISSING       không ở đâu cả = MẤT DỮ LIỆU IM LẶNG
  UNVERIFIED    chưa xử lý và không đọc được broker nên không biết nó đang ở đâu

Ngoài ra còn các kiểm tra nội bộ DB: đơn mồ côi, bất biến tồn kho; và các điểm lạ như sự kiện có
trong DB mà sổ không có ("thừa"), message đã xử lý nhưng vẫn còn trong DMQ.

Duyệt queue/DMQ bằng Queue Browser (đọc KHÔNG xóa message). SEMP không đọc được nội dung message,
nên reconcile dựa vào application_message_id mà publisher đã gắn (xem ledger.py).

Mã thoát: 0 = khớp | 1 = có sai lệch thật (MISSING, PUBLISH_NACK, đơn mồ côi, tồn kho lệch)
          | 2 = chưa kết luận được (UNVERIFIED)
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from dataclasses import dataclass, field

import db
import ledger

PROCESSED, IN_DMQ, IN_QUEUE = "PROCESSED", "IN_DMQ", "IN_QUEUE"
MISSING, PUBLISH_NACK, UNVERIFIED = "MISSING", "PUBLISH_NACK", "UNVERIFIED"

MAX_DETAIL_LINES = 15


@dataclass
class BrokerSnapshot:
    """Những gì đọc được từ broker. dmq/queue = None nghĩa là KHÔNG đọc được (khác với rỗng)."""
    dmq: Counter | None
    queue: Counter | None
    dmq_unattributed: int = 0     # message không có application_message_id
    queue_unattributed: int = 0
    error: str | None = None


@dataclass
class Report:
    status: dict[str, str] = field(default_factory=dict)   # key -> trạng thái
    notes: dict[str, str] = field(default_factory=dict)    # key -> giải thích
    unexpected: list[str] = field(default_factory=list)    # có trong DB nhưng sổ không có ("thừa")
    dmq_stale: list[str] = field(default_factory=list)     # đã xử lý nhưng vẫn còn bản chết trong DMQ
    foreign_dmq: int = 0     # message trong DMQ mà sổ không biết (thường là sót từ lần chạy trước)
    foreign_queue: int = 0
    dup_sent: int = 0        # số bản trùng publisher đã gửi
    dup_absorbed: int = 0    # số bản trùng DB đã chặn

    def counts(self) -> Counter:
        return Counter(self.status.values())

    def keys_with(self, state: str) -> list[str]:
        return sorted(k for k, v in self.status.items() if v == state)


def _missing_hint(e: ledger.Entry) -> str:
    if not e.dmq_eligible:
        return ("gửi với DMQ Eligible = false: khi bị reject hoặc hết lượt giao lại, "
                "broker XÓA message thay vì chuyển vào DMQ")
    if e.acked == 0:
        return "publisher chưa nhận được biên nhận (ACK): có thể message chưa hề tới broker"
    if e.ttl_ms:
        return f"message có TTL {e.ttl_ms} ms: kiểm tra TTL và respectTtlEnabled của queue"
    return "không còn trong DB, DMQ hay queue và không có lý do hiển nhiên: điều tra log worker và broker"


def classify(entries: dict[str, ledger.Entry], processed: dict[str, int],
             snap: BrokerSnapshot) -> Report:
    """Lõi của reconcile: hàm thuần, không đụng DB hay broker nên test được trực tiếp."""
    r = Report()
    dmq, queue = snap.dmq, snap.queue

    for key, e in entries.items():
        r.dup_sent += max(e.attempts - 1, 0)
        if e.attempts > 0 and e.nacked == e.attempts:
            r.status[key] = PUBLISH_NACK
            r.notes[key] = "broker từ chối (NACK) mọi lần gửi: sự kiện chưa vào hệ thống, publisher phải gửi lại"
        elif key in processed:
            r.status[key] = PROCESSED
            if dmq is not None and dmq.get(key):
                r.dmq_stale.append(key)
        elif dmq is None or queue is None:
            r.status[key] = UNVERIFIED
            r.notes[key] = "chưa xử lý trong DB và không đọc được broker nên không biết nó ở đâu"
        elif dmq.get(key):
            r.status[key] = IN_DMQ
        elif queue.get(key):
            r.status[key] = IN_QUEUE
        else:
            r.status[key] = MISSING
            r.notes[key] = _missing_hint(e)

    r.unexpected = sorted(k for k in processed if k not in entries)
    r.dup_absorbed = sum(processed.values())
    if dmq is not None:
        r.foreign_dmq = sum(n for k, n in dmq.items() if k not in entries)
    if queue is not None:
        r.foreign_queue = sum(n for k, n in queue.items() if k not in entries)
    return r


# ---------- Đọc broker ----------

def browse_app_ids(service, queue_name: str, exclusive: bool, max_msgs: int = 20000,
                   idle_ms: int = 1500) -> tuple[Counter, int]:
    """Duyệt (không xóa) một queue, trả về (Counter các application_message_id, số message không có id)."""
    from solace.messaging.resources.queue import Queue

    queue = (Queue.durable_exclusive_queue(queue_name) if exclusive
             else Queue.durable_non_exclusive_queue(queue_name))
    browser = service.create_message_queue_browser_builder().build(queue)
    browser.start()
    ids, unattributed = Counter(), 0
    try:
        while sum(ids.values()) + unattributed < max_msgs:
            msg = browser.receive_message(timeout=idle_ms)
            if msg is None:  # hết message (hoặc hết thời gian chờ)
                break
            mid = msg.get_application_message_id()
            if mid:
                ids[mid] += 1
            else:
                unattributed += 1
    finally:
        browser.terminate()
    return ids, unattributed


class SolaceBroker:
    def __init__(self, queue: str, dmq: str):
        self.queue, self.dmq, self.service = queue, dmq, None

    def read(self) -> BrokerSnapshot:
        try:
            if self.service is None:
                from common import build_service
                self.service = build_service()
            dmq, dmq_un = browse_app_ids(self.service, self.dmq, exclusive=True)
            queue, q_un = browse_app_ids(self.service, self.queue, exclusive=False)
            return BrokerSnapshot(dmq, queue, dmq_un, q_un)
        except Exception as e:  # không đọc được broker thì báo UNVERIFIED chứ không báo mất oan
            return BrokerSnapshot(None, None, error=f"{type(e).__name__}: {e}")

    def close(self) -> None:
        if self.service is not None:
            try:
                self.service.disconnect()
            except Exception:
                pass


# ---------- Báo cáo ----------

def _details(title: str, keys: list[str], notes: dict[str, str]) -> None:
    print(f"\n{title}")
    for k in keys[:MAX_DETAIL_LINES]:
        print(f"  - {k}" + (f": {notes[k]}" if k in notes else ""))
    if len(keys) > MAX_DETAIL_LINES:
        print(f"  ... và {len(keys) - MAX_DETAIL_LINES} sự kiện nữa")


def print_report(entries, bad_lines, rep: Report, snap: BrokerSnapshot, processed,
                 inv_bad, orphans) -> int:
    counts = rep.counts()
    attempts = sum(e.attempts for e in entries.values())
    acked = sum(e.acked for e in entries.values())
    nacked = sum(e.nacked for e in entries.values())
    unconfirmed = sum(e.unconfirmed for e in entries.values())

    print("=== Đối chiếu: sổ publisher  <->  PostgreSQL  <->  broker ===")
    print(f"Sổ publisher : {len(entries)} sự kiện nghiệp vụ, {attempts} lần gửi ({rep.dup_sent} bản trùng); "
          f"biên nhận: {acked} ACK, {nacked} NACK, {unconfirmed} chưa rõ"
          + (f"; bỏ qua {bad_lines} dòng hỏng" if bad_lines else ""))
    print(f"PostgreSQL   : {len(processed)} sự kiện đã xử lý, {rep.dup_absorbed} bản trùng bị chặn")
    if snap.dmq is None:
        print(f"Broker       : KHÔNG đọc được ({snap.error})")
    else:
        print(f"Broker       : DMQ có {sum(snap.dmq.values()) + snap.dmq_unattributed} message, "
              f"queue có {sum(snap.queue.values()) + snap.queue_unattributed} message")

    print("\nKết quả theo sự kiện:")
    for state, label in [(PROCESSED, "đã xử lý"), (IN_DMQ, "đang trong DMQ (cần người xử lý)"),
                         (IN_QUEUE, "còn trong queue (chưa kết luận)"),
                         (PUBLISH_NACK, "broker từ chối khi gửi"),
                         (MISSING, "MẤT DỮ LIỆU IM LẶNG"), (UNVERIFIED, "chưa kiểm chứng được")]:
        print(f"  {state:<13}{counts.get(state, 0):>5}  {label}")

    if rep.keys_with(MISSING):
        _details("!! MẤT DỮ LIỆU IM LẶNG:", rep.keys_with(MISSING), rep.notes)
    if rep.keys_with(PUBLISH_NACK):
        _details("!! Broker từ chối khi gửi:", rep.keys_with(PUBLISH_NACK), rep.notes)
    if rep.keys_with(IN_DMQ):
        _details("Đang chờ xử lý trong DMQ:", rep.keys_with(IN_DMQ), {})
    if rep.keys_with(UNVERIFIED):
        _details("Chưa kiểm chứng được:", rep.keys_with(UNVERIFIED), rep.notes)

    warnings = []
    if rep.unexpected:
        warnings.append(f"{len(rep.unexpected)} sự kiện có trong DB nhưng KHÔNG có trong sổ (thừa): "
                        f"{', '.join(rep.unexpected[:5])}{'...' if len(rep.unexpected) > 5 else ''}")
    if rep.dmq_stale:
        warnings.append(f"{len(rep.dmq_stale)} sự kiện đã xử lý xong nhưng vẫn còn bản trong DMQ "
                        f"(có thể xóa an toàn): {', '.join(rep.dmq_stale[:5])}")
    if rep.foreign_dmq or rep.foreign_queue:
        warnings.append(f"broker có {rep.foreign_dmq} message trong DMQ và {rep.foreign_queue} trong queue "
                        f"mà sổ không biết (sót từ lần chạy trước? hãy dọn khi reset)")
    if snap.dmq is not None and (snap.dmq_unattributed or snap.queue_unattributed):
        warnings.append(f"{snap.dmq_unattributed + snap.queue_unattributed} message trên broker không có "
                        f"application_message_id nên không đối chiếu được")
    if warnings:
        print("\nĐiểm lạ cần xem:")
        for w in warnings:
            print(f"  - {w}")

    print("\nKiểm tra nội bộ DB:")
    print(f"  Đơn mồ côi (thiếu dấu xử lý): {len(orphans)}" + (f"  {orphans[:5]}" if orphans else ""))
    if inv_bad:
        for product, expected, actual in inv_bad:
            print(f"  Tồn kho LỆCH: {product}: kỳ vọng {expected}, thực tế {actual}")
    else:
        print("  Bất biến tồn kho: đúng cho mọi sản phẩm")

    hard = counts.get(MISSING, 0) + counts.get(PUBLISH_NACK, 0) + len(orphans) + len(inv_bad)
    if hard:
        print("\nKẾT LUẬN: CÓ SAI LỆCH. Xem các mục '!!' và kiểm tra nội bộ phía trên.")
        return 1
    if counts.get(UNVERIFIED, 0):
        print("\nKẾT LUẬN: CHƯA KẾT LUẬN ĐƯỢC vì không đọc được broker. Phần đã xử lý trong DB khớp sổ.")
        return 2
    if counts.get(IN_QUEUE, 0):
        print("\nKẾT LUẬN: chưa có sai lệch, nhưng còn message đang xử lý. Chạy lại khi worker rảnh (hoặc --wait).")
        return 0
    print("\nKẾT LUẬN: KHỚP. Mọi sự kiện đã gửi đều có dấu vết (đã xử lý hoặc đang chờ trong DMQ).")
    return 0


# ---------- Chạy ----------

def read_db():
    db.init_schema()
    with db.connect() as conn:
        return (db.processed_events(conn), db.inventory_mismatches(db.inventory_rows(conn)),
                db.orphan_orders(conn))


def main(argv=None, broker=None) -> int:
    ap = argparse.ArgumentParser(description="Đối chiếu sổ publisher, PostgreSQL và broker")
    ap.add_argument("--ledger", default=ledger.LEDGER_PATH, help="đường dẫn sổ cái (mặc định published_ledger.jsonl)")
    ap.add_argument("--queue", default="orders-queue")
    ap.add_argument("--dmq", default="orders-dmq")
    ap.add_argument("--no-broker", action="store_true", help="không đọc broker (chỉ đối chiếu sổ với DB)")
    ap.add_argument("--wait", type=int, default=0, metavar="GIÂY",
                    help="đợi tối đa chừng này giây cho tới khi hết message trong queue")
    args = ap.parse_args(argv)

    entries, bad_lines = ledger.load(args.ledger)
    if not entries:
        print(f"Sổ publisher '{args.ledger}' trống hoặc không tồn tại. Hãy chạy Order_publisher.py trước.")
        return 0

    own_broker = broker is None and not args.no_broker
    if own_broker:
        broker = SolaceBroker(args.queue, args.dmq)
    try:
        deadline = time.time() + args.wait
        while True:
            snap = broker.read() if broker else BrokerSnapshot(None, None, error="bỏ qua do --no-broker")
            processed, inv_bad, orphans = read_db()
            rep = classify(entries, processed, snap)
            if rep.counts().get(IN_QUEUE, 0) == 0 or time.time() >= deadline:
                break
            time.sleep(2)
    finally:
        if own_broker:
            broker.close()
    return print_report(entries, bad_lines, rep, snap, processed, inv_bad, orphans)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # DB không với tới v.v.
        raise SystemExit(f"[LỖI] {type(e).__name__}: {e}")