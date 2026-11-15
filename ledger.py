"""Sổ cái của publisher (hệ thống nguồn): ghi lại MỌI sự kiện đã định gửi và biên nhận từ broker.

File JSON Lines, chỉ ghi thêm (append-only). reconcile.py đối chiếu sổ này với PostgreSQL và broker.
Hai loại dòng:
  {"type": "sent",    "order_id", "event", "attempt", "scenario", "dmq_eligible", "ttl_ms", "ts"}
  {"type": "receipt", "order_id", "event", "attempt", "ok", "error", "ts"}

"Danh tính nghiệp vụ" của một sự kiện là message_key(order_id, event) = "order_id:event".
Publisher cũng gắn đúng chuỗi này vào application_message_id của message, nhờ vậy khi duyệt
queue/DMQ ta biết chính xác message nào ứng với sự kiện nào mà không cần đọc payload
(payload hỏng như bad-json thì không đọc được).
"""
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field

LEDGER_PATH = os.getenv("LEDGER_PATH", "published_ledger.jsonl")
_lock = threading.Lock()  # biên nhận của broker về từ thread khác với thread đang publish


def message_key(order_id: str, event: str) -> str:
    return f"{order_id}:{event}"


def _append(record: dict, path: str | None = None) -> None:
    record["ts"] = round(time.time(), 3)
    line = json.dumps(record, ensure_ascii=False)
    with _lock, open(path or LEDGER_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def record_sent(order_id: str, event: str, attempt: int, scenario: str,
                dmq_eligible: bool, ttl_ms: int | None, path: str | None = None) -> None:
    """Ghi TRƯỚC khi publish: nếu tiến trình chết giữa chừng thì sổ vẫn còn dấu vết."""
    _append({"type": "sent", "order_id": order_id, "event": event, "attempt": attempt,
             "scenario": scenario, "dmq_eligible": dmq_eligible, "ttl_ms": ttl_ms}, path)


def record_receipt(order_id: str, event: str, attempt: int, ok: bool,
                   error: str | None = None, path: str | None = None) -> None:
    _append({"type": "receipt", "order_id": order_id, "event": event, "attempt": attempt,
             "ok": ok, "error": error}, path)


@dataclass
class Entry:
    """Tổng hợp mọi lần gửi của MỘT sự kiện nghiệp vụ."""
    key: str
    order_id: str
    event: str
    attempts: int = 0   # số lần publisher đã gửi (>1 nghĩa là có bản trùng)
    acked: int = 0      # broker xác nhận đã lưu bền vững
    nacked: int = 0     # broker từ chối
    dmq_eligible: bool = True   # False nếu BẤT KỲ lần gửi nào tắt cờ DMQ
    ttl_ms: int | None = None
    scenarios: set = field(default_factory=set)

    @property
    def unconfirmed(self) -> int:
        return self.attempts - self.acked - self.nacked


def load(path: str | None = None) -> tuple[dict[str, Entry], int]:
    """Đọc sổ. Trả về (entries theo key, số dòng hỏng bị bỏ qua).
    Dòng cuối có thể bị cắt dở nếu publisher chết giữa lúc ghi, nên dòng hỏng được bỏ qua chứ không làm sập."""
    entries: dict[str, Entry] = {}
    bad = 0
    p = path or LEDGER_PATH
    if not os.path.exists(p):
        return entries, 0
    with open(p, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                key = message_key(rec["order_id"], rec["event"])
                if rec["type"] == "sent":
                    # Chỉ dòng "sent" mới được tạo sự kiện: không để dòng lạ hay biên nhận mồ côi
                    # sinh ra "sự kiện ma" (không có lần gửi nào) rồi bị reconcile báo MISSING oan.
                    entry = entries.setdefault(key, Entry(key, rec["order_id"], rec["event"]))
                    entry.attempts += 1
                    entry.dmq_eligible = entry.dmq_eligible and bool(rec.get("dmq_eligible", True))
                    entry.ttl_ms = rec.get("ttl_ms") or entry.ttl_ms
                    entry.scenarios.add(rec.get("scenario", "?"))
                elif rec["type"] == "receipt" and key in entries:
                    if rec["ok"]:
                        entries[key].acked += 1
                    else:
                        entries[key].nacked += 1
                else:
                    bad += 1  # loại dòng không biết, hoặc biên nhận không có dòng "sent" tương ứng
            except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
                bad += 1
    return entries, bad


def reset(path: str | None = None) -> None:
    p = path or LEDGER_PATH
    if os.path.exists(p):
        os.remove(p)