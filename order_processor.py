"""Order Processor: xử lý MỌI sự kiện đơn hàng (shop/orders/>).
Có thể chạy nhiều bản song song, broker sẽ chia đơn cho các worker.

Xử lý lỗi: handle() KHÔNG tự ack/nack, chỉ phân loại kết quả bằng exception:
  - return bình thường -> run_consumer settle ACCEPTED
  - TransientError     -> FAILED   (broker giao lại; vượt max redelivery thì vào DMQ)
  - PermanentError     -> REJECTED (vào DMQ ngay, không retry vô ích)

Idempotency (PostgreSQL): Solace là at-least-once nên cùng một sự kiện có thể đến nhiều lần,
kể cả ở NHIỀU worker khác nhau. Mỗi sự kiện được "xí chỗ" trong bảng processed_events với khóa
(order_id, event), CÙNG transaction với việc ghi đơn và trừ/hoàn kho (xem db.py).
"""
import json
import os
import random
import time

import db
from common import run_consumer
from errors import PermanentError, TransientError

WORKER = f"worker-{os.getpid()}"

# Các trường bắt buộc theo từng loại sự kiện
REQUIRED_FIELDS = {
    "created": ("order_id", "product", "quantity", "total"),
    "cancelled": ("order_id",),
}


def parse_order(event: str, payload: str) -> dict:
    """Parse + validate. Dữ liệu hỏng là lỗi vĩnh viễn: retry cũng vô ích."""
    try:
        order = json.loads(payload)
    except json.JSONDecodeError as e:
        raise PermanentError(f"JSON không hợp lệ: {e}") from e
    if not isinstance(order, dict):
        raise PermanentError("Payload phải là một JSON object")
    missing = [f for f in REQUIRED_FIELDS[event] if f not in order]
    if missing:
        raise PermanentError(f"Thiếu trường {missing} cho sự kiện '{event}'")
    return order


def simulate_failure(order: dict, stage: str = "before") -> None:
    """Hook để thực hành. Publish payload có thêm "_simulate":
      "transient" -> luôn lỗi tạm thời (quan sát retry rồi rơi vào DMQ)
      "flaky"     -> 50% lỗi tạm thời (quan sát retry rồi thành công)
      "permanent" -> lỗi vĩnh viễn (vào DMQ ngay)
      "txn-flaky" -> 50% lỗi GIỮA transaction, sau khi đã ghi DB (quan sát ROLLBACK)
    Xóa hàm này khi lên production."""
    mode = order.get("_simulate")
    if stage == "in_txn":
        if mode == "txn-flaky" and random.random() < 0.5:
            raise TransientError("Giả lập: lỗi GIỮA transaction, sau khi đã ghi DB -> ROLLBACK")
        return
    if mode == "transient":
        raise TransientError("Giả lập: downstream tạm thời không phản hồi")
    if mode == "flaky" and random.random() < 0.5:
        raise TransientError("Giả lập: downstream chập chờn")
    if mode == "permanent":
        raise PermanentError("Giả lập: dữ liệu đơn hàng không hợp lệ")


def apply_created(cur, order: dict) -> str:
    qty, total = order["quantity"], order["total"]
    if not isinstance(qty, int) or isinstance(qty, bool) or qty <= 0:
        raise PermanentError(f"quantity không hợp lệ: {qty!r}")
    if not isinstance(total, (int, float)) or isinstance(total, bool):
        raise PermanentError(f"total không hợp lệ: {total!r}")
    # Sản phẩm lạ -> vi phạm khóa ngoại -> db.transaction() đổi thành PermanentError
    cur.execute(
        "INSERT INTO orders (order_id, customer, product, quantity, total, status) "
        "VALUES (%s, %s, %s, %s, %s, 'created')",
        (order["order_id"], order.get("customer"), order["product"], qty, total))
    # Tồn kho về âm -> vi phạm CHECK (stock >= 0) -> PermanentError
    cur.execute("UPDATE inventory SET stock = stock - %s WHERE product = %s", (qty, order["product"]))
    return f"XỬ LÝ đơn {order['order_id']}: {qty} x {order['product']} = {total:,}đ (đã trừ kho)"


def apply_cancelled(cur, order: dict) -> str:
    # Chỉ hủy đơn đang ở trạng thái 'created'; RETURNING cho biết cần hoàn bao nhiêu
    cur.execute(
        "UPDATE orders SET status = 'cancelled', updated_at = now() "
        "WHERE order_id = %s AND status = 'created' RETURNING product, quantity",
        (order["order_id"],))
    row = cur.fetchone()
    if row is None:
        return f"HỦY đơn {order['order_id']}: không có đơn đang hoạt động -> không hoàn kho"
    product, qty = row
    cur.execute("UPDATE inventory SET stock = stock + %s WHERE product = %s", (qty, product))
    return f"HỦY đơn {order['order_id']} -> hoàn {qty} x {product} vào kho"


def handle(topic: str, payload: str) -> None:
    event = topic.split("/")[-1]
    if event not in REQUIRED_FIELDS:
        raise PermanentError(f"Sự kiện chưa hỗ trợ: {topic}")

    order = parse_order(event, payload)
    order_id = order["order_id"]

    time.sleep(1.5)  # giả lập thời gian xử lý, làm NGOÀI transaction để không giữ khóa DB lâu
    simulate_failure(order, "before")

    with db.transaction() as cur:
        if not db.claim_event(cur, order_id, event, WORKER):
            print(f"[{WORKER}] BỎ QUA đơn {order_id} ({event}): bản trùng, đã xử lý rồi")
            return  # vẫn ACCEPTED để broker xóa message trùng

        if event == "created":
            result = apply_created(cur, order)
        else:
            result = apply_cancelled(cur, order)

        simulate_failure(order, "in_txn")  # lỗi ở đây -> ROLLBACK cả claim lẫn nghiệp vụ

    print(f"[{WORKER}] {result}")  # chỉ in SAU khi COMMIT thành công


if __name__ == "__main__":
    try:
        db.init_schema()
    except Exception as e:
        raise SystemExit(f"[LỖI] Không kết nối được PostgreSQL ({type(e).__name__}: {e})\n"
                         f"      Đã chạy container Postgres chưa? (xem hướng dẫn docker run)")
    print("[OK] PostgreSQL sẵn sàng")
    run_consumer("orders-queue", ["shop/orders/>"], handle, exclusive=False)