"""Notification Service: gửi thông báo cho khách khi có sự kiện đơn hàng (shop/orders/>).

Đây là consumer thứ HAI của cùng một sự kiện. Nó có queue riêng (notifications-queue)
nên nhận một BẢN SAO của mọi message, độc lập hoàn toàn với order_processor.py:
tốc độ xử lý, retry và DMQ đều riêng.

Có thể chạy nhiều bản song song: các worker CÙNG queue sẽ chia việc cho nhau,
còn worker khác queue thì mỗi bên nhận đủ một bản.

Quy ước lỗi giống order_processor.py (xem errors.py):
  return bình thường -> ACCEPTED | TransientError -> FAILED | PermanentError -> REJECTED
"""
import json
import os
import time

from common import run_consumer
from errors import PermanentError, TransientError

WORKER = f"notify-{os.getpid()}"

# Service này cần dữ liệu khác với order_processor (cần tên khách để gửi thông báo).
# Mỗi service tự định nghĩa và tự kiểm tra "hợp đồng" dữ liệu của mình.
REQUIRED_FIELDS = {
    "created": ("order_id", "customer", "product"),
    "cancelled": ("order_id", "customer"),
}

# Chống gửi trùng (gửi thông báo 2 lần còn tệ hơn xử lý đơn 2 lần).
# Giống order_processor: chỉ sống trong 1 process, production cần lưu dùng chung (DB).
SENT = set()


def parse_order(event: str, payload: str) -> dict:
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


def simulate_failure(order: dict) -> None:
    """Hook thực hành: publish với --scenario notify-down thì payload có
    "_simulate_notify": "transient". Chỉ service này đọc trường đó,
    nên order_processor vẫn xử lý bình thường. Xóa hàm này khi lên production."""
    if order.get("_simulate_notify") == "transient":
        raise TransientError("Giả lập: cổng gửi email/SMS tạm thời không phản hồi")


def handle(topic: str, payload: str) -> None:
    event = topic.split("/")[-1]
    if event not in REQUIRED_FIELDS:
        raise PermanentError(f"Sự kiện chưa hỗ trợ: {topic}")

    order = parse_order(event, payload)

    key = (order["order_id"], event)
    if key in SENT:
        print(f"[{WORKER}] BỎ QUA đơn {order['order_id']} ({event}): đã gửi thông báo rồi")
        return

    time.sleep(0.5)  # giả lập gọi cổng gửi email (nhanh hơn order_processor 1.5s)
    simulate_failure(order)

    if event == "created":
        print(f"[{WORKER}] EMAIL cho {order['customer']}: đơn {order['order_id']} "
              f"({order['product']}) đã được ghi nhận")
    elif event == "cancelled":
        print(f"[{WORKER}] EMAIL cho {order['customer']}: đơn {order['order_id']} đã bị hủy")

    SENT.add(key)


if __name__ == "__main__":
    run_consumer("notifications-queue", ["shop/orders/>"], handle, exclusive=False)