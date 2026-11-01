"""Order Processor: xử lý MỌI sự kiện đơn hàng (shop/orders/>).
Có thể chạy nhiều bản song song, broker sẽ chia đơn cho các worker.

Xử lý lỗi: handle() KHÔNG tự ack/nack, chỉ phân loại kết quả bằng exception:
  - return bình thường -> run_consumer settle ACCEPTED
  - TransientError     -> FAILED   (broker giao lại; vượt max redelivery thì vào DMQ)
  - PermanentError     -> REJECTED (vào DMQ ngay, không retry vô ích)
"""
import json
import os
import random
import time

from common import run_consumer
from errors import PermanentError, TransientError

WORKER = f"worker-{os.getpid()}"

# Các trường bắt buộc theo từng loại sự kiện
REQUIRED_FIELDS = {
    "created": ("order_id", "product", "quantity", "total"),
    "cancelled": ("order_id",),
}

# Chống xử lý trùng: Solace là at-least-once nên message có thể đến 2 lần.
# Lưu ý: set này chỉ sống trong 1 process. Khi chạy nhiều worker song song,
# cần lưu ở nơi dùng chung (vd DB với unique key (order_id, event)).
PROCESSED = set()


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


def simulate_failure(order: dict) -> None:
    """Hook để thực hành. Publish payload có thêm "_simulate":
      "transient" -> luôn lỗi tạm thời (quan sát retry rồi rơi vào DMQ)
      "flaky"     -> 50% lỗi tạm thời (quan sát retry rồi thành công)
      "permanent" -> lỗi vĩnh viễn (vào DMQ ngay)
    Xóa hàm này khi lên production."""
    mode = order.get("_simulate")
    if mode == "transient":
        raise TransientError("Giả lập: downstream tạm thời không phản hồi")
    if mode == "flaky" and random.random() < 0.5:
        raise TransientError("Giả lập: downstream chập chờn")
    if mode == "permanent":
        raise PermanentError("Giả lập: dữ liệu đơn hàng không hợp lệ")


def handle(topic: str, payload: str) -> None:
    event = topic.split("/")[-1]
    if event not in REQUIRED_FIELDS:
        # Trước đây chỉ in log rồi ACK (mất message âm thầm); giờ đưa vào DMQ để còn điều tra.
        raise PermanentError(f"Sự kiện chưa hỗ trợ: {topic}")

    order = parse_order(event, payload)

    key = (order["order_id"], event)
    if key in PROCESSED:
        print(f"[{WORKER}] BỎ QUA đơn {order['order_id']} ({event}): đã xử lý rồi")
        return  # vẫn ACCEPTED để broker xóa message trùng

    time.sleep(1.5)  # giả lập thời gian xử lý
    simulate_failure(order)

    if event == "created":
        print(f"[{WORKER}] XỬ LÝ đơn {order['order_id']}: {order['quantity']} x "
              f"{order['product']} = {order['total']:,}đ")
    elif event == "cancelled":
        print(f"[{WORKER}] HỦY đơn {order['order_id']} -> hoàn kho")

    PROCESSED.add(key)  # đánh dấu SAU khi side effect xong


if __name__ == "__main__":
    run_consumer("orders-queue", ["shop/orders/>"], handle, exclusive=False)