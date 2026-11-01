"""Order Service: sinh đơn hàng giả và publish lên topic shop/orders/<sự kiện>.

Chạy bình thường (như trước):
    python Order_publisher.py

Thực hành xử lý lỗi / retry / DMQ:
    python Order_publisher.py --scenario transient --count 2   # lỗi tạm thời -> retry -> DMQ
    python Order_publisher.py --scenario flaky --count 6       # lỗi chập chờn -> retry rồi thành công
    python Order_publisher.py --scenario permanent             # lỗi vĩnh viễn -> DMQ ngay
    python Order_publisher.py --scenario bad-json              # payload không phải JSON -> DMQ ngay
    python Order_publisher.py --scenario missing-field         # thiếu trường bắt buộc -> DMQ ngay
    python Order_publisher.py --scenario unknown-event         # sự kiện chưa hỗ trợ -> DMQ ngay

Thực hành TTL (TẮT worker trước, gửi, chờ quá TTL, rồi xem orders-dmq):
    python Order_publisher.py --count 2 --ttl 5000

Thực hành lỗi "quên cờ DMQ Eligible" (message bị xóa luôn, KHÔNG vào DMQ):
    python Order_publisher.py --scenario permanent --no-dmq
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid

from solace.messaging.config.solace_properties.message_properties import (
    PERSISTENT_DMQ_ELIGIBLE,
    PERSISTENT_TIME_TO_LIVE,
)
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.persistent_message_publisher import MessagePublishReceiptListener

from common import build_service

PRODUCTS = [("Áo thun", 150_000), ("Giày thể thao", 850_000), ("Balo", 400_000), ("Tai nghe", 1_200_000)]

SCENARIOS = ["normal", "transient", "flaky", "permanent", "bad-json", "missing-field", "unknown-event"]


class ReceiptListener(MessagePublishReceiptListener):
    """Nhận xác nhận từ broker rằng message đã được lưu bền vững."""

    def on_publish_receipt(self, publish_receipt):
        if publish_receipt.exception:
            print(f"   [NACK] {publish_receipt.user_context}: {publish_receipt.exception}")
        else:
            print(f"   [ACK ] broker đã lưu đơn {publish_receipt.user_context}")


def make_order() -> dict:
    name, price = random.choice(PRODUCTS)
    qty = random.randint(1, 3)
    return {
        "order_id": str(uuid.uuid4())[:8],
        "customer": random.choice(["An", "Bình", "Chi", "Dũng"]),
        "product": name,
        "quantity": qty,
        "total": price * qty,
    }


def build_event(scenario: str) -> tuple[str, str, str]:
    """Trả về (event, payload, order_id) theo kịch bản."""
    order = make_order()
    event = "created"

    if scenario == "normal":
        # Phần lớn là đơn mới; thỉnh thoảng có đơn bị hủy
        event = "cancelled" if random.random() < 0.2 else "created"
        payload = json.dumps(order, ensure_ascii=False)
    elif scenario in ("transient", "flaky", "permanent"):
        order["_simulate"] = scenario  # order_processor.simulate_failure() đọc trường này
        payload = json.dumps(order, ensure_ascii=False)
    elif scenario == "bad-json":
        payload = "{đây không phải JSON"
    elif scenario == "missing-field":
        del order["product"]
        payload = json.dumps(order, ensure_ascii=False)
    elif scenario == "unknown-event":
        event = "refunded"
        payload = json.dumps(order, ensure_ascii=False)
    else:
        raise ValueError(f"Kịch bản không hợp lệ: {scenario}")

    return event, payload, order["order_id"]


def build_message(service, payload: str, ttl_ms: int | None, dmq_eligible: bool):
    props = {PERSISTENT_DMQ_ELIGIBLE: dmq_eligible}  # API mặc định là True, đặt rõ cho dễ thấy
    if ttl_ms:
        props[PERSISTENT_TIME_TO_LIVE] = ttl_ms  # đơn vị: mili giây
    return service.message_builder().build(payload, additional_message_properties=props)


def main(count: int, scenario: str = "normal", ttl_ms: int | None = None,
         dmq_eligible: bool = True) -> None:
    service = build_service()
    publisher = service.create_persistent_message_publisher_builder().build()
    publisher.start()
    publisher.set_message_publish_receipt_listener(ReceiptListener())

    print(f"Kịch bản: {scenario} | số message: {count} | TTL: "
          f"{f'{ttl_ms} ms' if ttl_ms else 'không'} | DMQ eligible: {dmq_eligible}")
    try:
        for _ in range(count):
            event, payload, order_id = build_event(scenario)
            topic = Topic.of(f"shop/orders/{event}")
            message = build_message(service, payload, ttl_ms, dmq_eligible)

            print(f"-> Gửi {topic.get_name()}: {order_id}")
            publisher.publish(message, topic, user_context=order_id)
            time.sleep(0.2)
        time.sleep(2)  # chờ các xác nhận cuối cùng từ broker
    finally:
        publisher.terminate()
        service.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Order Service: publish đơn hàng giả lên Solace")
    parser.add_argument("--scenario", choices=SCENARIOS, default="normal",
                        help="kịch bản thực hành xử lý lỗi (mặc định: normal)")
    parser.add_argument("--count", type=int, default=None,
                        help="số message (mặc định: 12 cho normal, 3 cho kịch bản lỗi)")
    parser.add_argument("--ttl", type=int, default=None, metavar="MS",
                        help="time-to-live của message, tính bằng mili giây")
    parser.add_argument("--no-dmq", action="store_true",
                        help="đặt DMQ Eligible = false (message hỏng sẽ bị xóa thay vì vào DMQ)")
    args = parser.parse_args()

    count = args.count if args.count is not None else (12 if args.scenario == "normal" else 3)
    main(count, args.scenario, args.ttl, dmq_eligible=not args.no_dmq)