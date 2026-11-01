"""Các hàm dùng chung: kết nối broker và vòng lặp consumer."""
import os

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.config.missing_resources_creation_configuration import (
    MissingResourcesCreationStrategy,
)
from solace.messaging.config.message_acknowledgement_configuration import Outcome

from errors import PermanentError, TransientError


def build_service() -> MessagingService:
    """Tạo và kết nối MessagingService. Có thể đổi cấu hình qua biến môi trường."""
    props = {
        "solace.messaging.transport.host": os.getenv("SOLACE_HOST", "tcp://localhost:55555"),
        "solace.messaging.service.vpn-name": os.getenv("SOLACE_VPN", "default"),
        "solace.messaging.authentication.scheme.basic.username": os.getenv("SOLACE_USER", "admin"),
        "solace.messaging.authentication.scheme.basic.password": os.getenv("SOLACE_PASSWORD", "admin"),
    }
    service = MessagingService.builder().from_properties(props).build()
    service.connect()
    print(f"[OK] Đã kết nối broker {props['solace.messaging.transport.host']}")
    return service


def decode_payload(msg) -> str:
    """Lấy payload dạng text. Message không đọc được là lỗi vĩnh viễn."""
    payload = msg.get_payload_as_string()
    if payload is None:
        raw = msg.get_payload_as_bytes()
        if raw is None:
            raise PermanentError("Message không có payload")
        try:
            payload = raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise PermanentError(f"Payload không phải UTF-8: {e}") from e
    return payload


def process_message(receiver, msg, handle) -> None:
    """Gọi handle() rồi settle message theo kết quả:
        thành công          -> ACCEPTED (broker xóa message)
        TransientError      -> FAILED   (broker giao lại, vượt max redelivery thì vào DMQ)
        PermanentError      -> REJECTED (vào DMQ ngay)
        exception khác      -> FAILED   (coi là tạm thời; max redelivery sẽ chặn vòng lặp vô hạn)
    """
    topic = msg.get_destination_name()
    if msg.is_redelivered():
        print(f"[GIAO LẠI] {topic}")

    try:
        handle(topic, decode_payload(msg))
    except PermanentError as e:
        print(f"[REJECTED] {topic}: {e} -> chuyển DMQ")
        outcome = Outcome.REJECTED
    except TransientError as e:
        print(f"[FAILED] {topic}: {e} -> chờ broker giao lại")
        outcome = Outcome.FAILED
    except Exception as e:  # bug chưa lường trước (KeyError, TypeError...)
        print(f"[FAILED] {topic}: lỗi không lường trước {type(e).__name__}: {e} "
              f"-> coi là tạm thời")
        outcome = Outcome.FAILED
    else:
        outcome = Outcome.ACCEPTED

    receiver.settle(msg, outcome)


def run_consumer(queue_name: str, topics: list[str], handle, exclusive: bool = True) -> None:
    service = build_service()
    if exclusive:
        queue = Queue.durable_exclusive_queue(queue_name)
    else:
        queue = Queue.durable_non_exclusive_queue(queue_name)

    receiver = (
        service.create_persistent_message_receiver_builder()
        .with_missing_resources_creation_strategy(MissingResourcesCreationStrategy.CREATE_ON_START)
        .with_message_client_acknowledgement()
        # Bắt buộc khai báo trước thì mới được settle FAILED / REJECTED
        .with_required_message_outcome_support(Outcome.FAILED, Outcome.REJECTED)
        .build(queue)
    )
    receiver.start()

    for topic in topics:
        try:
            receiver.add_subscription(TopicSubscription.of(topic))
            print(f"[OK] Queue '{queue_name}' đã subscribe topic: {topic}")
        except Exception as e:
            print(f"[BỎ QUA] Không tự gắn được topic '{topic}' ({type(e).__name__}). "
                  f"Giả định đã gắn sẵn trong Broker Manager.")

    print("Đang chờ message... (Ctrl+C để dừng)")
    try:
        while True:
            msg = receiver.receive_message(timeout=5000)
            if msg is None:
                continue
            process_message(receiver, msg, handle)
    except KeyboardInterrupt:
        print("\nĐang dừng...")
    finally:
        receiver.terminate()
        service.disconnect()