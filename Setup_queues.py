"""Cấu hình queue qua SEMP v2: tạo DMQ, gắn DMQ vào queue nghiệp vụ, đặt max redelivery.

API messaging (Python) không cấu hình được DMQ/max redelivery, nên phải làm qua SEMP
(hoặc tay trên Broker Manager). Chạy 1 lần TRƯỚC khi start worker; chạy lại vẫn an toàn.

Biến môi trường (mặc định khớp với common.py):
    SOLACE_SEMP      http://localhost:8080   (cổng management của broker)
    SOLACE_VPN       default
    SOLACE_USER / SOLACE_PASSWORD   admin / admin
"""
from __future__ import annotations

import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

SEMP_BASE = os.getenv("SOLACE_SEMP", "http://localhost:8080").rstrip("/")
VPN = os.getenv("SOLACE_VPN", "default")
USER = os.getenv("SOLACE_USER", "admin")
PASSWORD = os.getenv("SOLACE_PASSWORD", "admin")

# Fan-out: mỗi service có queue + DMQ RIÊNG. Hai queue cùng subscribe một topic nên
# mỗi message publish lên shop/orders/... được broker sao chép vào CẢ HAI queue.
# (queue, dmq, danh sách topic subscription)
SERVICES = [
    ("orders-queue", "orders-dmq", ["shop/orders/>"]),
    ("notifications-queue", "notifications-dmq", ["shop/orders/>"]),
]
MAX_REDELIVERY = 3


def semp(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    url = f"{SEMP_BASE}/SEMP/v2/config/msgVpns/{urllib.parse.quote(VPN, safe='')}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    token = base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    req.add_header("Authorization", f"Basic {token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, (json.loads(raw) if raw else {})


def error_info(resp: dict) -> tuple[str, str]:
    err = resp.get("meta", {}).get("error", {})
    return err.get("status", ""), err.get("description", "")


def ensure_queue(name: str, access_type: str, permission: str, **config) -> None:
    """Tạo queue nếu chưa có; nếu có rồi thì chỉ PATCH phần cấu hình."""
    create_body = {
        "queueName": name,
        "accessType": access_type,
        "permission": permission,
        "ingressEnabled": True,   # queue tạo qua SEMP mặc định là tắt, phải bật
        "egressEnabled": True,
        **config,
    }
    status, resp = semp("POST", "/queues", create_body)
    if status == 200:
        print(f"[OK] Đã tạo queue '{name}'")
        return
    err_status, err_desc = error_info(resp)
    if err_status != "ALREADY_EXISTS":
        sys.exit(f"[LỖI] Tạo queue '{name}' thất bại ({status}): {err_status} {err_desc}")

    if not config:
        print(f"[OK] Queue '{name}' đã tồn tại")
        return
    status, resp = semp("PATCH", f"/queues/{urllib.parse.quote(name, safe='')}", config)
    if status != 200:
        err_status, err_desc = error_info(resp)
        sys.exit(f"[LỖI] Cập nhật queue '{name}' thất bại ({status}): {err_status} {err_desc}\n"
                 f"      Nếu broker yêu cầu tắt queue trước khi sửa, hãy sửa tay trong Broker Manager.")
    print(f"[OK] Queue '{name}' đã tồn tại, đã cập nhật cấu hình")


def ensure_subscription(queue: str, topic: str) -> None:
    path = f"/queues/{urllib.parse.quote(queue, safe='')}/subscriptions"
    status, resp = semp("POST", path, {"subscriptionTopic": topic})
    err_status, err_desc = error_info(resp)
    if status == 200:
        print(f"[OK] Queue '{queue}' đã gắn topic {topic}")
    elif err_status == "ALREADY_EXISTS":
        print(f"[OK] Queue '{queue}' đã có topic {topic}")
    else:
        sys.exit(f"[LỖI] Gắn topic '{topic}' thất bại ({status}): {err_status} {err_desc}")


if __name__ == "__main__":
    for queue, dmq, topics in SERVICES:
        # 1) DMQ phải tồn tại trước khi queue nghiệp vụ trỏ tới nó
        ensure_queue(dmq, "exclusive", "consume")

        # 2) Queue nghiệp vụ: giao lại tối đa N lần rồi chuyển DMQ, tôn trọng TTL của message
        ensure_queue(
            queue, "non-exclusive", "modify-topic",
            deadMsgQueue=dmq,
            maxRedeliveryCount=MAX_REDELIVERY,
            redeliveryEnabled=True,
            respectTtlEnabled=True,
        )

        for t in topics:
            ensure_subscription(queue, t)
        print(f"[XONG] '{queue}' -> DMQ '{dmq}', max redelivery = {MAX_REDELIVERY}\n")