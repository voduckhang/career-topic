"""Hồi quy cho setup_queues.py bằng SEMP server giả (không cần broker)."""
import http.server
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


class FakeSemp:
    def __init__(self):
        self.queues, self.subs, self.calls = {}, set(), []
        self.fail_create_for = None  # tên queue mà POST sẽ bị từ chối

    def handler(self):
        state = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def reply(self, code, obj):
                body = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def body(self):
                return json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")

            @staticmethod
            def err(status, desc="x"):
                return {"meta": {"error": {"status": status, "description": desc}}}

            def do_POST(self):
                b = self.body()
                state.calls.append(("POST", self.path, b))
                if self.path.endswith("/subscriptions"):
                    key = (self.path.split("/queues/")[1].split("/")[0], b["subscriptionTopic"])
                    if key in state.subs:
                        return self.reply(400, self.err("ALREADY_EXISTS"))
                    state.subs.add(key)
                    return self.reply(200, {})
                if b["queueName"] == state.fail_create_for:
                    return self.reply(400, self.err("NOT_ALLOWED", "không đủ quyền"))
                if b["queueName"] in state.queues:
                    return self.reply(400, self.err("ALREADY_EXISTS"))
                state.queues[b["queueName"]] = dict(b)
                self.reply(200, {})

            def do_PATCH(self):
                state.calls.append(("PATCH", self.path, self.body()))
                state.queues[self.path.rsplit("/", 1)[1]].update(state.calls[-1][2])
                self.reply(200, {})

        return H


@pytest.fixture
def semp():
    state = FakeSemp()
    server = http.server.HTTPServer(("127.0.0.1", 0), state.handler())
    threading.Thread(target=server.serve_forever, daemon=True).start()
    state.url = f"http://127.0.0.1:{server.server_address[1]}"
    yield state
    server.shutdown()


def run_setup(semp):
    env = {**os.environ, "SOLACE_SEMP": semp.url}
    return subprocess.run([sys.executable, "setup_queues.py"], cwd=ROOT, env=env, capture_output=True, text=True)


PAIRS = [("orders-queue", "orders-dmq"), ("notifications-queue", "notifications-dmq")]


def test_each_service_gets_its_own_queue_and_dmq_with_retry_policy(semp):
    result = run_setup(semp)
    assert result.returncode == 0, result.stderr
    for queue, dmq in PAIRS:
        config = semp.queues[queue]
        assert config["deadMsgQueue"] == dmq
        assert config["maxRedeliveryCount"] == 3 and config["respectTtlEnabled"] is True
        assert config["accessType"] == "non-exclusive"
        assert semp.queues[dmq]["ingressEnabled"] and semp.queues[dmq]["egressEnabled"]
        assert (queue, "shop/orders/>") in semp.subs  # fan-out: cả hai queue cùng subscribe một topic


def test_dmq_is_created_before_the_queue_that_points_to_it(semp):
    run_setup(semp)
    created = [c[2]["queueName"] for c in semp.calls if c[0] == "POST" and c[1].endswith("/queues")]
    assert created.index("orders-dmq") < created.index("orders-queue")
    assert created.index("notifications-dmq") < created.index("notifications-queue")


def test_rerun_is_idempotent(semp):
    assert run_setup(semp).returncode == 0
    snapshot = json.dumps(semp.queues, sort_keys=True)
    second = run_setup(semp)
    assert second.returncode == 0 and "đã tồn tại" in second.stdout
    assert json.dumps(semp.queues, sort_keys=True) == snapshot


def test_rerun_patches_only_config_never_access_type(semp):
    run_setup(semp)
    semp.calls.clear()
    run_setup(semp)
    patches = [c[2] for c in semp.calls if c[0] == "PATCH"]
    assert patches and all("accessType" not in p and "queueName" not in p for p in patches)


def test_broker_refusal_fails_loudly(semp):
    semp.fail_create_for = "orders-dmq"
    result = run_setup(semp)
    assert result.returncode != 0 and "không đủ quyền" in (result.stdout + result.stderr)