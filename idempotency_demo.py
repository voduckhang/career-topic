"""Thực hành idempotency với PostgreSQL. KHÔNG cần broker Solace, chỉ cần container Postgres.

    python idempotency_demo.py

Mỗi thí nghiệm cho thấy một cách làm SAI rồi một cách làm ĐÚNG:
  A. 8 worker nhận CÙNG một sự kiện cùng lúc:
       SAI : SELECT kiểm tra rồi mới INSERT  (có khoảng hở giữa hai bước)
       ĐÚNG: INSERT ... ON CONFLICT DO NOTHING (xí chỗ nguyên tử)
  B. Worker chết giữa chừng rồi broker giao lại:
       SAI : claim và nghiệp vụ nằm ở 2 transaction riêng -> mất việc im lặng
       ĐÚNG: claim và nghiệp vụ cùng 1 transaction -> ROLLBACK, lần giao lại làm lại được
Mỗi sự kiện trừ kho đúng 1 đơn vị, kho ban đầu 100, nên kết quả đúng luôn là 99.
"""
import threading
import time

import psycopg

from db import DATABASE_URL

THREADS = 8
START, EXPECTED = 100, 99


def new_conn():
    return psycopg.connect(DATABASE_URL, autocommit=True)


def setup(conn):
    conn.execute("DROP TABLE IF EXISTS demo_events, demo_stock")
    conn.execute("CREATE TABLE demo_stock (id int PRIMARY KEY, stock int NOT NULL)")
    conn.execute("CREATE TABLE demo_events (event_id text PRIMARY KEY)")
    conn.execute("INSERT INTO demo_stock VALUES (1, %s)", (START,))


def stock(conn):
    return conn.execute("SELECT stock FROM demo_stock WHERE id = 1").fetchone()[0]


def verdict(actual):
    return "ĐÚNG" if actual == EXPECTED else f"SAI (kho lệch {actual - EXPECTED:+d} so với kỳ vọng)"


# ---------- Thí nghiệm A: tranh chấp đồng thời ----------

def naive_worker(conn, event_id):
    if conn.execute("SELECT 1 FROM demo_events WHERE event_id = %s", (event_id,)).fetchone():
        return "bỏ qua"
    time.sleep(0.05)  # "xử lý": khoảng hở giữa lúc kiểm tra và lúc ghi dấu
    conn.execute("UPDATE demo_stock SET stock = stock - 1 WHERE id = 1")
    conn.execute("INSERT INTO demo_events VALUES (%s) ON CONFLICT DO NOTHING", (event_id,))
    return "đã xử lý"


def claim_worker(conn, event_id):
    with conn.transaction():
        cur = conn.execute("INSERT INTO demo_events VALUES (%s) ON CONFLICT DO NOTHING", (event_id,))
        if cur.rowcount == 0:
            return "bỏ qua"
        time.sleep(0.05)
        conn.execute("UPDATE demo_stock SET stock = stock - 1 WHERE id = 1")
        return "đã xử lý"


def run_concurrent(worker, event_id):
    barrier, results = threading.Barrier(THREADS), []

    def target():
        with new_conn() as conn:
            barrier.wait()  # cả 8 worker xuất phát cùng lúc
            results.append(worker(conn, event_id))

    threads = [threading.Thread(target=target) for _ in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def experiment_a(conn):
    print(f"=== Thí nghiệm A: {THREADS} worker nhận cùng 1 sự kiện cùng lúc ===")
    for name, worker in [("SELECT rồi INSERT      ", naive_worker),
                         ("INSERT ON CONFLICT      ", claim_worker)]:
        setup(conn)
        results = run_concurrent(worker, "order-1:created")
        done = results.count("đã xử lý")
        print(f"  {name}: {done} worker xử lý, {THREADS - done} bỏ qua | "
              f"kho = {stock(conn)} (đúng phải là {EXPECTED}) -> {verdict(stock(conn))}")


# ---------- Thí nghiệm B: chết giữa chừng ----------

def split_txn(conn, event_id, crash):
    # autocommit: câu INSERT commit NGAY, tách khỏi phần nghiệp vụ phía sau
    cur = conn.execute("INSERT INTO demo_events VALUES (%s) ON CONFLICT DO NOTHING", (event_id,))
    if cur.rowcount == 0:
        return "bỏ qua (đã có dấu xử lý)"
    if crash:
        raise RuntimeError("tiến trình chết")
    conn.execute("UPDATE demo_stock SET stock = stock - 1 WHERE id = 1")
    return "đã xử lý"


def same_txn(conn, event_id, crash):
    with conn.transaction():
        cur = conn.execute("INSERT INTO demo_events VALUES (%s) ON CONFLICT DO NOTHING", (event_id,))
        if cur.rowcount == 0:
            return "bỏ qua (đã có dấu xử lý)"
        if crash:
            raise RuntimeError("tiến trình chết")  # -> ROLLBACK, dấu xử lý biến mất
        conn.execute("UPDATE demo_stock SET stock = stock - 1 WHERE id = 1")
        return "đã xử lý"


def experiment_b(conn):
    print("\n=== Thí nghiệm B: worker chết giữa chừng, broker giao lại ===")
    for name, worker in [("2 transaction riêng", split_txn), ("cùng 1 transaction ", same_txn)]:
        setup(conn)
        try:
            worker(conn, "order-2:created", crash=True)
        except RuntimeError:
            first = "chết giữa chừng"
        second = worker(conn, "order-2:created", crash=False)  # broker giao lại
        print(f"  {name}: lần 1 {first}; lần giao lại: {second} | "
              f"kho = {stock(conn)} (đúng phải là {EXPECTED}) -> {verdict(stock(conn))}")


if __name__ == "__main__":
    try:
        with new_conn() as conn:
            experiment_a(conn)
            experiment_b(conn)
            conn.execute("DROP TABLE IF EXISTS demo_events, demo_stock")
    except psycopg.OperationalError as e:
        raise SystemExit(f"[LỖI] Không kết nối được PostgreSQL: {e}")
    print("\nKết luận: idempotency cần (1) xí chỗ NGUYÊN TỬ bằng ràng buộc unique và "
          "(2) xí chỗ + nghiệp vụ trong CÙNG transaction.")