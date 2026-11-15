"""Tầng PostgreSQL cho idempotency.

Ý tưởng cốt lõi: bảng processed_events có khóa chính (order_id, event). Worker "xí chỗ" (claim)
sự kiện bằng INSERT ... ON CONFLICT DO NOTHING TRONG CÙNG TRANSACTION với nghiệp vụ:
  - claim thành công (rowcount = 1) -> làm nghiệp vụ -> COMMIT: cả hai cùng có hiệu lực
  - claim thất bại (rowcount = 0)   -> sự kiện đã xử lý rồi -> bỏ qua, vẫn ACCEPTED
  - lỗi giữa chừng                  -> ROLLBACK: claim biến mất, lần giao lại sẽ làm lại từ đầu

Biến môi trường: DATABASE_URL (mặc định khớp lệnh docker run trong hướng dẫn).
"""
import os
from contextlib import contextmanager

import psycopg
from psycopg import errors as pgerr

from errors import PermanentError, TransientError

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://solace:solace@localhost:5432/solace_demo")

PRODUCTS = ["Áo thun", "Giày thể thao", "Balo", "Tai nghe"]  # khớp PRODUCTS của publisher
INITIAL_STOCK = 10_000

SCHEMA = """
CREATE TABLE IF NOT EXISTS inventory (
    product       text PRIMARY KEY,
    initial_stock int  NOT NULL,
    stock         int  NOT NULL CHECK (stock >= 0)   -- bất biến: không bao giờ âm
);
CREATE TABLE IF NOT EXISTS orders (
    order_id   text PRIMARY KEY,
    customer   text,
    product    text NOT NULL REFERENCES inventory(product),
    quantity   int  NOT NULL CHECK (quantity > 0),
    total      bigint,
    status     text NOT NULL,                        -- 'created' | 'cancelled'
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS processed_events (
    order_id     text NOT NULL,
    event        text NOT NULL,
    worker       text,
    processed_at timestamptz NOT NULL DEFAULT now(),
    dup_count    int  NOT NULL DEFAULT 0,            -- số lần bản trùng bị chặn
    PRIMARY KEY (order_id, event)                    -- chính là "chìa khóa" idempotency
);
"""

_conn = None


def connect() -> psycopg.Connection:
    # autocommit=True: ta tự mở transaction rõ ràng bằng conn.transaction()
    # statement_timeout / lock_timeout: không để câu lệnh treo vô hạn khi DB nghẽn hoặc khóa chéo
    return psycopg.connect(
        DATABASE_URL, autocommit=True, connect_timeout=5,
        options="-c statement_timeout=5000 -c lock_timeout=3000",
    )


def get_conn() -> psycopg.Connection:
    global _conn
    if _conn is None or _conn.closed:
        _conn = connect()
    return _conn


def reset_conn() -> None:
    global _conn
    try:
        if _conn is not None:
            _conn.close()
    except Exception:
        pass
    _conn = None


def init_schema() -> None:
    """Tạo bảng (nếu chưa có) và nạp tồn kho ban đầu. Chạy lại nhiều lần vẫn an toàn."""
    with connect() as conn:
        conn.execute(SCHEMA)
        for p in PRODUCTS:
            conn.execute(
                "INSERT INTO inventory (product, initial_stock, stock) VALUES (%s, %s, %s) "
                "ON CONFLICT (product) DO NOTHING", (p, INITIAL_STOCK, INITIAL_STOCK))


def reset_data() -> None:
    """Xóa sạch dữ liệu demo và nạp lại tồn kho ban đầu."""
    init_schema()
    with connect() as conn:
        conn.execute("TRUNCATE processed_events, orders")
        conn.execute("UPDATE inventory SET stock = initial_stock")


@contextmanager
def transaction():
    """BEGIN ... COMMIT (hoặc ROLLBACK nếu có lỗi), kèm dịch lỗi Postgres sang quy ước của ta:
        deadlock / lock timeout / statement timeout / mất kết nối -> TransientError (retry được)
        dữ liệu vi phạm ràng buộc (CHECK, FK, kiểu dữ liệu)       -> PermanentError (retry vô ích)
    """
    try:
        conn = get_conn()
        with conn.transaction():
            with conn.cursor() as cur:
                yield cur
    except (pgerr.DeadlockDetected, pgerr.SerializationFailure,
            pgerr.LockNotAvailable, pgerr.QueryCanceled) as e:
        raise TransientError(f"DB tạm thời không xử lý được ({type(e).__name__})") from e
    except (psycopg.OperationalError, psycopg.InterfaceError) as e:
        reset_conn()  # kết nối có thể đã hỏng; lần giao lại sẽ mở kết nối mới
        raise TransientError(f"Mất kết nối DB ({type(e).__name__}: {e})") from e
    except (pgerr.IntegrityError, pgerr.DataError) as e:
        raise PermanentError(f"DB từ chối dữ liệu ({type(e).__name__}: {e})") from e


def claim_event(cur, order_id: str, event: str, worker: str) -> bool:
    """Xí chỗ sự kiện. True = lần đầu (cứ làm nghiệp vụ), False = bản trùng (bỏ qua).

    ON CONFLICT DO NOTHING là một thao tác nguyên tử: nếu hai worker claim cùng lúc, worker
    thứ hai CHỜ transaction của worker thứ nhất kết thúc. Thứ nhất COMMIT -> thứ hai nhận
    rowcount 0 (trùng). Thứ nhất ROLLBACK -> thứ hai claim được và làm thay.
    """
    cur.execute(
        "INSERT INTO processed_events (order_id, event, worker) VALUES (%s, %s, %s) "
        "ON CONFLICT (order_id, event) DO NOTHING", (order_id, event, worker))
    if cur.rowcount == 1:
        return True
    cur.execute("UPDATE processed_events SET dup_count = dup_count + 1 "
                "WHERE order_id = %s AND event = %s", (order_id, event))
    return False


# ---------- Các hàm chỉ ĐỌC, dùng cho db_report.py và reconcile.py ----------

def processed_events(conn) -> dict[str, int]:
    """{ "order_id:event": dup_count } cho mọi sự kiện đã xử lý."""
    rows = conn.execute("SELECT order_id, event, dup_count FROM processed_events").fetchall()
    return {f"{oid}:{ev}": dup for oid, ev, dup in rows}


def inventory_rows(conn) -> list[tuple]:
    """(product, tồn ban đầu, tồn hiện tại, tổng số lượng các đơn đang 'created')."""
    rows = conn.execute("""
        SELECT i.product, i.initial_stock, i.stock,
               coalesce(sum(o.quantity) FILTER (WHERE o.status = 'created'), 0)
        FROM inventory i LEFT JOIN orders o ON o.product = i.product
        GROUP BY i.product, i.initial_stock, i.stock ORDER BY i.product""").fetchall()
    return [(p, initial, actual, int(active)) for p, initial, actual, active in rows]


def inventory_mismatches(rows: list[tuple]) -> list[tuple]:
    """Bất biến: tồn hiện tại = tồn ban đầu - tổng số lượng đơn đang hoạt động.
    Trả về [(product, kỳ vọng, thực tế)] cho các sản phẩm bị lệch."""
    return [(p, initial - active, actual) for p, initial, actual, active in rows
            if actual != initial - active]


def orphan_orders(conn) -> list[str]:
    """Đơn có trong bảng orders nhưng không có dấu xử lý tương ứng: dấu hiệu ghi dữ liệu
    lách qua cơ chế claim (hoặc transaction bị cắt nửa chừng)."""
    rows = conn.execute("""
        SELECT o.order_id FROM orders o
        WHERE NOT EXISTS (SELECT 1 FROM processed_events p
                          WHERE p.order_id = o.order_id AND p.event = 'created')
           OR (o.status = 'cancelled' AND NOT EXISTS (
                  SELECT 1 FROM processed_events p
                  WHERE p.order_id = o.order_id AND p.event = 'cancelled'))
        ORDER BY o.order_id""").fetchall()
    return [r[0] for r in rows]