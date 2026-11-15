"""Báo cáo nhanh trạng thái DB và kiểm tra bất biến tồn kho (reconciliation thu nhỏ).

    python db_report.py          # xem báo cáo
    python db_report.py --reset  # xóa sạch dữ liệu demo VÀ sổ cái của publisher

Bất biến: với mỗi sản phẩm,
    tồn kho hiện tại = tồn kho ban đầu - tổng số lượng các đơn đang 'created'
Nếu một sự kiện bị xử lý 2 lần hoặc xử lý nửa vời, bất biến này lệch ngay.
Thoát với mã 1 khi có lệch, nên dùng được trong test hồi quy.

Muốn đối chiếu đầy đủ với publisher và broker thì dùng reconcile.py.
"""
import sys

import db
import ledger


def main() -> int:
    if "--reset" in sys.argv:
        db.reset_data()
        ledger.reset()  # DB và sổ cái phải reset CÙNG NHAU, nếu không reconcile sẽ báo mất oan
        print("[OK] Đã reset dữ liệu, tồn kho và sổ cái publisher.")
        print("     Nhớ dọn cả message còn sót trong orders-queue / orders-dmq nếu có.")
        return 0

    db.init_schema()
    with db.connect() as conn:
        orders = conn.execute(
            "SELECT status, count(*) FROM orders GROUP BY status ORDER BY status").fetchall()
        events = conn.execute(
            "SELECT event, count(*), coalesce(sum(dup_count), 0) FROM processed_events "
            "GROUP BY event ORDER BY event").fetchall()
        stock_rows = db.inventory_rows(conn)

    print("Đơn hàng      :", ", ".join(f"{s}={n}" for s, n in orders) or "(trống)")
    print("Sự kiện đã xử lý (bản trùng bị chặn):",
          ", ".join(f"{e}={n} ({int(d)})" for e, n, d in events) or "(trống)")
    print(f"\n{'Sản phẩm':<16}{'Ban đầu':>9}{'Đang giữ':>10}{'Kỳ vọng':>9}{'Thực tế':>9}  Kết quả")
    bad = 0
    for product, initial, actual, active in stock_rows:
        expected = initial - active
        ok = actual == expected
        bad += not ok
        print(f"{product:<16}{initial:>9}{active:>10}{expected:>9}{actual:>9}  "
              f"{'OK' if ok else f'LỆCH {actual - expected:+d}'}")
    print("\n" + ("Bất biến tồn kho đúng cho mọi sản phẩm." if not bad
                  else f"CẢNH BÁO: {bad} sản phẩm lệch, có sự kiện bị xử lý sai."))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())