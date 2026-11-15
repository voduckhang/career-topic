"""Hồi quy cho db_report.py: báo cáo bất biến tồn kho và lệnh --reset."""
import pytest

import db
import db_report
import ledger
import order_processor as OP
from tests.conftest import make_order

pytestmark = pytest.mark.db


def run_report(monkeypatch, *args):
    monkeypatch.setattr("sys.argv", ["db_report.py", *args])
    return db_report.main()


def test_clean_database_has_exit_code_zero(clean_db, deliver, monkeypatch, capsys):
    deliver(OP.handle, "created", make_order("A1", qty=2))
    assert run_report(monkeypatch) == 0
    assert "Bất biến tồn kho đúng" in capsys.readouterr().out


def test_tampered_inventory_has_exit_code_one(clean_db, deliver, monkeypatch, capsys):
    deliver(OP.handle, "created", make_order("A1", qty=2))
    with db.connect() as conn:
        conn.execute("UPDATE inventory SET stock = stock + 3 WHERE product = 'Balo'")
    assert run_report(monkeypatch) == 1
    assert "LỆCH +3" in capsys.readouterr().out


def test_reset_clears_database_and_ledger_together(clean_db, deliver, monkeypatch):
    deliver(OP.handle, "created", make_order("A1", qty=2))
    ledger.record_sent("A1", "created", 0, "normal", True, None)
    assert run_report(monkeypatch, "--reset") == 0
    with db.connect() as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM processed_events").fetchone()[0] == 0
        assert db.inventory_mismatches(db.inventory_rows(conn)) == []
    assert ledger.load() == ({}, 0)  # nếu sổ còn sót, reconcile sẽ báo mất oan