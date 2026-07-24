import json
from storage_scanner import db


def test_db_ingest_safe_writes(tmp_path, monkeypatch):
    dbfile = tmp_path / "t.db"
    monkeypatch.setattr(db, "DB_PATH", dbfile)
    report = {"scan_info": {"scanned_path": "/Volumes/NXT 009", "scan_date": "2026-07-24T00:00:00+02:00", "volume_uuid": "U9"},
              "projects": [], "unassigned": [],
              "folder_tree": [{"rel_path": "P", "depth": 1, "parent_rel_path": None,
                                "size_bytes": 10, "file_count": 1, "mtime": "x"}]}
    rp = tmp_path / "NXT_009_report.json"
    rp.write_text(json.dumps(report))
    assert db.db_ingest_safe(str(rp)) is True

    conn = db.connect(dbfile)
    assert conn.execute("SELECT COUNT(*) FROM disks WHERE uuid='U9'").fetchone()[0] == 1


def test_db_ingest_safe_swallows_errors(tmp_path):
    assert db.db_ingest_safe(str(tmp_path / "missing.json")) is False
