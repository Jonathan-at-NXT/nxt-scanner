from storage_scanner import db


def _report(uuid="U1", name="NXT 001"):
    return {
        "scan_info": {"scanned_path": f"/Volumes/{name}", "scan_date": "2026-07-24T08:00:00+02:00",
                      "volume_uuid": uuid, "valid_folders": 1, "unassigned_folders": 1},
        "projects": [], "unassigned": [],
        "folder_tree": [
            {"rel_path": "A", "depth": 1, "parent_rel_path": None, "size_bytes": 1000, "file_count": 2, "mtime": "2026-01-01T00:00:00"},
            {"rel_path": "A/x", "depth": 2, "parent_rel_path": "A", "size_bytes": 400, "file_count": 1, "mtime": "2026-01-01T00:00:00"},
            {"rel_path": "B", "depth": 1, "parent_rel_path": None, "size_bytes": 500, "file_count": 1, "mtime": "2026-01-01T00:00:00"},
        ],
    }


def test_ingest_and_idempotent():
    conn = db.connect(":memory:")
    payload = db.ingest_report(_report(), conn)
    assert payload["disk"]["uuid"] == "U1"
    assert len(payload["folder_tree"]) == 3

    n_disks = conn.execute("SELECT COUNT(*) FROM disks").fetchone()[0]
    n_nodes = conn.execute("SELECT COUNT(*) FROM folder_tree").fetchone()[0]
    assert n_disks == 1 and n_nodes == 3
    # used_bytes = Summe depth-1 (A + B = 1500)
    assert conn.execute("SELECT used_bytes FROM disks WHERE uuid='U1'").fetchone()[0] == 1500

    # zweiter Ingest -> keine Duplikate (delete+insert)
    db.ingest_report(_report(), conn)
    assert conn.execute("SELECT COUNT(*) FROM folder_tree").fetchone()[0] == 3
    assert conn.execute("SELECT COUNT(*) FROM disks").fetchone()[0] == 1


def test_backfill_synthesizes_tree_from_projects_when_missing():
    # Alt-Report ohne folder_tree, aber mit projects (inkl. children) + unassigned
    old = {
        "scan_info": {"scanned_path": "/Volumes/OLD 1", "scan_date": "2026-03-01T00:00:00+02:00", "volume_uuid": "OLD"},
        "projects": [
            {"name": "2025-01-01_P", "size_bytes": 900, "file_count": 3, "last_modified": "2025-01-01T00:00:00",
             "type": "PROJECT",
             "children": [
                 {"name": "2025-01-01_P_FOOTAGE", "size_bytes": 800, "file_count": 2, "last_modified": "2025-01-01T00:00:00"},
             ]},
        ],
        "unassigned": [
            {"name": "LOOSE", "size_bytes": 100, "file_count": 1, "last_modified": "2025-01-01T00:00:00"},
        ],
    }
    conn = db.connect(":memory:")
    db.ingest_report(old, conn)
    rels = dict(conn.execute("SELECT rel_path, depth FROM folder_tree WHERE disk_uuid='OLD'").fetchall())
    assert rels == {"2025-01-01_P": 1, "2025-01-01_P/2025-01-01_P_FOOTAGE": 2, "LOOSE": 1}
    # used_bytes = Summe depth-1 (900 + 100)
    assert conn.execute("SELECT used_bytes FROM disks WHERE uuid='OLD'").fetchone()[0] == 1000


def test_second_disk_isolated():
    conn = db.connect(":memory:")
    db.ingest_report(_report("U1", "NXT 001"), conn)
    db.ingest_report(_report("U2", "NXT 002"), conn)
    assert conn.execute("SELECT COUNT(*) FROM disks").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM folder_tree").fetchone()[0] == 6
