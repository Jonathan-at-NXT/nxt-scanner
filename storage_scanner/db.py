"""Lokale SQLite: disks + folder_tree (nur Rohstruktur, additiv zu Notion).

Die Scanner speichern bewusst NUR die Rohstruktur. Klassifikation (projects)
und Trend (scans_log) werden zentral abgeleitet (Phase 2).
"""

import json
import logging
import sqlite3
from pathlib import Path

from .paths import DB_PATH

_log = logging.getLogger("db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS disks (
    uuid TEXT PRIMARY KEY, name TEXT, fs_type TEXT, is_network INTEGER,
    used_bytes INTEGER, capacity_bytes INTEGER, last_scan TEXT, last_user TEXT
);
CREATE TABLE IF NOT EXISTS folder_tree (
    id INTEGER PRIMARY KEY, disk_uuid TEXT, rel_path TEXT, depth INTEGER,
    parent_rel_path TEXT, size_bytes INTEGER, file_count INTEGER, mtime TEXT,
    UNIQUE(disk_uuid, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_tree_disk_depth ON folder_tree(disk_uuid, depth);
"""


def connect(db_path=DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA)
    return conn


def _disk_uuid(report: dict) -> str:
    si = report.get("scan_info", {})
    uuid = (si.get("volume_uuid") or "").strip()
    if uuid:
        return uuid
    name = Path(si.get("scanned_path", "unknown")).name
    return f"name:{name}"


def ingest_report(report: dict, conn: sqlite3.Connection, user_name: str = "") -> dict:
    """Upsert disks + delete/insert folder_tree je disk_uuid. Gibt Push-Payload zurück."""
    si = report.get("scan_info", {})
    uuid = _disk_uuid(report)
    tree = report.get("folder_tree", [])
    name = Path(si.get("scanned_path", "")).name
    used = sum(n["size_bytes"] for n in tree if n.get("depth") == 1)

    disk_row = {
        "uuid": uuid, "name": name, "fs_type": None, "is_network": None,
        "used_bytes": used, "capacity_bytes": None,
        "last_scan": si.get("scan_date"), "last_user": user_name or None,
    }

    with conn:
        conn.execute(
            """INSERT INTO disks(uuid,name,fs_type,is_network,used_bytes,capacity_bytes,last_scan,last_user)
               VALUES(:uuid,:name,:fs_type,:is_network,:used_bytes,:capacity_bytes,:last_scan,:last_user)
               ON CONFLICT(uuid) DO UPDATE SET
                 name=excluded.name, used_bytes=excluded.used_bytes,
                 last_scan=excluded.last_scan, last_user=excluded.last_user""",
            disk_row,
        )
        conn.execute("DELETE FROM folder_tree WHERE disk_uuid=?", (uuid,))
        conn.executemany(
            """INSERT INTO folder_tree(disk_uuid,rel_path,depth,parent_rel_path,size_bytes,file_count,mtime)
               VALUES(?,?,?,?,?,?,?)""",
            [(uuid, n["rel_path"], n["depth"], n.get("parent_rel_path"),
              n["size_bytes"], n["file_count"], n.get("mtime")) for n in tree],
        )
    return {"disk": disk_row, "folder_tree": tree}


def rebuild_from_reports(reports_dir, conn: sqlite3.Connection) -> int:
    """Ingestet alle *_report.json aus reports_dir. Gibt Zahl der Datenträger zurück."""
    reports_dir = Path(reports_dir)
    seen = set()
    for f in sorted(reports_dir.glob("*_report.json")):
        try:
            report = json.loads(f.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        payload = ingest_report(report, conn)
        seen.add(payload["disk"]["uuid"])
    return len(seen)


def db_ingest_safe(report_path: str, user_name: str = "") -> bool:
    """Best-effort: Report-Datei in lokale SQLite ingesten. Schluckt jeden Fehler."""
    try:
        report = json.loads(Path(report_path).read_text())
        conn = connect()
        try:
            ingest_report(report, conn, user_name=user_name)
        finally:
            conn.close()
        return True
    except Exception as e:  # noqa: BLE001 - non-blocking, darf Scan/Notion nie stören
        _log.error("DB-Ingest fehlgeschlagen fuer %s: %s", report_path, e)
        return False


if __name__ == "__main__":
    import argparse
    from .paths import REPORTS_DIR

    ap = argparse.ArgumentParser(description="NXT Scanner lokale DB")
    ap.add_argument("--rebuild", action="store_true", help="DB aus reports/ neu aufbauen")
    args = ap.parse_args()
    if args.rebuild:
        conn = connect()
        n = rebuild_from_reports(REPORTS_DIR, conn)
        conn.close()
        print(f"Rebuild abgeschlossen: {n} Datenträger aus Reports importiert.")
