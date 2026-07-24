# SQLite-Tracking Phase 1 (Client) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Jeder Scanner erfasst den Ordnerbaum (Top-Level + 2 Ebenen, alle Ordner) in einem Plattendurchlauf und schreibt `disks` + `folder_tree` in eine lokale SQLite — additiv, Notion und Report-Struktur unverändert.

**Architecture:** Ein einziger Tiefen-Walk (`du -d` + `find`) je Top-Level-Ordner ersetzt den heutigen `du -sk`+`find`-Aufruf und liefert alle Knoten. `scan.py` schreibt die Projekt-/Kinder-Zahlen weiterhin identisch (aus dem depth-0/1-Knoten abgeleitet) und hängt zusätzlich einen flachen `folder_tree`-Abschnitt an den Report. `db.py` ingestet Report → lokale SQLite (idempotent, delete+insert je Datenträger).

**Tech Stack:** Python 3.13 (Stdlib `sqlite3`, `subprocess`), pytest (dev-only, nicht gebündelt).

## Global Constraints

- Notion-Pfad (`notion_sync`) und die **Report-JSON-Struktur** (Keys `scan_info`, `projects`, `unassigned` inkl. aller bestehenden Felder) bleiben **byte-kompatibel**; der neue Key `folder_tree` wird nur additiv ergänzt.
- Neue DB-Datei: `~/Library/Application Support/NXT Scanner/nxt_scanner.db`.
- Scanner speichern lokal **nur** `disks` + `folder_tree`. `projects`/`scans_log` werden hier NICHT gebaut (zentral, Phase 2).
- Alles best-effort/non-blocking: ein DB-Fehler darf Scan/Notion nie abbrechen.
- Nur Stdlib im Client (keine neue gebündelte Dependency).
- Dev-Python: `/Library/Frameworks/Python.framework/Versions/3.13/bin/python3` (im Plan `PY`).

---

### Task 1: Tiefen-Walk in `analyzer.py`

**Files:**
- Modify: `storage_scanner/analyzer.py`
- Test: `tests/test_analyzer_tree.py`

**Interfaces:**
- Produces: `walk_tree(root: Path, sub_depth: int = 2, timeout: int = 180) -> list[dict]` — Knoten `{"rel_path": str, "depth": int, "parent_rel_path": str|None, "size_bytes": int, "file_count": int, "mtime": str}`. `depth 0` = `root` selbst (`rel_path == ""`), `depth 1` = direkte Kinder usw. Größen & Dateizahlen sind **kumulativ** (ganzer Teilbaum). Leere Liste bei Timeout/Fehler.
- Produces: `is_network_volume(path: str) -> bool` — True bei Mount-Typ `smbfs`/`nfs`/`afpfs`/`webdav`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_analyzer_tree.py
import os, subprocess
from pathlib import Path
from storage_scanner.analyzer import walk_tree

def _mkfile(p: Path, kb: int):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as f:
        f.write(b"\0" * (kb * 1024))

def test_walk_tree_depths_and_cumulative(tmp_path):
    # top/ (depth0)  ->  top/a (depth1) -> top/a/x (depth2) -> top/a/x/deep (depth3, jenseits sub_depth)
    _mkfile(tmp_path / "top" / "root.bin", 100)
    _mkfile(tmp_path / "top" / "a" / "a.bin", 200)
    _mkfile(tmp_path / "top" / "a" / "x" / "x.bin", 300)
    _mkfile(tmp_path / "top" / "a" / "x" / "deep" / "d.bin", 400)

    nodes = walk_tree(tmp_path / "top", sub_depth=2)
    by = {n["rel_path"]: n for n in nodes}

    # Knotenmenge: "", "a", "a/x"  (deep liegt jenseits sub_depth=2 -> kein eigener Knoten)
    assert set(by) == {"", "a", "a/x"}
    assert by[""]["depth"] == 0 and by[""]["parent_rel_path"] is None
    assert by["a"]["depth"] == 1 and by["a"]["parent_rel_path"] == ""
    assert by["a/x"]["depth"] == 2 and by["a/x"]["parent_rel_path"] == "a"

    # kumulative Dateizahlen
    assert by[""]["file_count"] == 4          # root.bin + a.bin + x.bin + d.bin
    assert by["a"]["file_count"] == 3         # a.bin + x.bin + d.bin
    assert by["a/x"]["file_count"] == 2       # x.bin + d.bin (inkl. deep)

    # kumulative Größen (>= Summe der bekannten Nutzbytes, Blockgröße toleriert)
    assert by[""]["size_bytes"] >= (100+200+300+400) * 1024
    assert by["a/x"]["size_bytes"] >= (300+400) * 1024
    assert by[""]["size_bytes"] > by["a"]["size_bytes"] > by["a/x"]["size_bytes"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PY=/Library/Frameworks/Python.framework/Versions/3.13/bin/python3; $PY -m pytest tests/test_analyzer_tree.py -v`
Expected: FAIL (`ImportError: cannot import name 'walk_tree'`).

- [ ] **Step 3: Write minimal implementation**

Append to `storage_scanner/analyzer.py`:

```python
import subprocess
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def is_network_volume(path: str) -> bool:
    """True wenn der Mount ein Netzlaufwerk ist (smbfs/nfs/afpfs/webdav)."""
    try:
        r = subprocess.run(["stat", "-f", "%T", str(path)],
                           capture_output=True, text=True, timeout=10)
        # Fallback: mount-Tabelle prüfen
        mr = subprocess.run(["mount"], capture_output=True, text=True, timeout=10)
        for line in mr.stdout.splitlines():
            if f" on {path} " in line or line.endswith(f" on {path}"):
                return any(fs in line for fs in ("smbfs", "nfs", "afpfs", "webdav"))
    except (subprocess.TimeoutExpired, OSError):
        pass
    return False


def _safe_iso_mtime(p: str) -> str:
    try:
        return datetime.fromtimestamp(os.path.getmtime(p)).isoformat()
    except OSError:
        return datetime.now().isoformat()


def walk_tree(root, sub_depth: int = 2, timeout: int = 180) -> list[dict]:
    """Erfasst den Ordnerbaum ab root bis sub_depth Ebenen (kumulativ).

    depth 0 == root selbst (rel_path ""). Größen via 'du -d', Dateizahlen via
    'find'. Leere Liste bei Timeout/Fehler.
    """
    root = Path(root)
    root_str = os.path.normpath(str(root))

    # 1) Größen je Knoten bis sub_depth (BSD du: -d Tiefe, -k KiB)
    sizes = {}
    try:
        r = subprocess.run(["du", "-d", str(sub_depth), "-k", root_str],
                           capture_output=True, text=True, timeout=timeout)
        if r.stderr and "Invalid argument" in r.stderr:
            return []
        for line in r.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            kb, p = parts
            try:
                sizes[os.path.normpath(p)] = int(kb) * 1024
            except ValueError:
                continue
    except (subprocess.TimeoutExpired, OSError):
        return []

    if root_str not in sizes:
        return []

    # 2) Dateizahlen kumulativ je Knoten (find -> Vorfahren bis sub_depth zählen)
    counts = defaultdict(int)
    find_proc = None
    try:
        find_proc = subprocess.Popen(["find", root_str, "-type", "f"],
                                     stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        for bline in find_proc.stdout:
            fpath = os.path.normpath(bline.decode("utf-8", "replace").rstrip("\n"))
            d = os.path.dirname(fpath)
            rel = os.path.relpath(d, root_str)
            parts = [] if rel == "." else rel.split(os.sep)
            # Vorfahren-Knoten: root + progressive Prefixe bis sub_depth
            counts[root_str] += 1
            acc = root_str
            for seg in parts[:sub_depth]:
                acc = os.path.join(acc, seg)
                counts[acc] += 1
        find_proc.wait(timeout=timeout)
    except (subprocess.TimeoutExpired, OSError):
        if find_proc is not None:
            try:
                find_proc.kill()
            except OSError:
                pass

    # 3) Knoten bauen
    nodes = []
    for abspath, size in sizes.items():
        rel = os.path.relpath(abspath, root_str)
        rel = "" if rel == "." else rel.replace(os.sep, "/")
        depth = 0 if rel == "" else rel.count("/") + 1
        parent = None if depth == 0 else "/".join(rel.split("/")[:-1])
        nodes.append({
            "rel_path": rel,
            "depth": depth,
            "parent_rel_path": parent,
            "size_bytes": size,
            "file_count": counts.get(abspath, 0),
            "mtime": _safe_iso_mtime(abspath),
        })
    return nodes
```

(Falls `import os` oben in der Datei fehlt, ergänzen.)

- [ ] **Step 4: Run test to verify it passes**

Run: `$PY -m pytest tests/test_analyzer_tree.py -v`
Expected: PASS (3 assertions-Block grün).

- [ ] **Step 5: Commit**

```bash
git add storage_scanner/analyzer.py tests/test_analyzer_tree.py
git commit -m "feat(analyzer): walk_tree Tiefen-Erfassung + Netzlaufwerk-Erkennung"
```

---

### Task 2: `folder_tree` in den Report (`scan.py`)

**Files:**
- Modify: `storage_scanner/scan.py` (`run_scan` und `main`)
- Test: `tests/test_scan_tree.py`

**Interfaces:**
- Consumes: `analyzer.walk_tree`, `analyzer.is_network_volume`.
- Produces: Report-Dict enthält zusätzlich `report["folder_tree"]` = flache Liste von Knoten mit `disk`-relativem `rel_path` (Präfix = Top-Level-Ordnername). Bestehende Keys unverändert.

**Design:** Für jeden bereits aufgelösten Top-Level-Ordner (`subfolders`) wird `walk_tree(folder, sub_depth)` aufgerufen; die Knoten werden mit dem Ordnernamen als rel_path-Präfix in `folder_tree` gesammelt. Die bestehende `analyze_folder`-Logik für `projects`/`unassigned`/`children` bleibt unangetastet (Notion identisch). `sub_depth` = 0 bei Netzlaufwerk (nur Top-Level), sonst 2.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_scan_tree.py
from pathlib import Path
from storage_scanner.scan import run_scan
import json

def _mkfile(p: Path, kb: int):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as f:
        f.write(b"\0" * (kb * 1024))

def test_report_has_folder_tree(tmp_path):
    vol = tmp_path / "vol"
    _mkfile(vol / "2025-01-01_PROJ_FOOTAGE" / "sub" / "a.bin", 50)
    _mkfile(vol / "LOOSEFOLDER" / "b.bin", 30)
    out = tmp_path / "r.json"
    run_scan(str(vol), str(out))
    report = json.loads(out.read_text())

    assert "folder_tree" in report
    rels = {n["rel_path"] for n in report["folder_tree"]}
    # Top-Level beider Ordner vorhanden (auch der lose)
    assert "2025-01-01_PROJ_FOOTAGE" in rels
    assert "LOOSEFOLDER" in rels
    # Unterebene erfasst
    assert "2025-01-01_PROJ_FOOTAGE/sub" in rels
    # bestehende Struktur unangetastet
    assert "scan_info" in report and "projects" in report and "unassigned" in report
```

- [ ] **Step 2: Run test to verify it fails**

Run: `$PY -m pytest tests/test_scan_tree.py -v`
Expected: FAIL (`assert "folder_tree" in report`).

- [ ] **Step 3: Write minimal implementation**

In `scan.py` oben ergänzen: `from .analyzer import analyze_folder, walk_tree, is_network_volume`.

Neue Hilfsfunktion in `scan.py`:

```python
def _build_folder_tree(subfolders, scan_path) -> list[dict]:
    """Baut den flachen folder_tree (disk-relative rel_paths) aus den
    Top-Level-Ordnern. Netzlaufwerke: nur Top-Level (sub_depth=0)."""
    sub_depth = 0 if is_network_volume(str(scan_path)) else 2
    tree = []
    for folder in subfolders:
        for node in walk_tree(folder, sub_depth=sub_depth):
            rel = folder.name if node["rel_path"] == "" else f"{folder.name}/{node['rel_path']}"
            parent = (folder.name if node["parent_rel_path"] == "" else
                      f"{folder.name}/{node['parent_rel_path']}") if node["parent_rel_path"] is not None else None
            tree.append({
                "rel_path": rel,
                "depth": node["depth"] + 1,   # +1: Top-Level ist Ebene 1 unter Volume-Root
                "parent_rel_path": parent,
                "size_bytes": node["size_bytes"],
                "file_count": node["file_count"],
                "mtime": node["mtime"],
            })
    return tree
```

In `run_scan` und `main`, direkt vor `report = generate_report(...)`, ergänzen:

```python
    report = generate_report(str(scan_path), projects, unassigned)
    report["folder_tree"] = _build_folder_tree(subfolders, scan_path)
    save_report(report, out)   # bzw. output_path in main()
```

(Die bestehende `save_report`-Zeile ersetzt durch die zwei Zeilen; in `main` heißt die Zielvariable `output_path`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `$PY -m pytest tests/test_scan_tree.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add storage_scanner/scan.py tests/test_scan_tree.py
git commit -m "feat(scan): folder_tree-Abschnitt additiv in den Report"
```

---

### Task 3: DB-Pfad in `paths.py`

**Files:**
- Modify: `storage_scanner/paths.py`

**Interfaces:**
- Produces: `DB_PATH = DATA_DIR / "nxt_scanner.db"`.

- [ ] **Step 1: Implementierung**

In `paths.py` nach `KNOWN_VOLUMES_PATH` ergänzen:

```python
DB_PATH = DATA_DIR / "nxt_scanner.db"
```

- [ ] **Step 2: Verify Import**

Run: `$PY -c "from storage_scanner.paths import DB_PATH; print(DB_PATH)"`
Expected: Pfad endet auf `NXT Scanner/nxt_scanner.db`.

- [ ] **Step 3: Commit**

```bash
git add storage_scanner/paths.py
git commit -m "feat(paths): DB_PATH fuer lokale SQLite"
```

---

### Task 4: Lokale SQLite (`db.py`)

**Files:**
- Create: `storage_scanner/db.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Produces: `connect(db_path=DB_PATH) -> sqlite3.Connection` (legt Schema an, `PRAGMA foreign_keys`, WAL).
- Produces: `ingest_report(report: dict, conn) -> dict` — upsert `disks`, delete+insert `folder_tree` je `disk_uuid`; gibt Push-Payload `{"disk": {...}, "folder_tree": [...]}` zurück.
- Produces: `rebuild_from_reports(reports_dir, conn) -> int` — ingestet alle `*_report.json`, gibt Zahl der Datenträger zurück.

**Design:** `disk_uuid` = `scan_info.volume_uuid`; fehlt sie, Fallback = `"name:" + Ordnername des Reports` (stabil pro Report). `disks`-Felder `fs_type`/`is_network`/`capacity_bytes` optional (None wenn nicht ermittelbar); `used_bytes` = Summe der depth-1-Knoten; `last_scan` = `scan_info.scan_date`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_db.py
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

def test_second_disk_isolated():
    conn = db.connect(":memory:")
    db.ingest_report(_report("U1", "NXT 001"), conn)
    db.ingest_report(_report("U2", "NXT 002"), conn)
    assert conn.execute("SELECT COUNT(*) FROM disks").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM folder_tree").fetchone()[0] == 6
```

- [ ] **Step 2: Run test to verify it fails**

Run: `$PY -m pytest tests/test_db.py -v`
Expected: FAIL (`ModuleNotFoundError`/`AttributeError`).

- [ ] **Step 3: Write minimal implementation**

```python
# storage_scanner/db.py
"""Lokale SQLite: disks + folder_tree (nur Rohstruktur, additiv zu Notion)."""

import json
import sqlite3
from pathlib import Path

from .paths import DB_PATH

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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `$PY -m pytest tests/test_db.py -v`
Expected: PASS (beide Tests grün).

- [ ] **Step 5: Commit**

```bash
git add storage_scanner/db.py tests/test_db.py
git commit -m "feat(db): lokale SQLite mit disks + folder_tree (idempotenter Ingest)"
```

---

### Task 5: Hook in den Scan-Flow (`auto_scan.py`, `menubar.py`)

**Files:**
- Modify: `storage_scanner/auto_scan.py` (`scan_and_sync`)
- Modify: `storage_scanner/menubar.py` (`_do_scan`)
- Test: `tests/test_ingest_hook.py`

**Interfaces:**
- Produces: `db_ingest_safe(report_path: str, user_name: str = "") -> bool` in `db.py` — liest Report-Datei, ingestet in lokale SQLite (`connect()`), schluckt jeden Fehler (loggt via `logging`), gibt Erfolg zurück.

**Design:** Nach dem bestehenden `run_sync(...)` wird `db.db_ingest_safe(report_path, user_name)` aufgerufen. Notion bleibt zuerst und unberührt; ein DB-Fehler wird nur geloggt.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_ingest_hook.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `$PY -m pytest tests/test_ingest_hook.py -v`
Expected: FAIL (`AttributeError: db_ingest_safe`).

- [ ] **Step 3: Write minimal implementation**

In `db.py` ergänzen:

```python
import logging

_log = logging.getLogger("db")


def db_ingest_safe(report_path: str, user_name: str = "") -> bool:
    """Best-effort: Report-Datei in lokale SQLite ingesten. Schluckt Fehler."""
    try:
        report = json.loads(Path(report_path).read_text())
        conn = connect()
        try:
            ingest_report(report, conn, user_name=user_name)
        finally:
            conn.close()
        return True
    except Exception as e:  # noqa: BLE001 - non-blocking
        _log.error("DB-Ingest fehlgeschlagen fuer %s: %s", report_path, e)
        return False
```

In `auto_scan.scan_and_sync`, nach `run_sync(str(report_path))`:

```python
        run_sync(str(report_path))
        log.info(f"Notion-Sync abgeschlossen: {volume_name}")

        from .db import db_ingest_safe
        if db_ingest_safe(str(report_path)):
            log.info(f"DB-Ingest abgeschlossen: {volume_name}")
        return True
```

In `menubar._do_scan`, unmittelbar nach dem bestehenden `notion_sync.run_sync(...)`:

```python
        from .db import db_ingest_safe
        db_ingest_safe(report_path, user_name=self._user_name)
```

(Exakte Variablennamen `report_path`/`self._user_name` an `_do_scan` anpassen.)

- [ ] **Step 4: Run test to verify it passes**

Run: `$PY -m pytest tests/test_ingest_hook.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add storage_scanner/db.py storage_scanner/auto_scan.py storage_scanner/menubar.py tests/test_ingest_hook.py
git commit -m "feat: DB-Ingest additiv nach Notion-Sync (best-effort)"
```

---

### Task 6: Backfill-CLI + PyInstaller-hiddenimport

**Files:**
- Modify: `storage_scanner/db.py` (`__main__`-Block)
- Modify: `nxt_scanner.spec` (`hiddenimports`: `storage_scanner.db`)
- Test: manuell gegen die echten 62 Reports

**Interfaces:**
- Produces: `python -m storage_scanner.db --rebuild` baut die lokale DB aus `REPORTS_DIR` neu und meldet die Datenträgerzahl.

- [ ] **Step 1: Implementierung CLI**

Am Ende von `db.py`:

```python
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
```

- [ ] **Step 2: hiddenimport ergänzen**

In `nxt_scanner.spec` in der `hiddenimports`-Liste `'storage_scanner.db',` ergänzen.

- [ ] **Step 3: Backfill gegen echte Reports (Verifikation)**

Run: `$PY -m storage_scanner.db --rebuild`
Expected: `Rebuild abgeschlossen: N Datenträger …` (N ~ 60). Danach:
`$PY -c "import sqlite3; from storage_scanner.paths import DB_PATH; c=sqlite3.connect(str(DB_PATH)); print('disks', c.execute('SELECT COUNT(*) FROM disks').fetchone()[0]); print('nodes', c.execute('SELECT COUNT(*) FROM folder_tree').fetchone()[0])"`
Expected: `disks` ~60, `nodes` > disks (Alt-Reports haben teils nur 1 Ebene — volle Tiefe kommt beim nächsten Live-Scan).

- [ ] **Step 4: Commit**

```bash
git add storage_scanner/db.py nxt_scanner.spec
git commit -m "feat(db): --rebuild Backfill-CLI + PyInstaller hiddenimport"
```

---

## Manuelle Abnahme (gemeinsam, gegen echte SSDs)

Nach Task 6, mit angebundenen SSDs an diesem Mac:

1. `$PY -c "from storage_scanner.scan import run_scan; run_scan('/Volumes/<SSD>', '/tmp/ssd_report.json')"` — läuft der Tiefen-Scan durch, `folder_tree` gefüllt?
2. `$PY -c "from storage_scanner.db import db_ingest_safe; print(db_ingest_safe('/tmp/ssd_report.json'))"` — True?
3. DB inspizieren: Top-Level- und 2-Ebenen-Knoten für die SSD vorhanden, Größen plausibel (Abgleich mit Finder)?
4. Report-Diff: `scan_info/projects/unassigned` identisch zum bisherigen Format (keine Regression im Notion-relevanten Teil)?

## Self-Review

- **Spec-Abdeckung:** Erfassungstiefe + Netz-Drosselung (T1/T2), lokale SQLite disks+folder_tree (T3/T4), non-blocking Hook (T5), Backfill (T6). Cloud (D1/Worker/derive/Config) = bewusst Phase 2, nicht hier.
- **Platzhalter:** keine — jeder Step hat konkreten Code/Command.
- **Typkonsistenz:** `walk_tree`-Knoten-Keys identisch in T1/T2/T4; `folder_tree`-depth in T2 auf Volume-Root normiert (+1), in T4 als `used_bytes = Σ depth==1` konsistent genutzt; `db_ingest_safe`-Signatur identisch in T5.
