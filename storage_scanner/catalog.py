"""Datei-Katalog: jede Datei jedes Datenträgers (lokal, eigene SQLite).

Ergänzt den Ordner-Scan um die Datei-Ebene, die D1 bewusst nicht hat:
Pfad, Größe, Zeiten, Inode, Medien-Art und (in einem zweiten, langsamen
Durchlauf) den blake2b-Fingerprint aus ingest.verify. Damit lassen sich
Kopien dateigenau vergleichen, auch wenn die Platten nicht mehr gemountet sind.

Zwei Durchläufe:
- inventory: nur stat(), schnell, liest keine Dateiinhalte. Team-tauglich.
- hash: liest Samples (bis 12 MiB/Datei), resumable, mit Zeitbudget.
  Gedacht für Sicherungs-/Agent-Macs, nicht für Edit-Workstations.

Quell-Datenträger werden nur gelesen. Opt-in per Config `file_catalog: true`.
Jede Zeile trägt `changed_at`, damit ein späterer Sync nach D1 nur Änderungen
schicken muss.
"""

import argparse
import json
import logging
import os
import plistlib
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path

from .paths import DATA_DIR

_log = logging.getLogger("catalog")

CATALOG_DB_PATH = DATA_DIR / "file_catalog.db"
FP_ALGO = "b2s4m"  # blake2b-128 über Größe + head/mid/tail à 4 MiB (ingest.verify)

SCHEMA = """
CREATE TABLE IF NOT EXISTS volume (
    uuid TEXT PRIMARY KEY, name TEXT, fs_type TEXT,
    capacity_bytes INTEGER, free_bytes INTEGER,
    device_id TEXT, whole_disk TEXT, physical_store TEXT,
    media_name TEXT, bus_protocol TEXT, solid_state INTEGER, device_serial TEXT,
    first_seen TEXT, last_seen TEXT, last_inventory TEXT, last_hash_pass TEXT
);
CREATE TABLE IF NOT EXISTS file (
    id INTEGER PRIMARY KEY,
    volume_uuid TEXT NOT NULL, rel_path TEXT NOT NULL,
    name TEXT, ext TEXT, kind TEXT,
    size INTEGER, mtime REAL, birthtime REAL, inode INTEGER,
    fingerprint TEXT, fp_algo TEXT, hashed_at TEXT,
    present INTEGER DEFAULT 1,
    first_seen TEXT, last_seen TEXT, gone_at TEXT, changed_at TEXT,
    UNIQUE(volume_uuid, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_file_fp ON file(fingerprint);
CREATE INDEX IF NOT EXISTS idx_file_name_size ON file(name, size);
CREATE INDEX IF NOT EXISTS idx_file_vol_present ON file(volume_uuid, present);
CREATE INDEX IF NOT EXISTS idx_file_changed ON file(changed_at);
CREATE TABLE IF NOT EXISTS run_error (
    run_started TEXT, volume_uuid TEXT, path TEXT, message TEXT
);
CREATE TABLE IF NOT EXISTS run (
    id INTEGER PRIMARY KEY, volume_uuid TEXT, kind TEXT,
    started_at TEXT, finished_at TEXT, host TEXT,
    files INTEGER, bytes INTEGER, added INTEGER, changed INTEGER,
    removed INTEGER, hashed INTEGER, errors INTEGER, complete INTEGER
);
"""

# macOS-Systemordner mit Varianten-Namen (z. B. ".DocumentRevisions-V100-bad-1")
_SKIP_PREFIXES = (".DocumentRevisions-V100", ".Spotlight-V100", ".fseventsd", ".Trashes", ".TemporaryItems", ".MobileBackups")

# Systemordner/-dateien, die nie Footage sind
_SKIP_DIRS = {
    ".Spotlight-V100", ".fseventsd", ".Trashes", ".TemporaryItems",
    ".DocumentRevisions-V100", ".DS_Store", "$RECYCLE.BIN",
    "System Volume Information", ".PKInstallSandboxManager",
}

_KINDS = {
    "video": {"mp4", "mov", "mxf", "mts", "m2ts", "avi", "mkv", "braw", "r3d",
              "insv", "crm", "m4v", "mpg", "mpeg", "webm", "360", "ari"},
    "proxy": {"lrv", "lrf"},
    "photo": {"jpg", "jpeg", "heic", "heif", "png", "tif", "tiff", "dng", "arw",
              "cr2", "cr3", "nef", "raf", "orf", "rw2", "insp", "gpr", "psd", "webp"},
    "audio": {"wav", "mp3", "aac", "m4a", "aif", "aiff", "flac", "bwf", "ogg"},
    "project": {"drp", "drt", "prproj", "aep", "fcpxml", "otio", "edl", "aaf"},
    "sidecar": {"xml", "xmp", "thm", "srt", "cpi", "bim", "smi", "lut", "cube"},
}
_EXT_KIND = {ext: kind for kind, exts in _KINDS.items() for ext in exts}


def classify(name: str) -> tuple[str, str]:
    """(ext, kind) aus dem Dateinamen."""
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    return ext, _EXT_KIND.get(ext, "other")


def connect(db_path=None) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path or CATALOG_DB_PATH), timeout=120)  # App + CLI parallel
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    return conn


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


# ── Datenträger-Identität ─────────────────────────────────────────

def _diskutil(target: str) -> dict:
    try:
        r = subprocess.run(["diskutil", "info", "-plist", target],
                           capture_output=True, timeout=30)
        if r.returncode == 0:
            return plistlib.loads(r.stdout)
    except (subprocess.TimeoutExpired, OSError, plistlib.InvalidFileException):
        pass
    return {}


# Platzhalter, die billige USB-SATA-Bridges statt einer echten Nummer melden
_BOGUS_SERIALS = {"0123456789ABCDEF", "123456789ABC", "000000000000", "NOT PROVIDED",
                  "0000000000000000", "1234567890ABCDEF"}


def _plausible_serial(serial: str | None) -> bool:
    s = (serial or "").strip().upper()
    return len(s) >= 6 and s not in _BOGUS_SERIALS and len(set(s)) > 2


def parse_ioreg_serials(text: str) -> dict[str, str]:
    """ganze Platte (disk4) -> USB-Seriennummer des Gehäuses, aus `ioreg -c IOUSBHostDevice -r -l`.
    Die Nummer steht im Baum immer vor den BSD-Namen ihrer Platten."""
    out: dict[str, str] = {}
    serial = None
    for line in text.splitlines():
        if line.startswith("+-o "):
            serial = None  # neues USB-Gerät auf oberster Ebene
        if '"USB Serial Number" = "' in line:
            serial = line.split('"USB Serial Number" = "', 1)[1].split('"', 1)[0]
        elif '"BSD Name" = "disk' in line:
            bsd = line.split('"BSD Name" = "', 1)[1].split('"', 1)[0]
            if "s" not in bsd[4:] and _plausible_serial(serial):
                out.setdefault(bsd, serial)
    return out


def _usb_serials() -> dict[str, str]:
    """bsd_name der ganzen Platte -> Seriennummer des USB-Geräts (best effort).
    Platzhalter-Nummern billiger Bridges werden verworfen (nicht eindeutig)."""
    try:
        r = subprocess.run(["ioreg", "-c", "IOUSBHostDevice", "-r", "-l", "-w0"],
                           capture_output=True, timeout=60, text=True, errors="replace")
        return parse_ioreg_serials(r.stdout)
    except (subprocess.TimeoutExpired, OSError):
        return {}


def volume_info(mount: str, serials: dict | None = None) -> dict:
    """Identität + Kapazität eines gemounteten Volumes aus diskutil."""
    info = _diskutil(mount)
    stores = info.get("APFSPhysicalStores") or []
    phys = stores[0].get("APFSPhysicalStore") if stores else None
    phys_whole = _diskutil(phys).get("ParentWholeDisk") if phys else None
    whole = phys_whole or info.get("ParentWholeDisk")
    if serials is None:
        serials = _usb_serials()
    free = info.get("APFSContainerFree") or info.get("FreeSpace") or None
    return {
        "uuid": info.get("VolumeUUID") or f"name:{Path(mount).name}",
        "name": info.get("VolumeName") or Path(mount).name,
        "fs_type": info.get("FilesystemType"),
        "capacity_bytes": info.get("TotalSize"),
        "free_bytes": free,
        "device_id": info.get("DeviceIdentifier"),
        "whole_disk": whole,
        "physical_store": phys,
        "media_name": (info.get("MediaName") or "").strip() or None,
        "bus_protocol": info.get("BusProtocol"),
        "solid_state": int(bool(info["SolidState"])) if "SolidState" in info else None,
        "device_serial": serials.get(whole) if whole else None,
    }


def upsert_volume(conn: sqlite3.Connection, vol: dict) -> None:
    now = _now()
    row = {**vol, "now": now}
    conn.execute(
        """INSERT INTO volume(uuid,name,fs_type,capacity_bytes,free_bytes,device_id,whole_disk,
               physical_store,media_name,bus_protocol,solid_state,device_serial,first_seen,last_seen)
           VALUES(:uuid,:name,:fs_type,:capacity_bytes,:free_bytes,:device_id,:whole_disk,
               :physical_store,:media_name,:bus_protocol,:solid_state,:device_serial,:now,:now)
           ON CONFLICT(uuid) DO UPDATE SET
               name=excluded.name, fs_type=excluded.fs_type,
               capacity_bytes=excluded.capacity_bytes, free_bytes=excluded.free_bytes,
               device_id=excluded.device_id, whole_disk=excluded.whole_disk,
               physical_store=excluded.physical_store, media_name=excluded.media_name,
               bus_protocol=excluded.bus_protocol, solid_state=excluded.solid_state,
               device_serial=COALESCE(excluded.device_serial, volume.device_serial),
               last_seen=excluded.last_seen""",
        row,
    )


# ── Inventur (nur stat) ───────────────────────────────────────────

def walk_files(root: Path, errors: list):
    """Iterativ alle regulären Dateien unter root: (rel_path, stat). Folgt keinen Symlinks."""
    root = Path(root)
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                entries = list(it)
        except FileNotFoundError:
            if not root.is_dir():
                errors.append((str(root), "Datenträger nicht mehr erreichbar (abgezogen/getrennt)"))
                return
            continue  # Ordner zwischenzeitlich gelöscht/umbenannt
        except OSError as e:
            errors.append((str(d), str(e)))
            if not root.is_dir():
                errors.append((str(root), "Datenträger nicht mehr erreichbar (abgezogen/getrennt)"))
                return
            continue
        for e in entries:
            n = e.name
            if n in _SKIP_DIRS or n.startswith("._") or n.endswith(".tmp") or n.startswith(_SKIP_PREFIXES):
                continue
            try:
                if e.is_dir(follow_symlinks=False):
                    stack.append(Path(e.path))
                elif e.is_file(follow_symlinks=False):
                    yield os.path.relpath(e.path, root), e.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue  # zwischen Auflisten und stat() verschwunden (z. B. Lightroom-Vorschauen) - kein Lesefehler
            except OSError as err:
                errors.append((e.path, str(err)))


def inventory(mount: str, conn: sqlite3.Connection, vol: dict | None = None,
              host: str = "") -> dict:
    """Erfasst alle Dateien eines Volumes. Neue/geänderte/verschwundene Dateien
    werden markiert; ein geänderter Inhalt (Größe/mtime) verwirft den Fingerprint."""
    mount = str(Path(mount))
    if not Path(mount).is_dir():
        raise FileNotFoundError(f"Nicht gemountet/kein Ordner: {mount}")
    vol = vol or volume_info(mount)
    uuid = vol["uuid"]
    started = _now()
    with conn:
        upsert_volume(conn, vol)

    existing = {
        r[0]: (r[1], r[2], r[3], r[4])
        for r in conn.execute(
            "SELECT rel_path,size,mtime,inode,present FROM file WHERE volume_uuid=?", (uuid,))
    }
    seen = set()
    errors: list = []
    stats = {"files": 0, "bytes": 0, "added": 0, "changed": 0, "removed": 0}
    # Fortschritt ans Dashboard: Erwartung = letzte Inventur dieser Kennung (falls vorhanden)
    from .progress import Reporter
    prev = conn.execute("SELECT files, bytes FROM run WHERE volume_uuid=? AND kind='inventory' ORDER BY complete DESC, id DESC LIMIT 1",
                        (uuid,)).fetchone()
    rep = Reporter("inventur", uuid, vol["name"], total_files=prev[0] if prev else (len(existing) or None),
                   total_bytes=prev[1] if prev else None)
    batch_new, batch_upd, batch_touch = [], [], []
    now = started

    def flush():
        with conn:
            conn.executemany(
                """INSERT INTO file(volume_uuid,rel_path,name,ext,kind,size,mtime,birthtime,inode,
                       present,first_seen,last_seen,changed_at)
                   VALUES(?,?,?,?,?,?,?,?,?,1,?,?,?)""", batch_new)
            conn.executemany(
                """UPDATE file SET size=?,mtime=?,birthtime=?,inode=?,fingerprint=NULL,fp_algo=NULL,
                       hashed_at=NULL,present=1,gone_at=NULL,last_seen=?,changed_at=?
                   WHERE volume_uuid=? AND rel_path=?""", batch_upd)
            conn.executemany(
                "UPDATE file SET last_seen=? WHERE volume_uuid=? AND rel_path=?", batch_touch)
        batch_new.clear(); batch_upd.clear(); batch_touch.clear()

    for rel, st in walk_files(Path(mount), errors):
        seen.add(rel)
        stats["files"] += 1
        stats["bytes"] += st.st_size
        mtime = round(st.st_mtime, 3)
        birth = getattr(st, "st_birthtime", None)
        old = existing.get(rel)
        if old is None:
            name = os.path.basename(rel)
            ext, kind = classify(name)
            batch_new.append((uuid, rel, name, ext, kind, st.st_size, mtime, birth, st.st_ino,
                              now, now, now))
            stats["added"] += 1
        elif old[0] != st.st_size or old[1] != mtime or not old[3]:
            # Inhalt geändert oder wieder aufgetaucht -> Fingerprint neu
            batch_upd.append((st.st_size, mtime, birth, st.st_ino, now, now, uuid, rel))
            stats["changed"] += 1
        else:
            batch_touch.append((now, uuid, rel))
        if len(batch_new) + len(batch_upd) + len(batch_touch) >= 5000:
            flush()
            rep.update(stats["files"], stats["bytes"])
    flush()

    # Nicht lesbare Ordner: Dateien darunter NICHT als verschwunden werten,
    # sonst "löscht" ein Lesefehler Teile des Katalogs.
    unreadable = [os.path.relpath(p, mount) for p, _ in errors]
    complete = not errors
    if "." in unreadable:
        gone = []  # Root nicht lesbar -> nichts ableiten
    else:
        blocked = tuple(u + "/" for u in unreadable)
        gone = [rel for rel, v in existing.items()
                if v[3] and rel not in seen and not (blocked and rel.startswith(blocked))
                and rel not in unreadable]
    if gone:
        with conn:
            conn.executemany(
                "UPDATE file SET present=0,gone_at=?,changed_at=? WHERE volume_uuid=? AND rel_path=?",
                [(now, now, uuid, rel) for rel in gone])
        stats["removed"] = len(gone)

    finished = _now()
    with conn:
        conn.execute("UPDATE volume SET last_inventory=? WHERE uuid=?", (finished, uuid))
        conn.execute(
            """INSERT INTO run(volume_uuid,kind,started_at,finished_at,host,files,bytes,added,changed,
                   removed,hashed,errors,complete) VALUES(?,?,?,?,?,?,?,?,?,?,0,?,?)""",
            (uuid, "inventory", started, finished, host, stats["files"], stats["bytes"],
             stats["added"], stats["changed"], stats["removed"], len(errors), int(complete)))
    if errors:
        with conn:
            conn.executemany("INSERT INTO run_error(run_started, volume_uuid, path, message) VALUES(?,?,?,?)",
                             [(started, uuid, p, m[:300]) for p, m in errors])
    rep.finish(stats["files"], stats["bytes"], ok=complete, message=None if complete else f"{len(errors)} Fehler")
    for path, msg in errors[:20]:
        _log.warning("Inventur-Fehler %s: %s", path, msg)
    if len(errors) > 20:
        _log.warning("… %d weitere Fehler (Tabelle run_error)", len(errors) - 20)
    return {**stats, "uuid": uuid, "name": vol["name"], "errors": len(errors), "complete": complete}


# ── Fingerprints (liest Inhalte, langsam, resumable) ──────────────

def hash_pass(mount: str, conn: sqlite3.Connection, uuid: str | None = None,
              budget_s: float | None = None, host: str = "", prefix: str = "") -> dict:
    """Fingerprintet alle noch ungehashten Dateien des Volumes, größte zuerst.
    Bricht nach budget_s Sekunden sauber ab; der nächste Lauf macht weiter."""
    from .ingest.verify import fingerprint

    mount = Path(mount)
    if not mount.is_dir():
        raise FileNotFoundError(f"Nicht gemountet/kein Ordner: {mount}")
    uuid = uuid or volume_info(str(mount), serials={})["uuid"]
    started, t0 = _now(), time.monotonic()
    q = """SELECT rel_path,size,mtime FROM file
           WHERE volume_uuid=? AND present=1 AND fingerprint IS NULL"""
    args = [uuid]
    if prefix.strip("/"):
        pre = prefix.strip("/")
        esc = pre.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        q += " AND (rel_path=? OR rel_path LIKE ? ESCAPE '\\')"
        args += [pre, esc + "/%"]
    todo = conn.execute(q + " ORDER BY size DESC", args).fetchall()
    hashed = errors = 0
    done_bytes = 0
    timed_out = False
    for rel, size, mtime in todo:
        if budget_s is not None and time.monotonic() - t0 > budget_s:
            timed_out = True
            break
        p = mount / rel
        try:
            st = p.stat()
            if st.st_size != size or round(st.st_mtime, 3) != mtime:
                continue  # zwischenzeitlich geändert -> nächste Inventur erfasst es neu
            fp = fingerprint(p)
        except OSError as e:
            errors += 1
            _log.warning("Hash-Fehler %s: %s", p, e)
            continue
        now = _now()
        with conn:
            conn.execute(
                """UPDATE file SET fingerprint=?,fp_algo=?,hashed_at=?,changed_at=?
                   WHERE volume_uuid=? AND rel_path=?""",
                (fp, FP_ALGO, now, now, uuid, rel))
        hashed += 1
        done_bytes += size
    remaining = len(todo) - hashed - errors if timed_out else 0
    finished = _now()
    with conn:
        if not timed_out and not prefix:
            conn.execute("UPDATE volume SET last_hash_pass=? WHERE uuid=?", (finished, uuid))
        conn.execute(
            """INSERT INTO run(volume_uuid,kind,started_at,finished_at,host,files,bytes,added,changed,
                   removed,hashed,errors,complete) VALUES(?,?,?,?,?,?,?,0,0,0,?,?,?)""",
            (uuid, "hash", started, finished, host, len(todo), done_bytes, hashed, errors,
             int(not timed_out)))
    return {"uuid": uuid, "hashed": hashed, "bytes": done_bytes, "errors": errors,
            "remaining": remaining, "complete": not timed_out}


# ── Vergleich zweier Kopien ───────────────────────────────────────

def _resolve_volume(conn, ref: str) -> str:
    """UUID aus UUID oder Volume-Name. Mehrdeutige Namen sind ein Fehler (siehe CLAUDE.md §3)."""
    rows = conn.execute("SELECT uuid FROM volume WHERE uuid=? OR name=?", (ref, ref)).fetchall()
    if not rows:
        raise SystemExit(f"Unbekanntes Volume: {ref}")
    if len(rows) > 1:
        raise SystemExit(f"Name '{ref}' ist mehrdeutig ({', '.join(r[0] for r in rows)}), bitte UUID angeben")
    return rows[0][0]


def _files_under(conn, uuid: str, prefix: str) -> dict:
    prefix = prefix.strip("/")
    q = "SELECT rel_path,size,fingerprint,name FROM file WHERE volume_uuid=? AND present=1"
    args = [uuid]
    if prefix:
        q += " AND (rel_path=? OR rel_path LIKE ? ESCAPE '\\')"
        esc = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        args += [prefix, esc + "/%"]
    cut = len(prefix) + 1 if prefix else 0
    return {r[0][cut:]: (r[1], r[2], r[3]) for r in conn.execute(q, args)}


def diff(conn: sqlite3.Connection, ref_vol: str, ref_prefix: str,
         copy_vol: str, copy_prefix: str) -> dict:
    """Vergleicht Referenz gegen Kopie (relativ zum jeweiligen Projektordner).

    - missing: in der Referenz, in der Kopie weder am gleichen Pfad noch woanders
      (Name+Größe bzw. Fingerprint) zu finden
    - moved: in der Kopie unter anderem Pfad vorhanden
    - mismatch: gleicher Pfad, aber andere Größe oder anderer Fingerprint
    - unverified: gleicher Pfad + Größe, aber mind. eine Seite ohne Fingerprint
    - ok: gleicher Pfad, Größe und Fingerprint
    - extra: nur in der Kopie
    """
    ref = _files_under(conn, _resolve_volume(conn, ref_vol), ref_prefix)
    cpy = _files_under(conn, _resolve_volume(conn, copy_vol), copy_prefix)
    by_fp = {v[1]: k for k, v in cpy.items() if v[1]}
    by_name_size = {(v[2], v[0]): k for k, v in cpy.items()}
    out = {k: [] for k in ("ok", "unverified", "mismatch", "moved", "missing", "extra")}
    matched = set()
    for rel, (size, fp, name) in sorted(ref.items()):
        c = cpy.get(rel)
        if c is not None:
            matched.add(rel)
            if c[0] != size or (fp and c[1] and fp != c[1]):
                out["mismatch"].append({"path": rel, "size": size, "copy_size": c[0]})
            elif fp and c[1]:
                out["ok"].append(rel)
            else:
                out["unverified"].append(rel)
            continue
        other = by_fp.get(fp) if fp else None
        other = other or by_name_size.get((name, size))
        if other:
            matched.add(other)
            out["moved"].append({"path": rel, "copy_path": other, "size": size})
        else:
            out["missing"].append({"path": rel, "size": size})
    out["extra"] = sorted(k for k in cpy if k not in matched)
    out["summary"] = {k: len(v) for k, v in out.items()}
    out["summary"]["missing_bytes"] = sum(m["size"] for m in out["missing"])
    out["summary"]["ref_files"] = len(ref)
    out["summary"]["copy_files"] = len(cpy)
    return out


def find_folders(conn: sqlite3.Connection, name: str) -> list[dict]:
    """Alle Ordner (über alle Volumes), deren Name `name` enthält, mit Dateizahl und Größe.
    Gedacht statt D1-rel_path: der reale Pfad kann einen Zwischenordner mehr haben
    (z. B. "NXT STUDIOS/"), je nachdem wo ein Scan angesetzt hat."""
    needle = name.lower()
    agg: dict[tuple, list] = {}
    rows = conn.execute(
        """SELECT v.name, f.volume_uuid, f.rel_path, f.size FROM file f
           JOIN volume v ON v.uuid=f.volume_uuid
           WHERE f.present=1 AND instr(lower(f.rel_path), ?) > 0""", (needle,))
    for vname, uuid, rel, size in rows:
        parts = rel.split("/")[:-1]  # nur Ordner, nicht der Dateiname
        for i, part in enumerate(parts):
            if needle in part.lower():
                key = (vname, uuid, "/".join(parts[:i + 1]))
                a = agg.setdefault(key, [0, 0])
                a[0] += 1
                a[1] += size or 0
                break  # äußersten passenden Ordner zählen
    return [{"volume": k[0], "uuid": k[1], "folder": k[2], "files": v[0], "bytes": v[1]}
            for k, v in sorted(agg.items())]


# ── Einbindung in den Auto-Scan ───────────────────────────────────

def catalog_safe(mount: str, config: dict) -> None:
    """Best-effort nach jedem Scan. Nur wenn `file_catalog` in der Config an ist.
    `file_catalog_hash_minutes` (Default 0 = nur Inventur) begrenzt den Hash-Lauf."""
    if not config.get("file_catalog"):
        return
    try:
        import socket
        host = socket.gethostname()
        conn = connect()
        try:
            res = inventory(mount, conn, host=host)
            _log.info("Katalog-Inventur %s: %s", mount, res)
            minutes = float(config.get("file_catalog_hash_minutes") or 0)
            if minutes > 0:
                h = hash_pass(mount, conn, uuid=res["uuid"], budget_s=minutes * 60, host=host)
                _log.info("Katalog-Hash %s: %s", mount, h)
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001 - darf Scan/Notion/D1 nie stören
        _log.error("Katalog fehlgeschlagen fuer %s: %s", mount, e)


# ── CLI ───────────────────────────────────────────────────────────

def _fmt(n: int) -> str:
    from .utils import format_size
    return format_size(n or 0)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m storage_scanner.catalog",
                                 description="NXT Datei-Katalog (lokal)")
    ap.add_argument("--db", help="Pfad zur Katalog-DB (Default: Application Support)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("inventory", help="alle Dateien eines Volumes erfassen (nur stat)")
    p.add_argument("mount")
    p.add_argument("--uuid", help="feste Kennung für Freigaben ohne Volume-UUID (z. B. nas:NXTCLOUD/NXT STUDIOS)")
    p.add_argument("--name", help="Anzeigename (Default: Ordnername)")
    p = sub.add_parser("report", help="Scan-Report aus dem Katalog bauen und nach D1 hochladen")
    p.add_argument("mount"); p.add_argument("--uuid", required=True); p.add_argument("--name")
    p = sub.add_parser("hash", help="Fingerprints nachziehen (liest Dateien)")
    p.add_argument("mount")
    p.add_argument("--minutes", type=float, help="Zeitbudget, danach sauber abbrechen")
    p.add_argument("--prefix", default="", help="nur Dateien unter diesem Ordner (relativ zum Volume)")
    p = sub.add_parser("diff", help="Referenz gegen Kopie vergleichen")
    p.add_argument("ref_volume"); p.add_argument("ref_prefix")
    p.add_argument("copy_volume"); p.add_argument("copy_prefix")
    p.add_argument("--json", action="store_true")
    p.add_argument("--limit", type=int, default=50)
    sub.add_parser("stats", help="Überblick pro Volume")
    p = sub.add_parser("find", help="Projektordner per Namensteil auf allen Volumes finden")
    p.add_argument("name")
    args = ap.parse_args(argv)

    conn = connect(args.db)
    if args.cmd == "inventory":
        vol = None
        if args.uuid:
            vol = volume_info(args.mount, serials={})
            vol["uuid"] = args.uuid
            vol["name"] = args.name or vol["name"]
        r = inventory(args.mount, conn, vol=vol)
        print(f"{r['name']} ({r['uuid']}): {r['files']} Dateien, {_fmt(r['bytes'])} | "
              f"neu {r['added']}, geändert {r['changed']}, weg {r['removed']}, Fehler {r['errors']}"
              + ("" if r["complete"] else "  [UNVOLLSTÄNDIG]"))
    elif args.cmd == "hash":
        budget = args.minutes * 60 if args.minutes else None
        r = hash_pass(args.mount, conn, budget_s=budget, prefix=args.prefix)
        print(f"gehasht {r['hashed']} ({_fmt(r['bytes'])}), Fehler {r['errors']}, "
              f"offen {r['remaining']}" + ("" if r["complete"] else "  [Budget erreicht]"))
    elif args.cmd == "diff":
        r = diff(conn, args.ref_volume, args.ref_prefix, args.copy_volume, args.copy_prefix)
        if args.json:
            print(json.dumps(r, ensure_ascii=False, indent=2))
        else:
            s = r["summary"]
            print(f"Referenz {s['ref_files']} Dateien, Kopie {s['copy_files']} Dateien")
            print(f"  ok {s['ok']} · ungeprüft {s['unverified']} · verschoben {s['moved']} · "
                  f"ABWEICHEND {s['mismatch']} · FEHLT {s['missing']} ({_fmt(s['missing_bytes'])}) · "
                  f"nur in Kopie {s['extra']}")
            for key in ("missing", "mismatch"):
                for item in r[key][:args.limit]:
                    print(f"  {key.upper():8} {item['path']}  ({_fmt(item['size'])})")
    elif args.cmd == "find":
        for r in find_folders(conn, args.name):
            print(f"{r['volume']:16} {r['files']:>7} Dateien {_fmt(r['bytes']):>10}  {r['folder']!r}")
    elif args.cmd == "report":
        from .paths import REPORTS_DIR
        from . import outbox
        from .notion_sync import load_config
        rep = report_from_catalog(conn, args.uuid, str(Path(args.mount)), name=args.name)
        if not rep["scan_info"]["complete"]:
            raise SystemExit("Letzte Inventur war unvollständig – kein Upload.")
        name = args.name or Path(args.mount).name
        rep["scan_info"]["scanned_path"] = str(Path(args.mount).parent / name)  # Anzeigename in D1
        out = REPORTS_DIR / (name.replace(" ", "_").replace("/", "_") + "_report.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(rep, ensure_ascii=False, indent=1))
        outbox.enqueue(str(out), channels=outbox.channels_for(load_config()))
        print(f"Report: {len(rep['folder_tree'])} Knoten, {rep['scan_info']['valid_folders']} gültige Projektordner → {out}")
        print("Upload:", outbox.flush_safe())
    elif args.cmd == "stats":
        rows = conn.execute(
            """SELECT v.name, v.uuid, v.device_serial, COUNT(f.id), COALESCE(SUM(f.size),0),
                      SUM(f.fingerprint IS NOT NULL), v.last_inventory
               FROM volume v LEFT JOIN file f ON f.volume_uuid=v.uuid AND f.present=1
               GROUP BY v.uuid ORDER BY v.name""").fetchall()
        for name, uuid, serial, n, size, hashed, last in rows:
            print(f"{name:22} {n:>9} Dateien {_fmt(size):>10}  gehasht {hashed or 0:>9}  "
                  f"Serial {serial or '-':18} {uuid}  zuletzt {last}")
    conn.close()


if __name__ == "__main__":
    main()


# ── Scan-Report aus dem Katalog (für Netzfreigaben wie das NAS) ────

_FLAT_CONTAINERS = {"NXT STUDIOS", "OLDER"}   # wie scan.py: Inhalt zählt als oberste Ebene


def report_from_catalog(conn: sqlite3.Connection, uuid: str, mount: str, name: str | None = None,
                        max_container_depth: int = 2) -> dict:
    """Baut einen Scan-Report (scan_info + folder_tree bis Tiefe 3) aus dem Datei-Katalog.

    Ein Durchlauf über die Freigabe (Inventur) statt du/find je Ordner: für ~80 TB über
    SMB deutlich schneller und ohne Zeitlimits. Sammelordner werden aufgelöst: die festen
    Container (NXT STUDIOS, OLDER) wie im Scanner, und allgemein jeder Ordner ohne gültigen
    Projektnamen, der Projektordner enthält (z. B. "UNSORTED ARCHIVE", "CONNOR").
    """
    from .rules import validate_folder
    from .report import _disk_usage
    rows = conn.execute("SELECT rel_path, size, mtime FROM file WHERE volume_uuid=? AND present=1", (uuid,)).fetchall()

    # Verzeichnisbaum aus Dateipfaden: Kinder je Ordner
    children: dict[str, set] = {}
    for rel, _, _ in rows:
        parts = rel.split("/")[:-1]
        for i in range(len(parts)):
            parent = "/".join(parts[:i])
            children.setdefault(parent, set()).add(parts[i])

    def has_project(path: str, levels: int) -> bool:
        """Liegt innerhalb von `levels` Ebenen unter path ein gültiger Projektordner?"""
        if levels <= 0:
            return False
        for k in children.get(path, ()):
            if validate_folder(k) or has_project(f"{path}/{k}", levels - 1):
                return True
        return False

    def expand(prefix: str, depth: int) -> list[tuple[str, str]]:
        """Liefert (rel_path_für_D1, echter_pfad) der Projekt-Einheiten unter prefix."""
        units = []
        for d in sorted(children.get(prefix, ())):
            real = f"{prefix}/{d}" if prefix else d
            if d.startswith(".") or d.startswith("#") or d.startswith("@"):
                continue  # #recycle, .snapshot, @eaDir …
            kids = children.get(real, set())
            if not prefix and d.upper() in _FLAT_CONTAINERS:
                units += [(k, f"{real}/{k}") for k in sorted(kids) if not k.startswith((".", "#", "@"))]
                continue
            if depth < max_container_depth and not validate_folder(d) and has_project(real, max_container_depth - depth):
                units += expand(real, depth + 1)
                continue
            units.append((real, real))
        return units

    units = expand("", 0)
    # Größen/Dateizahlen/mtime je echtem Ordnerpfad (kumulativ) bis 2 Ebenen unter der Einheit
    agg: dict[str, list] = {}
    unit_by_real = {real: rel for rel, real in units}
    for rel, size, mtime in rows:
        parts = rel.split("/")
        for i in range(1, len(parts)):
            p = "/".join(parts[:i])
            a = agg.setdefault(p, [0, 0, 0.0])
            a[0] += size or 0; a[1] += 1; a[2] = max(a[2], mtime or 0)

    from datetime import datetime as _dt
    iso = lambda ts: _dt.fromtimestamp(ts).isoformat() if ts else None
    tree = []
    for rel, real in units:
        a = agg.get(real, [0, 0, 0])
        tree.append({"rel_path": rel, "abs_path": f"{mount}/{real}", "depth": 1, "parent_rel_path": None,
                     "size_bytes": a[0], "file_count": a[1], "mtime": iso(a[2])})
        for c in sorted(children.get(real, ())):
            cr = f"{real}/{c}"; ca = agg.get(cr, [0, 0, 0])
            tree.append({"rel_path": f"{rel}/{c}", "abs_path": f"{mount}/{cr}", "depth": 2, "parent_rel_path": rel,
                         "size_bytes": ca[0], "file_count": ca[1], "mtime": iso(ca[2])})
            for g in sorted(children.get(cr, ())):
                gr = f"{cr}/{g}"; ga = agg.get(gr, [0, 0, 0])
                tree.append({"rel_path": f"{rel}/{c}/{g}", "abs_path": f"{mount}/{gr}", "depth": 3,
                             "parent_rel_path": f"{rel}/{c}", "size_bytes": ga[0], "file_count": ga[1], "mtime": iso(ga[2])})
    valid = sum(1 for rel, _ in units if validate_folder(rel.split("/")[-1]))
    run = conn.execute("SELECT complete FROM run WHERE volume_uuid=? AND kind='inventory' ORDER BY id DESC LIMIT 1", (uuid,)).fetchone()
    from zoneinfo import ZoneInfo
    return {
        "scan_info": {"scanned_path": mount, "scan_date": _dt.now(ZoneInfo("Europe/Berlin")).isoformat(),
                      "total_folders": len(units), "valid_folders": valid, "unassigned_folders": len(units) - valid,
                      "volume_uuid": uuid, "disk_usage": _disk_usage(mount),
                      "complete": bool(run and run[0]), "incomplete_folders": [], "source": "catalog"},
        "projects": [], "unassigned": [], "folder_tree": tree, "_display_name": name,
    }



def scan_via_catalog(mount: str, report_path: str, conn: sqlite3.Connection | None = None) -> dict:
    """Ein einziger Durchlauf: Inventur (stat) → Report wie run_scan, aber Größen/Baum aus dem Katalog.

    Auf HDDs 3–4× schneller als run_scan (das jeden Ordner per du + find + Baum mehrfach liest).
    Wirft scan.ScanIncomplete, wenn die Inventur unvollständig war (Report wird dann nicht geschrieben).
    """
    import socket
    from .scan import ScanIncomplete
    own = conn is None
    conn = conn or connect()
    try:
        res = inventory(mount, conn, host=socket.gethostname())
        if not res["complete"]:
            raise ScanIncomplete(f"Inventur unvollständig ({res['errors']} Fehler) – nichts hochgeladen")
        rep = report_from_catalog(conn, res["uuid"], str(Path(mount)), name=res["name"])
    finally:
        if own:
            conn.close()
    out = Path(report_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(rep, ensure_ascii=False, indent=1))
    tmp.replace(out)
    return rep
