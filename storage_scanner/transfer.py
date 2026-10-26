"""Sichere Übertragung: vollständig verifiziert, absturzfest, wiederaufnehmbar.

Garantien pro Datei (in dieser Reihenfolge):
1. Quelle wird nur gelesen, nie verändert oder gelöscht.
2. Ein vorhandenes, ANDERES Ziel wird nie überschrieben (Kollision -> Fehler).
3. Geschrieben wird nach `<name>.nxt.tmp`; der Zielname erscheint erst, wenn
   - alle Bytes auf der Platte sind (F_FULLFSYNC, nicht nur im Cache),
   - das Ziel komplett neu gelesen wurde (am Cache vorbei) und sein voller
     blake2b-Hash dem der Quelle entspricht,
   - die Quelle sich während der Kopie nicht verändert hat.
4. Eine Quelle wird einmal gelesen und parallel auf alle Ziele geschrieben.

Robustheit:
- Abbruch (Kabel, Strom, Absturz): halbe `.nxt.tmp` werden beim nächsten Lauf
  verworfen; fertige Dateien werden per Journal + Größe übersprungen.
- Verschwindet ein Datenträger, wird gewartet, bis er wieder da ist, dann
  wird die Datei neu begonnen (mehrere Versuche).
- Während eines Jobs verhindert `caffeinate` den Ruhezustand.
- Vor dem Start wird freier Platz auf jedem Ziel geprüft.
"""

import argparse
import errno
import fcntl
import hashlib
import logging
import os
import shutil
import sqlite3
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from .paths import DATA_DIR

_log = logging.getLogger("transfer")

JOURNAL_PATH = DATA_DIR / "transfers.db"
TMP_SUFFIX = ".nxt.tmp"
HASH_ALGO = "blake2b-256"
_CHUNK = 8 * 1024 * 1024
_SKIP = {".DS_Store", ".Spotlight-V100", ".fseventsd", ".Trashes", ".TemporaryItems",
         ".DocumentRevisions-V100", "$RECYCLE.BIN", "System Volume Information"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY, src_root TEXT, dst_roots TEXT, created_at TEXT,
    finished_at TEXT, state TEXT DEFAULT 'pending', note TEXT
);
CREATE TABLE IF NOT EXISTS job_file (
    job_id INTEGER, rel_path TEXT, size INTEGER, src_mtime REAL,
    hash TEXT, state TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0,
    error TEXT, done_at TEXT,
    PRIMARY KEY(job_id, rel_path)
);
"""


class TransferError(Exception):
    """Endgültiger Fehler für eine Datei (kein Retry sinnvoll)."""


class Collision(TransferError):
    """Am Ziel liegt eine andere Datei gleichen Namens."""


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


# ── Low-level: Durability + Cache-freies Lesen ────────────────────

def _full_fsync(fd: int) -> None:
    """Daten wirklich auf das Medium schreiben (macOS: F_FULLFSYNC)."""
    try:
        fcntl.fcntl(fd, getattr(fcntl, "F_FULLFSYNC", 51))
    except OSError:
        os.fsync(fd)  # z. B. exFAT/Netz ohne F_FULLFSYNC


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        _full_fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _no_cache(f) -> None:
    """Liest am Page-Cache vorbei, damit die Prüfung die Platte sieht, nicht den RAM."""
    try:
        fcntl.fcntl(f.fileno(), getattr(fcntl, "F_NOCACHE", 48), 1)
    except OSError:
        pass


def _clear_flags(path: Path) -> int:
    """Entfernt BSD-Datei-Flags (z. B. uchg) und gibt die alten zurück (0 = keine / nicht unterstützt)."""
    try:
        flags = os.lstat(path).st_flags
        if flags:
            os.chflags(path, 0)
        return flags
    except (OSError, AttributeError):
        return 0


def file_hash(path: Path, chunk: int = _CHUNK) -> str:
    """Voller blake2b-256 über alle Bytes, ohne Cache."""
    h = hashlib.blake2b(digest_size=32)
    with open(path, "rb") as f:
        _no_cache(f)
        for buf in iter(lambda: f.read(chunk), b""):
            h.update(buf)
    return h.hexdigest()


# ── Eine Datei auf N Ziele ────────────────────────────────────────

def safe_copy(src: Path, dsts: list[Path], chunk: int = _CHUNK, progress=None,
              src_hash: str | None = None) -> dict:
    """Kopiert src auf alle dsts mit den Garantien oben.

    Gibt {"hash", "copied": [dst...], "skipped": [dst...]} zurück.
    Wirft Collision bei fremder Datei am Ziel, TransferError bei Prüfungsfehler,
    OSError bei I/O-Problemen (Aufrufer entscheidet über Retry).
    """
    src = Path(src)
    st0 = src.stat()
    todo, skipped = [], []

    for d in map(Path, dsts):
        tmp = d.with_name(d.name + TMP_SUFFIX)
        if tmp.exists():
            _clear_flags(tmp)
            tmp.unlink()  # Rest eines abgebrochenen Laufs, nie vertrauenswürdig
        if d.exists():
            if d.stat().st_size != st0.st_size:
                raise Collision(f"Ziel existiert mit anderer Größe: {d}")
            src_hash = src_hash or file_hash(src, chunk)
            if file_hash(d, chunk) != src_hash:
                raise Collision(f"Ziel existiert mit anderem Inhalt: {d}")
            skipped.append(d)
        else:
            todo.append(d)

    if not todo:
        return {"hash": src_hash or file_hash(src, chunk), "copied": [], "skipped": skipped}

    tmps = []
    outs = []
    try:
        for d in todo:
            d.parent.mkdir(parents=True, exist_ok=True)
            tmp = d.with_name(d.name + TMP_SUFFIX)
            tmps.append(tmp)
            outs.append(open(tmp, "xb"))  # x: nie eine fremde Datei überschreiben
        h = hashlib.blake2b(digest_size=32)
        with open(src, "rb") as fin:
            _no_cache(fin)
            for buf in iter(lambda: fin.read(chunk), b""):
                h.update(buf)
                for fo in outs:
                    fo.write(buf)
                if progress:
                    progress(len(buf))
        for fo in outs:
            fo.flush()
            _full_fsync(fo.fileno())
            fo.close()
        outs = []
        digest = h.hexdigest()

        st1 = src.stat()
        if (st1.st_size, st1.st_mtime_ns) != (st0.st_size, st0.st_mtime_ns):
            raise TransferError(f"Quelle hat sich während der Kopie geändert: {src}")
        if src_hash and src_hash != digest:
            raise TransferError(f"Quelle weicht vom erwarteten Hash ab: {src}")

        for tmp in tmps:
            if tmp.stat().st_size != st0.st_size or file_hash(tmp, chunk) != digest:
                raise TransferError(f"Verifikation fehlgeschlagen: {tmp}")

        for tmp, d in zip(tmps, todo):
            try:
                shutil.copystat(src, tmp)
            except OSError:
                pass  # exFAT kennt nicht alle Attribute; Inhalt ist verifiziert
            if d.exists():
                raise Collision(f"Ziel ist während der Kopie aufgetaucht: {d}")
            # Gesperrte Dateien (chflags uchg, z. B. SONYCARD.IND) lassen sich nicht umbenennen:
            # Sperre erst nach dem Umbenennen wieder setzen
            flags = _clear_flags(tmp)
            os.rename(tmp, d)
            if flags:
                try:
                    os.chflags(d, flags)
                except OSError:
                    pass
            _fsync_dir(d.parent)
        tmps = []
        return {"hash": digest, "copied": list(todo), "skipped": skipped}
    finally:
        for fo in outs:
            try:
                fo.close()
            except OSError:
                pass
        for tmp in tmps:
            try:
                _clear_flags(tmp)
                tmp.unlink()
            except OSError:
                pass


# ── Robustheit: Ruhezustand, Wackelkontakt, Platz ─────────────────

@contextmanager
def keep_awake():
    """Verhindert Ruhezustand (Idle/System/Disk) solange der Block läuft."""
    proc = None
    try:
        proc = subprocess.Popen(["caffeinate", "-ims", "-w", str(os.getpid())],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError:
        _log.warning("caffeinate nicht verfügbar")
    try:
        yield
    finally:
        if proc:
            proc.terminate()


def volume_root(path: Path) -> Path:
    """Ankerpunkt, dessen Verschwinden "Datenträger weg" bedeutet:
    /Volumes/<Name> für externe Platten, sonst der nächste existierende Elternordner."""
    p = Path(path).absolute()
    if len(p.parts) >= 3 and p.parts[1] == "Volumes":
        return Path(*p.parts[:3])
    while not p.exists() and p != p.parent:
        p = p.parent
    return p


def wait_for(paths: list[Path], timeout_s: float, poll_s: float = 5.0) -> bool:
    """Wartet, bis alle Pfade wieder existieren (Platte neu gemountet)."""
    deadline = time.monotonic() + timeout_s
    while True:
        if all(Path(p).exists() for p in paths):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(poll_s)


_RETRYABLE = {errno.EIO, errno.ENXIO, errno.ENODEV, errno.ENOENT, errno.ETIMEDOUT,
              errno.EBUSY, errno.EAGAIN, errno.ESTALE if hasattr(errno, "ESTALE") else errno.EIO}


def check_space(plan_bytes: int, dst_roots: list[Path], margin: float = 0.02) -> None:
    """Wirft TransferError, wenn ein Ziel nicht genug Platz hat (+2 % Reserve)."""
    for root in dst_roots:
        free = shutil.disk_usage(volume_root(root)).free
        need = int(plan_bytes * (1 + margin))
        if free < need:
            raise TransferError(f"Zu wenig Platz auf {root}: frei {free:,} B, nötig {need:,} B")


# ── Jobs mit Journal ──────────────────────────────────────────────

def connect(db_path=None) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path or JOURNAL_PATH), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    return conn


def _list_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in _SKIP)
        for n in sorted(filenames):
            if n in _SKIP or n.startswith("._") or n.endswith(TMP_SUFFIX):
                continue
            p = Path(dirpath) / n
            if p.is_symlink():
                continue
            yield p


def create_job(conn, src_root: Path, dst_roots: list[Path], note: str = "", only: set[str] | None = None) -> int:
    """Legt einen Job an: alle Dateien unter src_root -> gleiche relative Pfade unter jedem Ziel.
    `only`: nur diese relativen Pfade (z. B. die Dateien, die im Ziel noch fehlen)."""
    src_root = Path(src_root)
    if not src_root.is_dir():
        raise TransferError(f"Quelle nicht gefunden: {src_root}")
    with conn:
        cur = conn.execute(
            "INSERT INTO job(src_root,dst_roots,created_at,note) VALUES(?,?,?,?)",
            (str(src_root), "\n".join(map(str, dst_roots)), _now(), note))
        job_id = cur.lastrowid
        rows = []
        for p in _list_files(src_root):
            rel = str(p.relative_to(src_root))
            if only is not None and rel not in only:
                continue
            st = p.stat()
            rows.append((job_id, rel, st.st_size, st.st_mtime))
        conn.executemany(
            "INSERT INTO job_file(job_id,rel_path,size,src_mtime) VALUES(?,?,?,?)", rows)
    return job_id


def run_job(conn, job_id: int, wait_s: float = 1800, max_attempts: int = 5,
            progress=None, log=print) -> dict:
    """Arbeitet einen Job ab. Beliebig oft aufrufbar: Erledigtes wird übersprungen."""
    src_root, dst_raw, state = conn.execute(
        "SELECT src_root,dst_roots,state FROM job WHERE id=?", (job_id,)).fetchone()
    src_root = Path(src_root)
    dst_roots = [Path(p) for p in dst_raw.split("\n")]
    mounts = [volume_root(src_root)] + [volume_root(d) for d in dst_roots]

    pending = conn.execute(
        "SELECT rel_path,size FROM job_file WHERE job_id=? AND state!='done' ORDER BY rel_path",
        (job_id,)).fetchall()
    check_space(sum(s for _, s in pending), dst_roots)

    counts = {"done": 0, "failed": 0, "collision": 0}
    with conn:
        conn.execute("UPDATE job SET state='running' WHERE id=?", (job_id,))
    with keep_awake():
        for rel, size in pending:
            src = src_root / rel
            dsts = [d / rel for d in dst_roots]
            attempts = 0
            while True:
                attempts += 1
                try:
                    res = safe_copy(src, dsts, progress=progress)
                    with conn:
                        conn.execute(
                            """UPDATE job_file SET state='done',hash=?,attempts=attempts+?,
                                   error=NULL,done_at=? WHERE job_id=? AND rel_path=?""",
                            (res["hash"], attempts, _now(), job_id, rel))
                    counts["done"] += 1
                    break
                except Collision as e:
                    _mark(conn, job_id, rel, "collision", attempts, e)
                    counts["collision"] += 1
                    log(f"KOLLISION {rel}: {e}")
                    break
                except TransferError as e:
                    if attempts >= max_attempts:
                        _mark(conn, job_id, rel, "failed", attempts, e)
                        counts["failed"] += 1
                        log(f"FEHLER {rel}: {e}")
                        break
                    log(f"Prüfung fehlgeschlagen, neuer Versuch {attempts + 1}: {rel}")
                except OSError as e:
                    gone = [m for m in mounts if not m.exists()]
                    if gone:
                        log(f"Datenträger weg ({', '.join(map(str, gone))}), warte bis zu "
                            f"{wait_s / 60:.0f} min ...")
                        if not wait_for(gone, wait_s):
                            _mark(conn, job_id, rel, "failed", attempts, e)
                            with conn:
                                conn.execute("UPDATE job SET state='paused' WHERE id=?", (job_id,))
                            log("Abbruch: Datenträger kam nicht zurück. Job später fortsetzen.")
                            return {**counts, "state": "paused"}
                        time.sleep(5)  # Mount stabilisieren lassen
                        attempts -= 1  # Wegfall der Platte zählt nicht als Versuch
                        continue
                    if e.errno not in _RETRYABLE or attempts >= max_attempts:
                        _mark(conn, job_id, rel, "failed", attempts, e)
                        counts["failed"] += 1
                        log(f"FEHLER {rel}: {e}")
                        break
                    time.sleep(min(60, 2 ** attempts))

    left = conn.execute("SELECT COUNT(*) FROM job_file WHERE job_id=? AND state!='done'",
                        (job_id,)).fetchone()[0]
    final = "done" if left == 0 else "incomplete"
    with conn:
        conn.execute("UPDATE job SET state=?, finished_at=? WHERE id=?", (final, _now(), job_id))
    return {**counts, "state": final, "open": left}


def _mark(conn, job_id, rel, state, attempts, err) -> None:
    with conn:
        conn.execute(
            "UPDATE job_file SET state=?, attempts=attempts+?, error=? WHERE job_id=? AND rel_path=?",
            (state, attempts, str(err), job_id, rel))


# ── CLI ───────────────────────────────────────────────────────────

def _reporter(conn, job_id: int, note: str):
    """Fortschritt (Bytes) als Balken ins Dashboard (best effort, gedrosselt)."""
    from .progress import Reporter
    total, done_b = conn.execute(
        "SELECT COALESCE(SUM(size),0), COALESCE(SUM(CASE WHEN state='done' THEN size ELSE 0 END),0) FROM job_file WHERE job_id=?",
        (job_id,)).fetchone()
    rep = Reporter("kopie", f"job:{job_id}", note or f"Kopie #{job_id}", total_bytes=total)
    state = {"b": done_b}

    def cb(n):
        state["b"] += n
        rep.update(0, state["b"])
    return cb


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m storage_scanner.transfer",
                                 description="Sichere, verifizierte, wiederaufnehmbare Kopie")
    ap.add_argument("--db", help="Journal-DB (Default: Application Support/transfers.db)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("copy", help="Ordner auf 1-2 Ziele kopieren (neuer Job)")
    p.add_argument("src"); p.add_argument("dst", nargs="+")
    p.add_argument("--note", default="")
    p.add_argument("--only-file", help="Textdatei mit relativen Pfaden (eine pro Zeile): nur diese kopieren")
    p = sub.add_parser("resume", help="Job fortsetzen")
    p.add_argument("job_id", type=int)
    sub.add_parser("list", help="Jobs anzeigen")
    args = ap.parse_args(argv)

    conn = connect(args.db)
    if args.cmd == "copy":
        only = None
        if args.only_file:
            only = {l.rstrip("\n") for l in open(args.only_file, encoding="utf-8") if l.strip()}
        job = create_job(conn, Path(args.src), [Path(d) for d in args.dst], args.note, only=only)
        print(f"Job {job} angelegt")
        print(run_job(conn, job, progress=_reporter(conn, job, args.note)))
    elif args.cmd == "resume":
        print(run_job(conn, args.job_id, progress=_reporter(conn, args.job_id, "")))
    elif args.cmd == "list":
        for row in conn.execute(
                """SELECT j.id, j.state, j.src_root, j.dst_roots,
                          SUM(f.state='done'), COUNT(f.rel_path), SUM(f.state!='done')
                   FROM job j LEFT JOIN job_file f ON f.job_id=j.id GROUP BY j.id ORDER BY j.id"""):
            jid, st, src, dst, done, total, open_ = row
            print(f"#{jid} {st:10} {done}/{total} fertig, offen {open_}  {src} -> {dst.replace(chr(10), ' + ')}")
    conn.close()


if __name__ == "__main__":
    main()
