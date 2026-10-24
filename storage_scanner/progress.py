"""Fortschritt laufender Arbeiten (Scan, Inventur, Hash, Kopie) ans Dashboard melden.

Best-effort: höchstens alle `interval` Sekunden ein kurzer POST an <d1_ingest_url>/ingest/progress.
Fehler (offline, kein Token) werden still ignoriert – Fortschritt ist nur Anzeige.

CLI (z. B. für eine Inventur, die ohne eingebaute Meldung läuft):
    python -m storage_scanner.progress watch --uuid "nas:NXTCLOUD/NXT STUDIOS" --name "NXT STUDIOS" --expected-files 503792
"""

import argparse
import socket
import time
from datetime import datetime


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _post(payload: dict) -> bool:
    import os
    if os.environ.get("NXT_NO_PROGRESS"):   # Tests/Offline: nie echte Meldungen senden
        return False
    try:
        import httpx
        from .notion_sync import load_config
        c = load_config()
        url, token = c.get("d1_ingest_url", ""), c.get("d1_token", "")
        if not url or not token:
            return False
        r = httpx.post(url.rstrip("/") + "/ingest/progress", json=payload,
                       headers={"Authorization": f"Bearer {token}"}, timeout=5)
        return 200 <= r.status_code < 300
    except Exception:  # noqa: BLE001 - reine Anzeige, darf nie stören
        return False


class Reporter:
    """Gedrosselter Melder für einen Job (kind = scan | inventur | hash | kopie | lesetest)."""

    def __init__(self, kind: str, disk_uuid: str, disk_name: str, total_files: int | None = None,
                 total_bytes: int | None = None, interval: float = 10.0, sender=_post):
        self.base = {"job_id": f"{socket.gethostname()}|{disk_uuid}|{kind}", "host": socket.gethostname(),
                     "kind": kind, "disk_uuid": disk_uuid, "disk_name": disk_name, "started_at": _now(),
                     "total_files": total_files, "total_bytes": total_bytes}
        self.interval, self.sender, self._last = interval, sender, 0.0

    def update(self, done_files: int, done_bytes: int, message: str | None = None, force: bool = False) -> None:
        t = time.monotonic()
        if not force and t - self._last < self.interval:
            return
        self._last = t
        self.sender({**self.base, "done_files": done_files, "done_bytes": done_bytes, "state": "running",
                     "message": message, "updated_at": _now()})

    def finish(self, done_files: int, done_bytes: int, ok: bool = True, message: str | None = None) -> None:
        self.sender({**self.base, "done_files": done_files, "done_bytes": done_bytes,
                     "state": "done" if ok else "failed", "message": message, "updated_at": _now()})


def watch_catalog(uuid: str, name: str, expected_files: int | None = None, interval: float = 15.0,
                  db_path=None) -> None:
    """Meldet den Fortschritt einer laufenden Inventur anhand der lokalen Katalog-DB,
    bis für diese Kennung ein neuer Inventur-Lauf protokolliert ist."""
    from .catalog import connect
    conn = connect(db_path)
    last_run = conn.execute("SELECT COALESCE(MAX(id), 0) FROM run").fetchone()[0]
    rep = Reporter("inventur", uuid, name, total_files=expected_files, interval=0)
    while True:
        # Die laufende Inventur schreibt in 5000er-Blöcken; Zeilenzahl = Fortschritt
        files, size = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(size),0) FROM file WHERE volume_uuid=? AND present=1", (uuid,)).fetchone()
        done = conn.execute("SELECT files, complete FROM run WHERE id>? AND volume_uuid=? AND kind='inventory' "
                            "ORDER BY id DESC LIMIT 1", (last_run, uuid)).fetchone()
        if done:
            rep.finish(done[0], size, ok=bool(done[1]), message=None if done[1] else "unvollständig")
            print(f"fertig: {done[0]} Dateien, vollständig={bool(done[1])}")
            return
        rep.update(files, size, force=True)
        print(f"{files} Dateien, {size / 1e12:.2f} TB", flush=True)
        time.sleep(interval)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m storage_scanner.progress")
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("watch", help="Fortschritt einer laufenden Inventur melden")
    w.add_argument("--uuid", required=True); w.add_argument("--name", required=True)
    w.add_argument("--expected-files", type=int)
    w.add_argument("--interval", type=float, default=15.0)
    args = ap.parse_args(argv)
    if args.cmd == "watch":
        watch_catalog(args.uuid, args.name, args.expected_files, args.interval)


if __name__ == "__main__":
    main()
