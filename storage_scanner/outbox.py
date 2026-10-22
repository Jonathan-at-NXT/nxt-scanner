"""Upload-Warteschlange: Scans laufen immer (auch offline), Uploads folgen, sobald Netz da ist.

Pro Datenträger steht höchstens EIN Eintrag in der Queue, der auf seinen
Report verweist. Ein neuer Scan desselben Datenträgers überschreibt Report und
Eintrag: hochgeladen wird immer nur der neueste Stand. Kanäle je Eintrag:
"notion" und "d1". Ein Kanal wird erst nach Erfolg abgehakt; Netzfehler
beenden den Durchlauf (offline), andere Fehler werden pro Eintrag gemerkt,
ohne die übrigen zu blockieren.
"""

import json
import logging
import os
import threading
from datetime import datetime
from pathlib import Path

import httpx

from .paths import DATA_DIR

_log = logging.getLogger("outbox")

QUEUE_PATH = DATA_DIR / "upload_queue.json"
CHANNELS = ("notion", "d1")
_lock = threading.RLock()
_flush_lock = threading.Lock()  # nie zwei Durchläufe gleichzeitig


class Offline(Exception):
    """Kein Netz / Gegenstelle nicht erreichbar: Durchlauf abbrechen, später erneut."""


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _load(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def _save(path: Path, queue: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(queue, indent=2, ensure_ascii=False))
    os.replace(tmp, path)  # atomar: nie eine halbe Queue-Datei


def enqueue(report_path: str, user_name: str = "", channels=CHANNELS, path: Path = QUEUE_PATH) -> None:
    """Merkt einen frischen Scan zum Upload vor. Ersetzt einen älteren Eintrag desselben Reports."""
    if not channels:
        return
    key = str(Path(report_path).resolve())
    with _lock:
        queue = _load(path)
        queue[key] = {"report": key, "user_name": user_name, "pending": list(channels),
                      "queued_at": _now(), "attempts": 0, "last_error": None,
                      "generation": queue.get(key, {}).get("generation", 0) + 1}
        _save(path, queue)


def pending(path: Path = QUEUE_PATH) -> dict:
    with _lock:
        return _load(path)


def _is_network_error(e: Exception) -> bool:
    if isinstance(e, (httpx.TransportError, ConnectionError, TimeoutError, Offline)):
        return True
    # Notion-Client nutzt eigene HTTP-Aufrufe; deren Verbindungsfehler tragen typische Texte
    msg = str(e).lower()
    return any(t in msg for t in ("name resolution", "nodename nor servname", "connection",
                                  "timed out", "network is unreachable", "temporary failure"))


def _send_notion(report: str, user_name: str) -> None:
    from .notion_sync import run_sync
    run_sync(report)


def _send_d1(report: str, user_name: str) -> None:
    from .d1_client import push_report
    if not push_report(report, user_name=user_name):
        raise RuntimeError("D1-Push ohne 2xx")


SENDERS = {"notion": _send_notion, "d1": _send_d1}


def channels_for(config: dict) -> list[str]:
    """Nur Kanäle, die auf diesem Mac eingerichtet sind."""
    ch = []
    if config.get("notion_token"):
        ch.append("notion")
    if config.get("d1_ingest_url") and config.get("d1_token"):
        ch.append("d1")
    return ch


def flush(path: Path = QUEUE_PATH, senders: dict | None = None) -> dict:
    """Arbeitet die Queue ab. Gibt {"sent": n, "left": n, "offline": bool} zurück."""
    if not _flush_lock.acquire(blocking=False):
        return {"sent": 0, "left": len(pending(path)), "offline": False, "busy": True}
    try:
        return _flush(path, senders or SENDERS)
    finally:
        _flush_lock.release()


def _flush(path: Path, senders: dict) -> dict:
    sent = 0
    offline = False
    with _lock:
        snapshot = _load(path)
    for key, entry in sorted(snapshot.items(), key=lambda kv: kv[1].get("queued_at", "")):
        if not Path(entry["report"]).exists():
            _finish(path, key, entry["generation"], None, drop=True)
            continue
        for ch in list(entry["pending"]):
            try:
                senders[ch](entry["report"], entry.get("user_name", ""))
            except Exception as e:  # noqa: BLE001 - je Eintrag merken, nicht die Queue blockieren
                if _is_network_error(e):
                    offline = True
                    _log.info("Upload pausiert (offline): %s", e)
                    break
                _log.error("Upload %s fehlgeschlagen fuer %s: %s", ch, key, e)
                _finish(path, key, entry["generation"], ch, error=str(e))
                continue
            _finish(path, key, entry["generation"], ch)
            sent += 1
        if offline:
            break
    with _lock:
        left = len(_load(path))
    return {"sent": sent, "left": left, "offline": offline}


def _finish(path: Path, key: str, generation: int, channel, error: str | None = None,
            drop: bool = False) -> None:
    """Kanal abhaken, aber nur, wenn inzwischen kein neuerer Scan den Eintrag ersetzt hat."""
    with _lock:
        queue = _load(path)
        cur = queue.get(key)
        if not cur or cur.get("generation") != generation:
            return  # neuerer Scan wartet schon -> der wird separat hochgeladen
        if drop:
            queue.pop(key)
        elif error is not None:
            cur["attempts"] += 1
            cur["last_error"] = f"{channel}: {error}"[:500]
        else:
            cur["pending"] = [c for c in cur["pending"] if c != channel]
            if not cur["pending"]:
                queue.pop(key)
        _save(path, queue)


def flush_safe() -> dict | None:
    """Best-effort für Timer/Auto-Scan. Schluckt jeden Fehler."""
    try:
        res = flush()
        if res["sent"]:
            _log.info("Upload-Queue: %s", res)
        return res
    except Exception as e:  # noqa: BLE001
        _log.error("Upload-Queue Fehler: %s", e)
        return None
