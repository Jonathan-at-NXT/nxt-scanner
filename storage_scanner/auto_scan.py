"""Auto-Scan – Wird von launchd getriggert wenn sich /Volumes ändert ODER per Timer.

Erkennt neu gemountete Datenträger, scannt sie und synct nach Notion.
Rescannt außerdem alle bekannten Auto-Scan-Volumes stündlich.
"""

import json
import re
import logging
from datetime import datetime
from pathlib import Path

from .paths import KNOWN_VOLUMES_PATH, LAST_SCAN_PATH, LOG_PATH, REPORTS_DIR

RESCAN_INTERVAL_SECONDS = 3600  # 1 Stunde

# Volumes die ignoriert werden sollen (System-Volumes)
IGNORED_VOLUMES = {"Macintosh HD", "Macintosh HD - Data", "Recovery", "Preboot", "VM", "Update"}

# Nur Volumes mit diesen Namensmustern werden automatisch gescannt (case-insensitive)
AUTO_SCAN_PATTERNS = [
    re.compile(r"^nxt\s+\d+$", re.IGNORECASE),       # NXT 005, nxt 007, ...
    re.compile(r"^tower\s+\d+$", re.IGNORECASE),      # TOWER 1, Tower 2, ...
    re.compile(r"^nxt\s+hub\s+\d+$", re.IGNORECASE),  # NXT HUB 1, nxt hub 2, ...
    re.compile(r"^n[hb]\s+\d+$", re.IGNORECASE),        # NH 01, NB 01 (Mehrfach-Gehäuse)
]


def _is_physical_local(name: str) -> bool:
    """Echte, lokal angeschlossene Platte (kein Netzlaufwerk, kein Disk-Image)."""
    import plistlib
    import subprocess
    try:
        r = subprocess.run(["diskutil", "info", "-plist", f"/Volumes/{name}"], capture_output=True, timeout=20)
        info = plistlib.loads(r.stdout) if r.returncode == 0 else {}
    except Exception:  # noqa: BLE001
        return False
    if not info or info.get("VirtualOrPhysical") == "Virtual" or info.get("Internal"):
        return False
    return (info.get("BusProtocol") or "") not in ("Disk Image", "")


def is_auto_scan_volume(name: str) -> bool:
    """Prüft ob ein Volume automatisch gescannt werden soll. Mit `auto_scan_all` in der
    Config (Sicherungs-Mac) zählt jede echte externe Platte, nicht nur die Namensmuster."""
    if any(p.match(name) for p in AUTO_SCAN_PATTERNS):
        return True
    try:
        from .notion_sync import load_config
        if load_config().get("auto_scan_all"):
            return _is_physical_local(name)
    except Exception:  # noqa: BLE001
        pass
    return False

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("auto_scan")


def get_mounted_volumes() -> set[str]:
    """Gibt alle aktuell unter /Volumes gemounteten Volumes zurück."""
    volumes_dir = Path("/Volumes")
    if not volumes_dir.exists():
        return set()
    return {
        entry.name
        for entry in volumes_dir.iterdir()
        if entry.is_dir() and entry.name not in IGNORED_VOLUMES
    }


def load_known_volumes() -> set[str]:
    if KNOWN_VOLUMES_PATH.exists():
        with open(KNOWN_VOLUMES_PATH) as f:
            return set(json.load(f))
    return set()


def save_known_volumes(volumes: set[str]) -> None:
    with open(KNOWN_VOLUMES_PATH, "w") as f:
        json.dump(sorted(volumes), f)


def load_last_scan_times() -> dict[str, str]:
    if LAST_SCAN_PATH.exists():
        with open(LAST_SCAN_PATH) as f:
            return json.load(f)
    return {}


def save_last_scan_times(times: dict[str, str]) -> None:
    with open(LAST_SCAN_PATH, "w") as f:
        json.dump(times, f, indent=2)


def seconds_since_last_scan(volume_name: str, scan_times: dict[str, str]) -> float:
    """Gibt die Sekunden seit dem letzten Scan zurück, oder inf wenn nie gescannt."""
    last = scan_times.get(volume_name)
    if not last:
        return float("inf")
    try:
        last_dt = datetime.fromisoformat(last)
        return (datetime.now() - last_dt).total_seconds()
    except (ValueError, TypeError):
        return float("inf")


def scan_and_sync(volume_name: str) -> bool:
    """Scannt ein Volume (auch offline) und stellt die Uploads in die Queue. True bei Erfolg."""
    from .scan import run_scan

    volume_path = f"/Volumes/{volume_name}"
    report_name = volume_name.replace(" ", "_") + "_report.json"
    report_path = REPORTS_DIR / report_name

    log.info(f"Starte Scan: {volume_path}")

    try:
        from .notion_sync import load_config as _lc
        _cfg = _lc()
        _single = bool(_cfg.get('file_catalog') and _cfg.get('d1_ingest_url') and _cfg.get('d1_token'))
        if _single:
            from .catalog import scan_via_catalog  # ein Durchlauf statt du/find je Ordner
            scan_via_catalog(volume_path, str(report_path))
        else:
            run_scan(volume_path, str(report_path))
        log.info(f"Scan abgeschlossen: {volume_name}")

        from .db import db_ingest_safe
        if db_ingest_safe(str(report_path)):
            log.info(f"DB-Ingest abgeschlossen: {volume_name}")

        # Uploads über die Queue (offline-fest, neuester Scan je Datenträger gewinnt)
        from .notion_sync import load_config
        from . import outbox
        config = load_config()
        outbox.enqueue(str(report_path), channels=outbox.channels_for(config))
        res = outbox.flush_safe()
        log.info(f"Upload-Queue: {res}")

        # Datei-Katalog: nur lokal und nur wenn in der Config aktiviert
        from .health import health_safe
        health_safe(volume_path, load_config())  # Platten-Check (opt-in)
        from .catalog import catalog_safe
        if not _single:
            catalog_safe(volume_path, load_config())
        return True
    except Exception as e:
        log.error(f"Fehler bei {volume_name}: {e}")
        return False


def main():
    current_volumes = get_mounted_volumes()
    known_volumes = load_known_volumes()
    scan_times = load_last_scan_times()

    new_volumes = current_volumes - known_volumes
    scanned = set()

    # 1. Neue Volumes sofort scannen
    for volume_name in sorted(new_volumes):
        if is_auto_scan_volume(volume_name):
            log.info(f"Neuer Datenträger erkannt: {volume_name}")
            if scan_and_sync(volume_name):
                scan_times[volume_name] = datetime.now().isoformat()
                scanned.add(volume_name)
        else:
            log.info(f"Neuer Datenträger ignoriert (kein NXT/Tower/Hub): {volume_name}")

    # 2. Rescan: Alle gemounteten Auto-Scan-Volumes, deren letzter Scan >1h her ist
    for volume_name in sorted(current_volumes):
        if volume_name in scanned:
            continue
        if not is_auto_scan_volume(volume_name):
            continue
        elapsed = seconds_since_last_scan(volume_name, scan_times)
        if elapsed >= RESCAN_INTERVAL_SECONDS:
            log.info(f"Rescan (letzter Scan vor {elapsed / 60:.0f} Min.): {volume_name}")
            if scan_and_sync(volume_name):
                scan_times[volume_name] = datetime.now().isoformat()

    # Uploads früherer Offline-Scans nachholen
    from . import outbox
    if outbox.pending():
        outbox.flush_safe()

    # Alle aktuell gemounteten Volumes als bekannt speichern
    save_known_volumes(current_volumes)
    save_last_scan_times(scan_times)


if __name__ == "__main__":
    main()
