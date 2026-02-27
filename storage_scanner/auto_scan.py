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
]


def is_auto_scan_volume(name: str) -> bool:
    """Prüft ob ein Volume automatisch gescannt werden soll."""
    return any(p.match(name) for p in AUTO_SCAN_PATTERNS)

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
    """Führt Scan und Notion-Sync für ein Volume aus. Gibt True bei Erfolg zurück."""
    from .scan import run_scan
    from .notion_sync import run_sync

    volume_path = f"/Volumes/{volume_name}"
    report_name = volume_name.replace(" ", "_") + "_report.json"
    report_path = REPORTS_DIR / report_name

    log.info(f"Starte Scan: {volume_path}")

    try:
        run_scan(volume_path, str(report_path))
        log.info(f"Scan abgeschlossen: {volume_name}")

        run_sync(str(report_path))
        log.info(f"Notion-Sync abgeschlossen: {volume_name}")
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

    # Alle aktuell gemounteten Volumes als bekannt speichern
    save_known_volumes(current_volumes)
    save_last_scan_times(scan_times)


if __name__ == "__main__":
    main()
