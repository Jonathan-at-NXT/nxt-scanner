"""Pfad-Auflösung für NXT Scanner.

Trennt Code-Verzeichnis (read-only im .app-Bundle) von User-Daten
(~/Library/Application Support/NXT Scanner/).
"""

import shutil
import sys
from pathlib import Path

APP_NAME = "NXT Scanner"
BUNDLE_ID = "com.nxtstudios.nxt-scanner"


def is_frozen() -> bool:
    """True wenn als PyInstaller-Bundle ausgeführt."""
    return getattr(sys, "frozen", False)


def get_resource_path(filename: str) -> Path:
    """Gibt den Pfad zu einer gebündelten Resource-Datei zurück."""
    if is_frozen():
        # PyInstaller: Resources liegen neben der Executable
        return Path(sys._MEIPASS) / "resources" / filename
    # Entwicklung: resources/ im Projekt-Root
    return Path(__file__).resolve().parent.parent / "resources" / filename


def get_data_dir() -> Path:
    """Beschreibbares Verzeichnis für Config, Logs, Reports."""
    data_dir = Path.home() / "Library" / "Application Support" / APP_NAME
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


DATA_DIR = get_data_dir()

# Datei-Pfade
CONFIG_PATH = DATA_DIR / ".notion_config.json"
LOG_PATH = DATA_DIR / "auto_scan.log"
LAST_SCAN_PATH = DATA_DIR / ".last_scan_times.json"
KNOWN_VOLUMES_PATH = DATA_DIR / ".known_volumes.json"
REPORTS_DIR = DATA_DIR / "reports"


def ensure_dirs() -> None:
    """Erstellt alle benötigten Unterverzeichnisse."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def migrate_legacy_data() -> None:
    """Einmalige Migration von ~/Desktop/SCANS/storage_scanner/ nach Application Support."""
    legacy_dir = Path.home() / "Desktop" / "SCANS" / "storage_scanner"
    marker = DATA_DIR / ".migrated"

    if marker.exists() or not legacy_dir.exists():
        return

    migrations = [
        (".notion_config.json", CONFIG_PATH),
        (".last_scan_times.json", LAST_SCAN_PATH),
        (".known_volumes.json", KNOWN_VOLUMES_PATH),
        ("auto_scan.log", LOG_PATH),
    ]

    for old_name, new_path in migrations:
        old_path = legacy_dir / old_name
        if old_path.exists() and not new_path.exists():
            shutil.copy2(old_path, new_path)

    # Report-Dateien migrieren
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    for report in legacy_dir.glob("*_report.json"):
        dest = REPORTS_DIR / report.name
        if not dest.exists():
            shutil.copy2(report, dest)

    marker.touch()
