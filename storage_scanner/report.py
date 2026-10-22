"""JSON-Report generieren und speichern."""

import json
import shutil
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .utils import get_volume_uuid


def generate_report(scanned_path: str, projects: list[dict], unassigned: list[dict]) -> dict:
    """Erstellt einen strukturierten Scan-Report.

    Args:
        scanned_path: Der gescannte Pfad.
        projects: Liste validierter Projektordner.
        unassigned: Liste ungültiger Ordner.

    Returns:
        Strukturierter Report als Dict.
    """
    return {
        "scan_info": {
            "scanned_path": scanned_path,
            "scan_date": datetime.now(ZoneInfo("Europe/Berlin")).isoformat(),
            "total_folders": len(projects) + len(unassigned),
            "valid_folders": len(projects),
            "unassigned_folders": len(unassigned),
            "volume_uuid": get_volume_uuid(scanned_path),
            "disk_usage": _disk_usage(scanned_path),
        },
        "projects": projects,
        "unassigned": unassigned,
    }


def _disk_usage(path: str) -> dict | None:
    """Kapazität zum Scan-Zeitpunkt, damit ein späterer (Offline-)Sync korrekte Werte hat."""
    try:
        u = shutil.disk_usage(path)
        return {"total_bytes": u.total, "used_bytes": u.used, "free_bytes": u.free}
    except OSError:
        return None


def save_report(report: dict, output_path: Path) -> None:
    """Schreibt den Report als JSON-Datei."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
