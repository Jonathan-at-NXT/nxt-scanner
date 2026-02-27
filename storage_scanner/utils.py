"""Hilfsfunktionen – Formatierung etc."""

import plistlib
import subprocess


def format_size(size_bytes: int) -> str:
    """Konvertiert Bytes in menschenlesbare Größe (SI-Einheiten, wie Finder)."""
    if size_bytes < 1000:
        return f"{size_bytes} B"

    for unit in ("KB", "MB", "GB", "TB"):
        size_bytes /= 1000
        if size_bytes < 1000 or unit == "TB":
            return f"{size_bytes:.1f} {unit}"


def bytes_to_gb(size_bytes: int) -> float:
    """Konvertiert Bytes nach GB (SI, wie Finder, gerundet auf 2 Dezimalstellen)."""
    return round(size_bytes / (1000 ** 3), 2)


def get_volume_uuid(path: str) -> str:
    """Ermittelt die VolumeUUID eines Datenträgers via diskutil."""
    try:
        result = subprocess.run(
            ["diskutil", "info", "-plist", path],
            capture_output=True,
        )
        if result.returncode == 0:
            info = plistlib.loads(result.stdout)
            return info.get("VolumeUUID", "")
    except Exception:
        pass
    return ""
