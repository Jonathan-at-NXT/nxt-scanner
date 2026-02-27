"""Hilfsfunktionen – Formatierung etc."""

import plistlib
import subprocess


def format_size(size_bytes: int) -> str:
    """Konvertiert Bytes in menschenlesbare Größe (B, KB, MB, GB, TB)."""
    if size_bytes < 1024:
        return f"{size_bytes} B"

    for unit in ("KB", "MB", "GB", "TB"):
        size_bytes /= 1024
        if size_bytes < 1024 or unit == "TB":
            return f"{size_bytes:.1f} {unit}"


def bytes_to_gb(size_bytes: int) -> float:
    """Konvertiert Bytes nach GB (gerundet auf 2 Dezimalstellen)."""
    return round(size_bytes / (1024 ** 3), 2)


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
