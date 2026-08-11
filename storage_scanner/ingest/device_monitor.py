"""Erkennt gemountete Volumes und klassifiziert Karte vs. Ziel-Platte.

Nutzt `diskutil info -plist` für Removable-/Größen-Info. Die reine
Klassifikationslogik (`classify`) ist ohne diskutil testbar.
"""

import logging
import plistlib
import subprocess
from pathlib import Path

from . import camera_map

_log = logging.getLogger("ingest.device_monitor")


def _volumes_root() -> Path:
    return Path("/Volumes")


def _volume_info(path: Path) -> dict | None:
    """diskutil-Info zu einem Mountpoint: name, path, is_removable, total_bytes."""
    try:
        out = subprocess.run(
            ["diskutil", "info", "-plist", str(path)],
            capture_output=True, timeout=15,
        ).stdout
        info = plistlib.loads(out)
    except Exception as e:  # noqa: BLE001
        _log.warning("diskutil info fehlgeschlagen für %s: %s", path, e)
        return None
    return {
        "name": info.get("VolumeName") or path.name,
        "path": str(path),
        "is_removable": bool(info.get("RemovableMedia") or info.get("Ejectable")),
        "total_bytes": int(info.get("TotalSize") or 0),
    }


def list_volumes() -> list[dict]:
    """Alle gemounteten /Volumes/* mit Info-Dict."""
    vols = []
    root = _volumes_root()
    if not root.is_dir():
        return vols
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        info = _volume_info(entry)
        if info:
            vols.append(info)
    return vols


def classify(vol: dict) -> str:
    """`card` (Wechselmedium mit Kamera-Signatur) / `target` (feste Platte) / `other`."""
    path = Path(vol["path"])
    if vol.get("is_removable") and camera_map.detect_vendor(path) is not None:
        return "card"
    if not vol.get("is_removable"):
        return "target"
    return "other"
