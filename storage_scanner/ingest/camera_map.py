"""Kamera-Karten-Struktur-Map: Hersteller-Detection, Media-Discovery,
Ableitung der "CAMERA INITIALEN" aus Dateinamen.

Datengetrieben und erweiterbar. Media-Discovery klassifiziert nach Extension
(nicht nach Ordner) und findet damit auch tiefe/unbekannte Kartenstrukturen
(z. B. Sony-Video unter PRIVATE/M4ROOT/CLIP).
"""

import os
import re
from pathlib import Path

# Medien-Extensions (lowercase, mit Punkt)
VIDEO_EXTS = {
    ".mp4", ".mov", ".mxf", ".braw", ".avi", ".m4v", ".mts", ".m2ts",
    ".mpg", ".mpeg", ".r3d", ".insv", ".lrv",
}
PHOTO_EXTS = {
    ".jpg", ".jpeg", ".png", ".heic", ".heif", ".tif", ".tiff", ".dng",
    ".raw", ".arw", ".cr2", ".cr3", ".nef", ".raf", ".rw2", ".orf", ".insp",
}

# Root-Signaturen -> Hersteller. Reihenfolge: spezifisch vor generisch.
# Jede Regel: (relativer Ordner-Glob als Teilpfad-Muster, Herstellername)
_VENDOR_SIGNATURES = [
    ("PRIVATE/M4ROOT", "Sony"),
    ("PRIVATE/SONY", "Sony"),
    ("XDROOT", "Sony"),
    ("DCIM/100GOPRO", "GoPro"),
    ("CONTENTS/CLIPS", "Canon"),
    ("DCIM/CANONMSC", "Canon"),
    ("PRIVATE/AVCHD", "Panasonic"),
    ("CONTENTS/VIDEO", "Panasonic"),
]

# Kamera-Default-Präfixe: wenn der abgeleitete Name so aussieht, ist es KEIN
# echter Name -> UI muss nachfragen.
DEFAULT_PREFIXES = (
    "C", "MVI_", "MVI", "DSC", "DJI_", "DJI", "GX", "GH", "GOPR", "GOPRO",
    "IMG_", "IMG", "A", "CLIP", "MAH", "P", "PANA",
)


def discover_media(root: Path) -> dict:
    """Rekursiv alle Medien unter root finden, nach Video/Foto klassifiziert."""
    root = Path(root)
    video: list[Path] = []
    photo: list[Path] = []
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            ext = os.path.splitext(name)[1].lower()
            if ext in VIDEO_EXTS:
                video.append(Path(dirpath) / name)
            elif ext in PHOTO_EXTS:
                photo.append(Path(dirpath) / name)
    video.sort()
    photo.sort()
    return {"video": video, "photo": photo}


def detect_vendor(root: Path) -> str | None:
    """Erkennt den Hersteller anhand vorhandener Root-Ordner-Signaturen."""
    root = Path(root)
    for rel, vendor in _VENDOR_SIGNATURES:
        if (root / rel).exists():
            return vendor
    # DJI: DCIM/DJI_* oder Dateien mit DJI_-Präfix
    dcim = root / "DCIM"
    if dcim.is_dir():
        for sub in dcim.iterdir():
            if sub.name.upper().startswith("DJI_"):
                return "DJI"
    return None


def _name_prefix(filename: str) -> str:
    """Namensteil ohne die hintere Zähl-Ziffernfolge (ohne Extension).

    "FX3 JO00001" -> "FX3 JO"  (nur die trailing-Zähler weg, "3" bleibt)
    "C0001" -> "C"; "DSC0001" -> "DSC"; "GX010001" -> "GX".
    """
    stem = os.path.splitext(filename)[0]
    return re.sub(r"\d+$", "", stem).rstrip()


def _is_default(prefix: str) -> bool:
    up = prefix.upper().strip()
    if not up:
        return True
    for d in DEFAULT_PREFIXES:
        if up == d.upper():
            return True
    return False


def infer_camera_name(files: list[Path]) -> str | None:
    """Gemeinsames Datei-Präfix als "CAMERA INITIALEN" ableiten.

    Gibt den Namen nur zurück, wenn alle Dateien dasselbe Präfix teilen und
    dieses KEIN reines Kamera-Default ist. Sonst None -> UI fragt nach.
    """
    if not files:
        return None
    prefixes = {_name_prefix(Path(f).name) for f in files}
    if len(prefixes) != 1:
        return None
    prefix = next(iter(prefixes))
    if _is_default(prefix):
        return None
    return prefix
