"""Naming-Convention Regex & Validierung für Projektordner."""

import re
from datetime import datetime

# Mapping: Alle erkannten Suffixe → kanonischer Typ
TYPE_ALIASES = {
    "FOOTAGE":  "FOOTAGE",
    "VIDEO":    "FOOTAGE",
    "VIDEOS":   "FOOTAGE",
    "PHOTOS":   "PHOTOS",
    "PHOTO":    "PHOTOS",
    "FOTOS":    "PHOTOS",
    "FOTO":     "PHOTOS",
    "WORKING":  "WORKING",
    "BTS":      "BTS",
    "PROXIES":  "PROXIES",
    "PROXY":    "PROXIES",
    "PROXYS":   "PROXIES",
}

# Alle Suffixe als case-insensitive Alternativen
_type_alternatives = "|".join(TYPE_ALIASES.keys())

PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[_-](.+?)[_-]\s*(" + _type_alternatives + r")\s*$",
    re.IGNORECASE,
)

# Fallback: Datum_Projektname ohne Typ-Suffix = PROJECT (abgeschlossenes Projekt)
PROJECT_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[_-](.+)$"
)


def validate_folder(name: str) -> dict | None:
    """Prüft ob ein Ordnername der Naming-Convention entspricht.

    Returns:
        Dict mit date, project_name, type bei Match; None bei Nicht-Match.
    """
    # Erst spezifische Typen prüfen (FOOTAGE, PHOTOS, WORKING + Aliase)
    match = PATTERN.match(name)
    if match:
        date_str = match.group(1)
        project_name = match.group(2).strip()
        raw_type = match.group(3).upper()
        canonical_type = TYPE_ALIASES.get(raw_type, raw_type)

        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return None

        return {
            "date": date_str,
            "project_name": project_name,
            "type": canonical_type,
        }

    # Fallback: PROJECT-Ordner (Datum_Name, ggf. mit Unterstrichen im Namen)
    # Nur wenn der letzte Teil kein bekannter Typ ist — sonst wäre es oben
    # schon als FOOTAGE/PHOTOS/etc. erkannt worden.
    match = PROJECT_PATTERN.match(name)
    if match:
        date_str = match.group(1)
        project_name = match.group(2).strip()

        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return None

        # Letztes Segment prüfen: wenn es ein bekannter Typ wäre, hätte
        # das PATTERN oben schon gegriffen. Hier landen nur echte Projektnamen.
        last_segment = project_name.rsplit("_", 1)[-1].rsplit("-", 1)[-1].upper()
        if last_segment not in TYPE_ALIASES:
            return {
                "date": date_str,
                "project_name": project_name,
                "type": "PROJECT",
            }

    return None
