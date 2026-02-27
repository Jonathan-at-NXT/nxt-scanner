"""Mini-Updater – prüft GitHub auf neue Versionen."""

import json
import logging
from urllib.request import urlopen, Request
from urllib.error import URLError

from . import __version__

logger = logging.getLogger(__name__)

VERSION_URL = "https://raw.githubusercontent.com/Jonathan-at-NXT/nxt-scanner/main/version.json"


def _parse_version(v: str) -> tuple[int, ...]:
    """Parsed '1.2.3' zu (1, 2, 3) für Vergleiche."""
    return tuple(int(x) for x in v.strip().split("."))


def check_for_update() -> dict | None:
    """Prüft remote version.json. Gibt Update-Info zurück oder None.

    Returns:
        Dict mit version, download_url, release_notes wenn Update verfügbar.
        None wenn aktuelle Version aktuell ist oder Check fehlschlägt.
    """
    try:
        req = Request(VERSION_URL, headers={"User-Agent": "NXT-Scanner-Updater"})
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())

        remote_version = data.get("version", "")
        if _parse_version(remote_version) > _parse_version(__version__):
            return {
                "version": remote_version,
                "download_url": data.get("download_url", ""),
                "release_notes": data.get("release_notes", ""),
            }
    except (URLError, json.JSONDecodeError, ValueError, OSError) as e:
        logger.debug(f"Update-Check fehlgeschlagen: {e}")

    return None
