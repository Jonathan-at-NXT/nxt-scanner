"""Mini-Updater – prüft GitHub auf neue Versionen und installiert Updates."""

import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

from . import __version__

logger = logging.getLogger(__name__)

VERSION_URL = "https://raw.githubusercontent.com/Jonathan-at-NXT/nxt-scanner/main/version.json"
GITHUB_RELEASE_BASE = "https://github.com/Jonathan-at-NXT/nxt-scanner/releases/download"
APP_INSTALL_PATH = Path("/Applications/NXT Scanner.app")


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
        url = f"{VERSION_URL}?t={int(time.time())}"
        req = Request(url, headers={"User-Agent": "NXT-Scanner-Updater", "Cache-Control": "no-cache"})
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


def install_update(version: str) -> bool:
    """Lädt die neue Version herunter und ersetzt die installierte App.

    Returns:
        True wenn erfolgreich – Caller sollte die App danach neustarten.
    """
    zip_url = f"{GITHUB_RELEASE_BASE}/v{version}/NXT-Scanner-{version}.zip"

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        zip_path = tmp_path / "update.zip"

        # Download
        req = Request(zip_url, headers={"User-Agent": "NXT-Scanner-Updater"})
        with urlopen(req, timeout=120) as resp:
            zip_path.write_bytes(resp.read())

        # Entpacken
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_path)

        new_app = tmp_path / "NXT Scanner.app"
        if not new_app.exists():
            raise FileNotFoundError("NXT Scanner.app nicht im ZIP gefunden")

        # Quarantine-Attribut entfernen
        subprocess.run(["xattr", "-cr", str(new_app)], capture_output=True)

        # Alte App ersetzen
        if APP_INSTALL_PATH.exists():
            shutil.rmtree(APP_INSTALL_PATH)
        shutil.copytree(new_app, APP_INSTALL_PATH, symlinks=True)

        # Quarantine auch von der installierten Version entfernen
        subprocess.run(["xattr", "-cr", str(APP_INSTALL_PATH)], capture_output=True)

    return True
