"""Mini-Updater – prüft GitHub auf neue Versionen und installiert Updates."""

import json
import logging
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

from . import __version__

logger = logging.getLogger(__name__)

GITHUB_API_LATEST = "https://api.github.com/repos/Jonathan-at-NXT/nxt-scanner/releases/latest"
GITHUB_RELEASE_BASE = "https://github.com/Jonathan-at-NXT/nxt-scanner/releases/download"
APP_INSTALL_PATH = Path("/Applications/NXT Scanner.app")


def _parse_version(v: str) -> tuple[int, ...]:
    """Parsed '1.2.3' zu (1, 2, 3) für Vergleiche."""
    return tuple(int(x) for x in v.strip().lstrip("v").split("."))


def check_for_update() -> dict | None:
    """Prüft GitHub Releases API auf neue Versionen.

    Returns:
        Dict mit version, download_url, release_notes wenn Update verfügbar.
        None wenn aktuelle Version aktuell ist oder Check fehlschlägt.
    """
    try:
        req = Request(GITHUB_API_LATEST, headers={
            "User-Agent": "NXT-Scanner-Updater",
            "Accept": "application/vnd.github+json",
        })
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())

        tag = data.get("tag_name", "")
        remote_version = tag.lstrip("v")
        if _parse_version(remote_version) > _parse_version(__version__):
            return {
                "version": remote_version,
                "download_url": data.get("html_url", ""),
                "release_notes": data.get("body", "Neue Version verfügbar."),
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
