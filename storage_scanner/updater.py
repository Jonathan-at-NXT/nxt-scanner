"""Mini-Updater – prüft GitHub auf neue Versionen und installiert Updates."""

import json
import logging
import shutil
import ssl
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path
from http.client import IncompleteRead
from urllib.request import urlopen, Request
from urllib.error import URLError

import certifi

from . import __version__

logger = logging.getLogger(__name__)

GITHUB_API_LATEST = "https://api.github.com/repos/Jonathan-at-NXT/nxt-scanner/releases/latest"
GITHUB_RELEASE_BASE = "https://github.com/Jonathan-at-NXT/nxt-scanner/releases/download"
APP_INSTALL_PATH = Path("/Applications/NXT Scanner.app")
_SSL_CTX = ssl.create_default_context(cafile=certifi.where())


def _parse_version(v: str) -> tuple[int, ...]:
    """Parsed '1.2.3' zu (1, 2, 3) für Vergleiche."""
    return tuple(int(x) for x in v.strip().lstrip("v").split("."))


def check_for_update() -> dict | None:
    """Prüft GitHub Releases API auf neue Versionen.

    Returns:
        Dict mit version, download_url, release_notes wenn Update verfügbar.
        None wenn aktuelle Version aktuell ist.

    Raises:
        Exception bei Netzwerk-/API-Fehlern (nicht mehr still geschluckt).
    """
    logger.info(f"Update-Check: certifi={certifi.where()}, version={__version__}")
    req = Request(GITHUB_API_LATEST, headers={
        "User-Agent": "NXT-Scanner-Updater",
        "Accept": "application/vnd.github+json",
    })
    logger.info("Update-Check: urlopen...")
    with urlopen(req, timeout=30, context=_SSL_CTX) as resp:
        logger.info(f"Update-Check: Status {resp.status}")
        data = json.loads(resp.read().decode())

    tag = data.get("tag_name", "")
    remote_version = tag.lstrip("v")
    if _parse_version(remote_version) > _parse_version(__version__):
        return {
            "version": remote_version,
            "download_url": data.get("html_url", ""),
            "release_notes": data.get("body", "Neue Version verfügbar."),
        }
    return None


def install_update(version: str, on_status=None) -> bool:
    """Lädt die neue Version herunter und ersetzt die installierte App.

    Args:
        version: Zielversion (z.B. "1.5.0").
        on_status: Optionaler Callback(status_text) für Fortschritts-Updates.

    Returns:
        True wenn erfolgreich – Caller sollte die App danach neustarten.
    """
    def _status(msg):
        if on_status:
            on_status(msg)

    zip_url = f"{GITHUB_RELEASE_BASE}/v{version}/NXT-Scanner-{version}.zip"

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        zip_path = tmp_path / "update.zip"

        # Download (mit Retry bei Netzwerkfehlern)
        _status(f"Wird heruntergeladen... (v{version})")
        last_error = None
        for attempt in range(3):
            try:
                if attempt > 0:
                    _status(f"Wird heruntergeladen... Versuch {attempt + 1}/3")
                    time.sleep(3)
                req = Request(zip_url, headers={"User-Agent": "NXT-Scanner-Updater"})
                with urlopen(req, timeout=120, context=_SSL_CTX) as resp:
                    # In Chunks lesen statt alles auf einmal
                    with open(zip_path, "wb") as f:
                        while True:
                            chunk = resp.read(1024 * 1024)  # 1 MB
                            if not chunk:
                                break
                            f.write(chunk)
                last_error = None
                break
            except (URLError, OSError, IncompleteRead) as e:
                last_error = e
                logger.info(f"Download-Versuch {attempt + 1} fehlgeschlagen: {e}")
        if last_error:
            raise last_error

        # Entpacken (ditto bewahrt Symlinks, zipfile nicht)
        _status("Entpacken...")
        subprocess.run(
            ["ditto", "-xk", str(zip_path), str(tmp_path)],
            capture_output=True, check=True,
        )

        new_app = tmp_path / "NXT Scanner.app"
        if not new_app.exists():
            raise FileNotFoundError("NXT Scanner.app nicht im ZIP gefunden")

        # Alte App ersetzen
        _status("Installieren...")
        if APP_INSTALL_PATH.exists():
            shutil.rmtree(APP_INSTALL_PATH)
        shutil.copytree(new_app, APP_INSTALL_PATH, symlinks=True)

        # Quarantine entfernen + Execute-Berechtigung sicherstellen
        subprocess.run(["xattr", "-cr", str(APP_INSTALL_PATH)], capture_output=True)
        main_exe = APP_INSTALL_PATH / "Contents" / "MacOS" / "NXT Scanner"
        if main_exe.exists():
            main_exe.chmod(0o755)

    return True
