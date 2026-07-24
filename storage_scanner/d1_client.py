"""Push von Scan-Payloads an den zentralen D1 Ingest-Worker.

Best-effort: jeder Fehler wird geloggt und geschluckt — der Scan/Notion-Ablauf
darf nie blockieren. URL + Token kommen aus der lokalen Config (manuell
eingetragen), nie aus dem Repo.
"""

import json
import logging
from pathlib import Path

import httpx

from .notion_sync import load_config

_log = logging.getLogger("d1_client")


def push(payload: dict, url: str, token: str, timeout: float = 60.0) -> bool:
    """POST {disk, folder_tree} an <url>/ingest mit Bearer-Token. True bei 2xx."""
    if not url or not token:
        return False
    endpoint = url.rstrip("/") + "/ingest"
    resp = httpx.post(
        endpoint, json=payload,
        headers={"Authorization": f"Bearer {token}"},
        timeout=timeout,
    )
    return 200 <= resp.status_code < 300


def push_safe(report_path: str, user_name: str = "") -> bool:
    """Liest Report, baut Payload, pusht nach D1. Schluckt jeden Fehler.

    Ist keine D1-Config gesetzt (d1_ingest_url/d1_token), wird still übersprungen.
    """
    try:
        from .db import build_payload

        config = load_config()
        url = config.get("d1_ingest_url", "")
        token = config.get("d1_token", "")
        if not url or not token:
            return False  # kein Push konfiguriert -> lokal reicht

        report = json.loads(Path(report_path).read_text())
        payload = build_payload(report, user_name=user_name or config.get("user_name", ""))
        ok = push(payload, url, token)
        if ok:
            _log.info("D1-Push ok: %s (%d Knoten)", payload["disk"]["name"], len(payload["folder_tree"]))
        else:
            _log.warning("D1-Push fehlgeschlagen (kein 2xx): %s", report_path)
        return ok
    except Exception as e:  # noqa: BLE001 - non-blocking
        _log.error("D1-Push Fehler fuer %s: %s", report_path, e)
        return False
