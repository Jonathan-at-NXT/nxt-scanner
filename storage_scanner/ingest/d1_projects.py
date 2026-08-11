"""Best-effort Push von Projekt/Session an den zentralen Worker.

Gleiche Mechanik wie `d1_client.push` (Bearer-Token aus lokaler Config, nie
aus dem Repo). Endpunkte: /ingest/project und /ingest/session.
"""

import logging

import httpx

from ..notion_sync import load_config

_log = logging.getLogger("ingest.d1_projects")


def _post(payload: dict, url: str, token: str, route: str,
          timeout: float = 30.0) -> bool:
    if not url or not token:
        return False
    endpoint = url.rstrip("/") + route
    resp = httpx.post(
        endpoint, json=payload,
        headers={"Authorization": f"Bearer {token}"},
        timeout=timeout,
    )
    return 200 <= resp.status_code < 300


def push_project(payload: dict, url: str, token: str) -> bool:
    return _post(payload, url, token, "/ingest/project")


def push_session(payload: dict, url: str, token: str) -> bool:
    return _post(payload, url, token, "/ingest/session")


def _config_url_token():
    config = load_config()
    return config.get("d1_ingest_url", ""), config.get("d1_token", "")


def push_project_safe(payload: dict) -> bool:
    try:
        url, token = _config_url_token()
        if not url or not token:
            return False
        return push_project(payload, url, token)
    except Exception as e:  # noqa: BLE001 - non-blocking
        _log.error("Projekt-Push Fehler: %s", e)
        return False


def push_session_safe(payload: dict) -> bool:
    try:
        url, token = _config_url_token()
        if not url or not token:
            return False
        return push_session(payload, url, token)
    except Exception as e:  # noqa: BLE001 - non-blocking
        _log.error("Session-Push Fehler: %s", e)
        return False
