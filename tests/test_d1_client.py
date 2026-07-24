import json
import httpx
from storage_scanner import d1_client


class _Resp:
    def __init__(self, code): self.status_code = code


def test_push_success(monkeypatch):
    seen = {}

    def fake_post(endpoint, json=None, headers=None, timeout=None):
        seen["endpoint"] = endpoint
        seen["auth"] = headers["Authorization"]
        return _Resp(200)

    monkeypatch.setattr(httpx, "post", fake_post)
    ok = d1_client.push({"disk": {"uuid": "U", "name": "X"}, "folder_tree": []},
                        "https://w.example.dev", "tok123")
    assert ok is True
    assert seen["endpoint"] == "https://w.example.dev/ingest"
    assert seen["auth"] == "Bearer tok123"


def test_push_missing_config_returns_false():
    assert d1_client.push({}, "", "") is False


def test_push_safe_skips_without_config(tmp_path, monkeypatch):
    # Config ohne d1_* -> Push wird still uebersprungen
    monkeypatch.setattr(d1_client, "load_config", lambda: {"user_name": "J"})
    rp = tmp_path / "r.json"
    rp.write_text(json.dumps({"scan_info": {"scanned_path": "/Volumes/X", "volume_uuid": "U"},
                              "projects": [], "unassigned": [], "folder_tree": []}))
    assert d1_client.push_safe(str(rp)) is False


def test_push_safe_swallows_errors(monkeypatch):
    monkeypatch.setattr(d1_client, "load_config", lambda: {"d1_ingest_url": "u", "d1_token": "t"})
    # Report-Datei fehlt -> Fehler wird geschluckt
    assert d1_client.push_safe("/nonexistent/r.json") is False
