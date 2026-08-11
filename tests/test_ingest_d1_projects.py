import httpx

from storage_scanner.ingest import d1_projects


class _Resp:
    def __init__(self, code):
        self.status_code = code


def test_push_project_endpoint_and_auth(monkeypatch):
    seen = {}

    def fake_post(endpoint, json=None, headers=None, timeout=None):
        seen["endpoint"] = endpoint
        seen["auth"] = headers["Authorization"]
        seen["body"] = json
        return _Resp(200)

    monkeypatch.setattr(httpx, "post", fake_post)
    ok = d1_projects.push_project({"id": "p1", "name": "X"},
                                  "https://w.example.dev", "tok")
    assert ok is True
    assert seen["endpoint"] == "https://w.example.dev/ingest/project"
    assert seen["auth"] == "Bearer tok"
    assert seen["body"]["id"] == "p1"


def test_push_session_endpoint(monkeypatch):
    seen = {}

    def fake_post(endpoint, **k):
        seen["e"] = endpoint
        return _Resp(200)

    monkeypatch.setattr(httpx, "post", fake_post)
    d1_projects.push_session({"id": "s1"}, "https://w.example.dev", "tok")
    assert seen["e"] == "https://w.example.dev/ingest/session"


def test_push_missing_token_no_request():
    assert d1_projects.push_project({"id": "p"}, "", "") is False


def test_push_project_safe_skips_without_config(monkeypatch):
    monkeypatch.setattr(d1_projects, "load_config", lambda: {"user_name": "J"})
    assert d1_projects.push_project_safe({"id": "p"}) is False
