"""NXT Data MCP: JSON-RPC-Handling, Ausgabe und idempotentes Eintragen in Claude Desktop."""
import io
import json

from storage_scanner import mcp_connect, mcp_server


def _rpc(*reqs):
    out = io.StringIO()
    mcp_server.main(stdin=io.StringIO("\n".join(json.dumps(r) for r in reqs) + "\n"), stdout=out)
    return [json.loads(l) for l in out.getvalue().splitlines()]


def test_initialize_list_and_notification():
    res = _rpc({"jsonrpc": "2.0", "id": 1, "method": "initialize"},
               {"jsonrpc": "2.0", "method": "notifications/initialized"},
               {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    assert [r["id"] for r in res] == [1, 2]                     # Benachrichtigung ohne Antwort
    assert {t["name"] for t in res[1]["result"]["tools"]} == {"check_folder", "check_disk", "list_volumes"}


def test_check_folder_formats_verdict(monkeypatch):
    seen = {}

    def fake(disk, folder, token=None):
        seen.update(disk=disk, folder=folder)
        return {"verdict": "nicht_loeschen", "grund": "nur 1 vollständige Kopie(n)",
                "source": {"disk": disk, "folder": folder, "files": 3, "bytes": 3e9},
                "complete_copies": [{"device": "NAS", "path": "NXT STUDIOS/X"}],
                "partial_copies": [{"device": "TOWER 11", "missing": 2, "missing_bytes": 2e9}],
                "unique_here": {"files": 1, "sample": ["a.mov"]}, "hinweis": "nicht hash-verifiziert"}
    monkeypatch.setattr(mcp_server, "query_redundancy", fake)
    res = _rpc({"jsonrpc": "2.0", "id": 5, "method": "tools/call",
                "params": {"name": "check_folder", "arguments": {"path": "/Volumes/NXT 041/MF #1/"}}})
    text = res[0]["result"]["content"][0]["text"]
    assert seen == {"disk": "NXT 041", "folder": "MF #1"}
    assert "NICHT LÖSCHEN" in text and "a.mov" in text and "zählt NICHT" in text
    assert "isError" not in res[0]["result"]


def test_tool_error_is_reported_not_raised():
    res = _rpc({"jsonrpc": "2.0", "id": 7, "method": "tools/call",
                "params": {"name": "check_folder", "arguments": {"path": "/tmp/x"}}})
    assert res[0]["result"]["isError"] is True


def test_desktop_config_merge_idempotent(tmp_path):
    cfg = tmp_path / "claude_desktop_config.json"
    cfg.write_text(json.dumps({"mcpServers": {"other": {"command": "x"}}, "theme": "dark"}))
    cmd = ["/Applications/NXT Scanner.app/Contents/MacOS/NXT Scanner", "--mcp"]
    mcp_connect.connect_claude_desktop(cmd, path=cfg)
    first = cfg.read_text()
    mcp_connect.connect_claude_desktop(cmd, path=cfg)
    assert cfg.read_text() == first                                # zweites Mal: unverändert
    data = json.loads(first)
    assert data["theme"] == "dark" and data["mcpServers"]["other"] == {"command": "x"}
    assert data["mcpServers"]["nxt-data"] == {"command": cmd[0], "args": ["--mcp"], "env": {"NXT_DATA_MODE": "remote"}}
    assert (tmp_path / "claude_desktop_config.json.vor-nxt-data").exists()


def test_claude_code_remove_then_add():
    calls = []

    class R:
        returncode, stdout, stderr = 0, "", ""

    def run(args, **kw):
        calls.append(args)
        return R()
    mcp_connect.connect_claude_code("/bin/claude", ["/app/NXT Scanner", "--mcp"], run=run)
    assert calls[0][1:4] == ["mcp", "remove", "nxt-data"]
    assert calls[1][1:3] == ["mcp", "add"] and calls[1][-3:] == ["--", "/app/NXT Scanner", "--mcp"]


def test_update_also_replaces_running_copy_elsewhere(monkeypatch, tmp_path):
    from storage_scanner import updater
    app = tmp_path / "Downloads" / "NXT Scanner.app"
    monkeypatch.setattr(updater.sys, "frozen", True, raising=False)
    monkeypatch.setattr(updater.sys, "executable", str(app / "Contents/MacOS/NXT Scanner"))
    assert updater.install_targets() == [updater.APP_INSTALL_PATH, app.resolve()]
    monkeypatch.setattr(updater.sys, "executable", "/Volumes/NXT Scanner/NXT Scanner.app/Contents/MacOS/NXT Scanner")
    assert updater.install_targets() == [updater.APP_INSTALL_PATH]    # aus dem DMG: nicht dorthin schreiben
