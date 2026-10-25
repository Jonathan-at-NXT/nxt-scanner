import json

from storage_scanner import catalog, dropbox_catalog as dc


def _fake_api(pages):
    calls = []

    def call(endpoint, body, token, path_root=None, tries=8):
        calls.append((endpoint, path_root))
        if endpoint == "/users/get_current_account":
            return {"root_info": {"root_namespace_id": "42"}}
        i = 0 if endpoint == "/files/list_folder" else int(body["cursor"])
        return {**pages[i], "cursor": str(i + 1), "has_more": i + 1 < len(pages)}
    return call, calls


def test_fetch_apply_replaces_stat_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(dc, "OUT", tmp_path / "l.jsonl")
    monkeypatch.setattr(dc, "STATE", tmp_path / "s.json")
    pages = [{"entries": [{".tag": "folder", "path_display": "/NXT STUDIOS"},
                          {".tag": "file", "path_display": "/NXT STUDIOS/A/clip.mov", "size": 5_000_000,
                           "server_modified": "2025-01-02T03:04:05Z", "content_hash": "h1"}]},
             {"entries": [{".tag": "file", "path_display": "/NXT STUDIOS/B/foto.jpg", "size": 2_000_000,
                           "server_modified": "2025-01-02T03:04:05Z", "content_hash": "h2"}]}]
    call, calls = _fake_api(pages)
    monkeypatch.setattr(dc, "call", call)
    conn = catalog.connect(tmp_path / "c.db")
    # alter stat-Stand: Platzhalter mit 0 Byte + eine Datei, die online nicht mehr existiert
    conn.execute("INSERT INTO file(volume_uuid, rel_path, name, kind, size, mtime, present, changed_at) VALUES"
                 "('dropbox:NXT','NXT STUDIOS/A/clip.mov','clip.mov','video',0,1,1,'x'),"
                 "('dropbox:NXT','NXT STUDIOS/alt.mov','alt.mov','video',9,1,1,'x')")
    conn.commit()
    s = dc.fetch(token="t")
    assert s == {"files": 2, "bytes": 7_000_000, "zero": 0}
    assert all(pr == "42" for ep, pr in calls if ep.startswith("/files"))
    r = dc.apply(conn)
    assert (r["added"], r["changed"], r["removed"]) == (1, 1, 1)
    rows = dict(conn.execute("SELECT rel_path, size FROM file WHERE present=1"))
    assert rows == {"NXT STUDIOS/A/clip.mov": 5_000_000, "NXT STUDIOS/B/foto.jpg": 2_000_000}
    sent = []
    catalog.sync_to_d1(conn, "dropbox:NXT", lambda p: sent.append(p) or True)
    assert sum(len(p["upserts"]) for p in sent) == 2
    assert "NXT STUDIOS/alt.mov" in [d for p in sent for d in p["deletes"]]
    assert json.loads((tmp_path / "s.json").read_text())["done"] is True
