"""Dropbox-Inventur über die API (nur Metadaten, nichts wird heruntergeladen).

Der lokale stat-Lauf über ~/Library/CloudStorage/Dropbox* sieht online-only-Platzhalter nicht
vollständig (Größe 0 oder gar nicht). Die API liefert den echten Serverstand.

  python -m storage_scanner.dropbox_catalog fetch     # Liste holen → JSONL, Summe je Ordner (schreibt nichts in den Katalog)
  python -m storage_scanner.dropbox_catalog apply     # JSONL in den lokalen Katalog (uuid "dropbox:NXT"), ersetzt den stat-Stand
  python -m storage_scanner.dropbox_catalog sync      # Katalog-Änderungen nach D1 (erst nach Freigabe durch Jonathan)

Token: ~/.nxt_dropbox_token (Scope files.metadata.read), wird nie ausgegeben.
Business-Teamordner: Pfad-Wurzel = root_namespace_id des Kontos (Header Dropbox-API-Path-Root).
"""
import argparse
import collections
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from . import catalog
from .paths import DATA_DIR

UUID, NAME = "dropbox:NXT", "Dropbox"
API = "https://api.dropboxapi.com/2"
TOKEN_FILE = Path.home() / ".nxt_dropbox_token"
OUT = DATA_DIR / "dropbox_listing.jsonl"
STATE = DATA_DIR / "dropbox_listing.state.json"


class TokenExpired(RuntimeError):
    pass


def _token() -> str:
    t = TOKEN_FILE.read_text().strip()
    if not t:
        raise RuntimeError(f"{TOKEN_FILE} ist leer")
    return t


def call(endpoint: str, body, token: str, path_root: str | None = None, tries: int = 8):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "User-Agent": "nxt-scanner-dropbox"}
    if path_root:
        headers["Dropbox-API-Path-Root"] = json.dumps({".tag": "root", "root": path_root})
    data = b"null" if body is None else json.dumps(body).encode()
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(API + endpoint, data=data, headers=headers, method="POST"),
                                        timeout=120) as r:
                return json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            msg = e.read().decode("utf-8", "replace")[:300]
            if e.code == 401:
                raise TokenExpired(f"Token abgelaufen/ungültig: {msg}") from None
            if e.code == 429 or e.code >= 500:
                wait = int(e.headers.get("Retry-After") or min(60, 2 ** i))
                time.sleep(wait)
                continue
            raise RuntimeError(f"{endpoint} HTTP {e.code}: {msg}") from None
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(min(60, 2 ** i))
    raise RuntimeError(f"{endpoint}: zu viele Fehlversuche")


def _epoch(iso: str | None) -> float | None:
    if not iso:
        return None
    return datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()


def fetch(token: str | None = None, restart: bool = False) -> dict:
    """Holt den kompletten Baum (rekursiv) und schreibt Dateien als JSONL. Fortsetzbar über den Cursor."""
    token = token or _token()
    acct = call("/users/get_current_account", None, token)
    root = (acct.get("root_info") or {}).get("root_namespace_id")
    st = json.loads(STATE.read_text()) if STATE.exists() and not restart else {}
    if st.get("root") not in (None, root):
        st = {}
    mode = "a" if st.get("cursor") and not st.get("done") else "w"
    if st.get("done") and not restart:
        print("Liste ist schon vollständig; neu holen mit --restart")
        return summary()
    pages = 0
    with open(OUT, mode, encoding="utf-8") as f:
        if mode == "w":
            st = {"root": root, "started": time.strftime("%Y-%m-%dT%H:%M:%S")}
            r = call("/files/list_folder", {"path": "", "recursive": True, "include_deleted": False,
                                            "include_non_downloadable_files": True, "limit": 2000}, token, root)
        else:
            r = call("/files/list_folder/continue", {"cursor": st["cursor"]}, token, root)
        while True:
            for e in r.get("entries", []):
                if e.get(".tag") != "file":
                    continue
                f.write(json.dumps({"p": e["path_display"].lstrip("/"), "s": e.get("size", 0),
                                    "m": e.get("server_modified"), "h": e.get("content_hash")}, ensure_ascii=False) + "\n")
            f.flush()
            st["cursor"] = r.get("cursor"); pages += 1
            STATE.write_text(json.dumps(st))
            if pages % 20 == 0:
                print(f"  … {pages} Seiten", flush=True)
            if not r.get("has_more"):
                break
            r = call("/files/list_folder/continue", {"cursor": st["cursor"]}, token, root)
    st["done"] = True; st["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    STATE.write_text(json.dumps(st))
    return summary()


def _rows():
    seen = {}
    for l in open(OUT, encoding="utf-8"):
        try:
            r = json.loads(l)
        except ValueError:
            continue
        seen[r["p"]] = r          # bei Fortsetzung doppelt geschriebene Seiten: letzter Stand gilt
    return seen


def summary() -> dict:
    rows = _rows()
    top, top2 = collections.Counter(), collections.Counter()
    zero = 0
    for p, r in rows.items():
        parts = p.split("/")
        top[parts[0]] += r["s"]
        if len(parts) > 2:
            top2["/".join(parts[:2])] += r["s"]
        zero += r["s"] == 0
    tot = sum(r["s"] for r in rows.values())
    print(f"Dropbox (API): {len(rows):,} Dateien, {tot / 1e12:.2f} TB, davon 0 Byte: {zero:,}")
    for k, b in top.most_common():
        print(f"  {b / 1e12:7.2f} TB  {k}")
        for k2, b2 in top2.most_common():
            if k2.startswith(k + "/") and b2 >= 1e9:
                print(f"      {b2 / 1e12:7.2f} TB  {k2}")
    return {"files": len(rows), "bytes": tot, "zero": zero}


def apply(conn=None) -> dict:
    """Ersetzt den Katalogstand von dropbox:NXT durch die API-Liste (neu/geändert/verschwunden wie bei inventory)."""
    st = json.loads(STATE.read_text()) if STATE.exists() else {}
    if not st.get("done"):
        raise RuntimeError("API-Liste nicht vollständig – erst fetch")
    conn = conn or catalog.connect()
    rows = _rows()
    now = catalog._now()
    with conn:
        catalog.upsert_volume(conn, {"uuid": UUID, "name": NAME, "fs_type": "dropbox", "capacity_bytes": None,
                                     "free_bytes": None, "device_id": None, "whole_disk": None, "physical_store": None,
                                     "media_name": "Dropbox (API)", "bus_protocol": "cloud", "solid_state": None,
                                     "device_serial": None})
    existing = {r[0]: (r[1], r[2], r[3]) for r in
                conn.execute("SELECT rel_path, size, mtime, present FROM file WHERE volume_uuid=?", (UUID,))}
    new, upd, touch = [], [], []
    for p, r in rows.items():
        m = _epoch(r["m"])
        old = existing.get(p)
        if old is None:
            name = p.rsplit("/", 1)[-1]
            ext, kind = catalog.classify(name)
            new.append((UUID, p, name, ext, kind, r["s"], m, r["h"], now, now, now))
        elif old[0] != r["s"] or old[1] != m or not old[2]:
            upd.append((r["s"], m, r["h"], now, now, UUID, p))
        else:
            touch.append((now, UUID, p))
    gone = [p for p, v in existing.items() if v[2] and p not in rows]
    with conn:
        conn.executemany("""INSERT INTO file(volume_uuid,rel_path,name,ext,kind,size,mtime,fingerprint,fp_algo,
                                present,first_seen,last_seen,changed_at) VALUES(?,?,?,?,?,?,?,?,'dropbox',1,?,?,?)""", new)
        conn.executemany("""UPDATE file SET size=?,mtime=?,fingerprint=?,fp_algo='dropbox',hashed_at=NULL,present=1,
                                gone_at=NULL,last_seen=?,changed_at=? WHERE volume_uuid=? AND rel_path=?""", upd)
        conn.executemany("UPDATE file SET last_seen=? WHERE volume_uuid=? AND rel_path=?", touch)
        conn.executemany("UPDATE file SET present=0,gone_at=?,changed_at=? WHERE volume_uuid=? AND rel_path=?",
                         [(now, now, UUID, p) for p in gone])
        tot = sum(r["s"] for r in rows.values())
        conn.execute("UPDATE volume SET last_inventory=? WHERE uuid=?", (now, UUID))
        conn.execute("""INSERT INTO run(volume_uuid,kind,started_at,finished_at,host,files,bytes,added,changed,removed,
                            hashed,errors,complete) VALUES(?,?,?,?,?,?,?,?,?,?,0,0,1)""",
                     (UUID, "dropbox-api", st.get("started"), now, socket.gethostname(), len(rows), tot,
                      len(new), len(upd), len(gone)))
    res = {"files": len(rows), "bytes": tot, "added": len(new), "changed": len(upd), "removed": len(gone)}
    print(res)
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(prog="dropbox_catalog")
    ap.add_argument("cmd", choices=["fetch", "summary", "apply", "sync"])
    ap.add_argument("--restart", action="store_true", help="Liste komplett neu holen")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "fetch":
            fetch(restart=a.restart)
        elif a.cmd == "summary":
            summary()
        elif a.cmd == "apply":
            apply()
        else:
            print(catalog.sync_to_d1(catalog.connect(), UUID, catalog.http_poster(), host=socket.gethostname(),
                                     progress=lambda n: print(f"  … {n:,} Zeilen", flush=True) if n % 50000 < 5000 else None))
    except TokenExpired as e:
        sys.exit(f"{e}\nNeues Token in {TOKEN_FILE} ablegen und denselben Befehl erneut starten (fetch setzt am Cursor fort).")


if __name__ == "__main__":
    main()
