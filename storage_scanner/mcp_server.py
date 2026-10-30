"""NXT Data MCP: read-only Auskunft für Claude, ob ein Ordner noch 2× woanders gesichert ist.

Läuft als stdio-MCP-Server im App-Binary:  "NXT Scanner" --mcp
Fragt nur den Worker-Endpunkt POST /query/redundancy (Bearer d1_token aus der Scanner-Config).
Die Regel (≥2 andere Geräte vollständig, NAS = 1 Gerät, im Zweifel nicht löschen) liegt im Worker.
Nur stdlib; stdout ist ausschließlich JSON-RPC, Diagnose geht nach stderr.

Werkzeuge: check_folder, check_disk, list_volumes.
"""
import json
import os
import re
import sys
import traceback
import urllib.error
import urllib.request

SERVER_NAME = "nxt-data"
SERVER_VERSION = "0.1.0"
CONFIG_PATH = os.path.expanduser("~/Library/Application Support/NXT Scanner/.notion_config.json")
DEFAULT_BASE = "https://nxt-scanner-ingest.nxt-studios.workers.dev"
HTTP_TIMEOUT = 30
USER_AGENT = f"NXT-Data-MCP/{SERVER_VERSION}"   # ohne User-Agent blockt Cloudflare (1010)

TOOLS = [
    {"name": "check_folder",
     "description": "Prüft, ob ein Ordner gelöscht werden darf: löschbar nur, wenn mindestens 2 ANDERE physische "
                    "Geräte eine vollständige Kopie haben (NAS zählt als 1 Gerät). Im Zweifel 'nicht löschen'. "
                    "Abgleich laut Datei-Katalog über Name + Größe, nicht hash-verifiziert.",
     "inputSchema": {"type": "object", "properties": {"path": {
         "type": "string", "description": "z. B. '/Volumes/NXT 041/2026-10-21_LANGDOCK x LW_FOOTAGE'"}},
         "required": ["path"]}},
    {"name": "check_disk",
     "description": "Bewertet alle Oberordner einer angesteckten Platte: welche löschbar sind (anderswo ≥2× vollständig) "
                    "und welche bleiben müssen.",
     "inputSchema": {"type": "object", "properties": {"volume": {
         "type": "string", "description": "'NXT 041' oder '/Volumes/NXT 041'"}}, "required": ["volume"]}},
    {"name": "list_volumes",
     "description": "Listet die angesteckten Platten.",
     "inputSchema": {"type": "object", "properties": {}}},
]


# ── Endpunkt ──────────────────────────────────────────────

def base_url() -> str:
    return os.environ.get("NXT_DATA_BASE_URL", DEFAULT_BASE).rstrip("/")


def read_token() -> str:
    with open(CONFIG_PATH, encoding="utf-8") as fh:
        token = str(json.load(fh).get("d1_token") or "").strip()
    if not token:
        raise RuntimeError("Kein D1-Token in der Scanner-Config (Menü „D1-Sync einrichten…“).")
    return token


def parse_volume_path(path: str) -> tuple[str, str]:
    m = re.match(r"^/Volumes/([^/]+)(?:/(.*))?$", path.rstrip("/"))
    if not m:
        raise ValueError(f"Pfad muss unter /Volumes/<Platte>/… liegen: {path}")
    return m.group(1), (m.group(2) or "")


def query_redundancy(disk: str, folder: str, token: str | None = None) -> dict:
    req = urllib.request.Request(base_url() + "/query/redundancy",
                                 data=json.dumps({"disk": disk, "folder": folder}).encode("utf-8"))
    req.add_header("Authorization", "Bearer " + (token or read_token()))
    req.add_header("content-type", "application/json")
    req.add_header("User-Agent", USER_AGENT)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8", "replace")[:300]
        except Exception:
            detail = ""
        raise RuntimeError(f"Endpunkt-Fehler HTTP {e.code}: {detail}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Endpunkt nicht erreichbar: {e.reason}")


def top_folders(volume: str) -> list[str]:
    root = f"/Volumes/{volume}"
    if not os.path.isdir(root):
        raise ValueError(f"Platte „{volume}“ ist nicht angesteckt ({root} fehlt).")
    return [n for n in sorted(os.listdir(root))
            if not n.startswith(".") and os.path.isdir(os.path.join(root, n))]


# ── Werkzeuge ─────────────────────────────────────────────

def _gb(n) -> str:
    return f"{(n or 0) / 1e9:.1f} GB"


def format_folder(resp: dict) -> str:
    src = resp.get("source") or {}
    cc = resp.get("complete_copies") or []
    lines = [f"Ordner: /Volumes/{src.get('disk', '?')}/{src.get('folder', '?')}",
             f"Inhalt (Katalog, Dateien ab 1 MB): {src.get('files', '?')} Dateien, {_gb(src.get('bytes'))}",
             f"Vollständige Kopien auf anderen Geräten: {len(cc)}  "
             + (", ".join(f"{c.get('device', '?')} ({c.get('path', '')})" for c in cc) or "(keine)")]
    for p in resp.get("partial_copies") or []:
        lines.append(f"  Teilkopie {p.get('device', '?')}: fehlt {p.get('missing', '?')} Datei(en), "
                     f"{_gb(p.get('missing_bytes'))} (zählt NICHT)")
    lines.append("")
    if resp.get("verdict") == "loeschbar":
        lines.append(f"URTEIL: LÖSCHBAR – {resp.get('grund', '')}")
    else:
        lines.append(f"URTEIL: NICHT LÖSCHEN – {resp.get('grund', '')}")
        uh = resp.get("unique_here") or {}
        if uh.get("files"):
            lines.append(f"  Nur hier vorhanden: {uh['files']} Datei(en), z. B.:")
            lines += [f"    - {u}" for u in (uh.get("sample") or [])[:10]]
    if resp.get("hinweis"):
        lines += ["", f"Hinweis: {resp['hinweis']}"]
    return "\n".join(lines)


def check_folder(args: dict) -> str:
    disk, folder = parse_volume_path(args["path"])
    if not folder:
        raise ValueError("Bitte einen Ordner angeben, nicht die ganze Platte (dafür check_disk).")
    return format_folder(query_redundancy(disk, folder))


def check_disk(args: dict) -> str:
    vol = args["volume"]
    name = vol[len("/Volumes/"):].rstrip("/") if vol.startswith("/Volumes/") else vol.strip()
    tops = top_folders(name)
    token = read_token()
    weg, bleibt = [], []
    for top in tops:
        r = query_redundancy(name, top, token=token)
        row = {"folder": top, "bytes": (r.get("source") or {}).get("bytes") or 0,
               "copies": [c.get("device") for c in (r.get("complete_copies") or [])],
               "uniq": (r.get("unique_here") or {}).get("files") or 0, "grund": r.get("grund", "")}
        (weg if r.get("verdict") == "loeschbar" else bleibt).append(row)
    lines = [f"Platte: {name}  ({len(tops)} Oberordner, angesteckt)", "",
             f"LÖSCHBAR (anderswo ≥2× vollständig), würde {_gb(sum(r['bytes'] for r in weg))} frei machen:"]
    lines += [f"  + {r['folder']}  ({_gb(r['bytes'])}) → {', '.join(r['copies'])}"
              for r in sorted(weg, key=lambda x: -x["bytes"])] or ["  (nichts)"]
    lines += ["", "BEHALTEN (nicht ausreichend gesichert):"]
    lines += [f"  - {r['folder']}  ({_gb(r['bytes'])}; vollständige Kopien: {len(r['copies'])}"
              + (f", {r['uniq']} nur hier" if r["uniq"] else "") + ")"
              for r in sorted(bleibt, key=lambda x: -x["bytes"])] or ["  (nichts)"]
    lines += ["", "Hinweis: laut Datei-Katalog (Name + Größe), nicht hash-verifiziert. Im Zweifel nicht löschen."]
    return "\n".join(lines)


def list_volumes(args: dict) -> str:
    names = [n for n in sorted(os.listdir("/Volumes"))
             if n != "Macintosh HD" and os.path.isdir(os.path.join("/Volumes", n))]
    return "Angesteckte Platten:\n" + ("\n".join(f"  {n}" for n in names) or "  (keine)")


HANDLERS = {"check_folder": check_folder, "check_disk": check_disk, "list_volumes": list_volumes}


# ── JSON-RPC über stdio ───────────────────────────────────

def handle(req: dict) -> dict | None:
    method, rid = req.get("method"), req.get("id")
    if method == "initialize":
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "protocolVersion": "2024-11-05", "capabilities": {"tools": {}},
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION}}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"tools": TOOLS}}
    if method == "tools/call":
        params = req.get("params") or {}
        fn = HANDLERS.get(params.get("name"))
        if not fn:
            return {"jsonrpc": "2.0", "id": rid,
                    "error": {"code": -32601, "message": f"Unbekanntes Werkzeug: {params.get('name')}"}}
        try:
            text, err = fn(params.get("arguments") or {}), False
        except Exception as e:
            text, err = f"Fehler: {e}", True
        result = {"content": [{"type": "text", "text": text}]}
        if err:
            result["isError"] = True
        return {"jsonrpc": "2.0", "id": rid, "result": result}
    if method == "ping":
        return {"jsonrpc": "2.0", "id": rid, "result": {}}
    if rid is None:                       # Benachrichtigung (z. B. notifications/initialized)
        return None
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"Methode nicht unterstützt: {method}"}}


def main(stdin=None, stdout=None) -> None:
    stdin, stdout = stdin or sys.stdin, stdout or sys.stdout
    for line in stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except ValueError:
            continue
        try:
            resp = handle(req)
        except Exception:
            traceback.print_exc(file=sys.stderr)
            resp = {"jsonrpc": "2.0", "id": req.get("id"), "error": {"code": -32603, "message": "interner Fehler"}}
        if resp is not None:
            stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
            stdout.flush()


if __name__ == "__main__":
    main()
