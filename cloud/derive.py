"""Zentrale Ableitung: aus D1-Rohdaten (folder_tree/disks) die Analyse-Tabellen
projects + scans_log bauen. Läuft auf einem Rechner mit wrangler-Login (kein
extra Secret, kein SQL-Endpoint). Wiederverwendet storage_scanner.rules.

Aufruf:
    PYTHONPATH=.. python3 derive.py            # gegen Remote-D1
    PYTHONPATH=.. python3 derive.py --local    # gegen lokale wrangler-D1
"""

import json
import os
import subprocess
import sys
import tempfile
from collections import defaultdict
from difflib import SequenceMatcher

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage_scanner.rules import validate_folder  # noqa: E402

DB_NAME = "nxt-scanner"
CONTAINER = {"OLDER", "OTHER", "NXT STUDIOS", "NXTSTUDIOS", "DIVERSE", "MISC"}


# ── reine Logik (testbar, ohne wrangler) ─────────────────────────────

def norm(name: str) -> str:
    import re
    s = re.sub(r"[_\-]", " ", (name or "").upper())
    s = re.sub(r"[^A-Z0-9 ]", "", s)
    return re.sub(r"\s+", " ", s).strip()


def classify(node: dict) -> dict:
    """Klassifiziert einen depth-1 folder_tree-Knoten zu einer projects-Zeile."""
    name = (node.get("rel_path") or "").split("/")[-1]
    out = {
        "disk_uuid": node["disk_uuid"], "rel_path": node["rel_path"], "name": name,
        "size_bytes": node.get("size_bytes"), "file_count": node.get("file_count"),
        "mtime": node.get("mtime"),
        "date": None, "project_name": None, "type": None,
        "norm_name": norm(name), "status": "unassigned", "cluster_id": None,
    }
    if norm(name) in CONTAINER:
        out["status"] = "container"
        return out
    v = validate_folder(name)
    if v:
        out.update(date=v["date"], project_name=v["project_name"], type=v["type"],
                   norm_name=norm(v["project_name"]), status="validated")
    return out


def _date_close(d1, d2, days=2):
    if d1 == d2:
        return True
    if not d1 or not d2:
        return False
    try:
        from datetime import date
        a = date(*map(int, d1.split("-")))
        b = date(*map(int, d2.split("-")))
        return abs((a - b).days) <= days
    except Exception:
        return False


def assign_clusters(projects: list[dict]) -> list[dict]:
    """Setzt cluster_id per Fuzzy-Union-Find (nur validierte Projekte).
    Nicht-validierte behalten cluster_id = None."""
    idx = [i for i, p in enumerate(projects) if p["status"] == "validated"]
    parent = {i: i for i in idx}

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    buckets = defaultdict(list)
    for i in idx:
        buckets[projects[i]["norm_name"][:3]].append(i)

    for i in idx:
        pi = projects[i]
        for j in buckets.get(pi["norm_name"][:3], []):
            if j <= i:
                continue
            pj = projects[j]
            a, b = pi["norm_name"], pj["norm_name"]
            if not a or not b:
                continue
            sim = SequenceMatcher(None, a, b).ratio()
            same = (
                (pi["date"] == pj["date"] and a == b) or
                (pi["date"] == pj["date"] and sim >= 0.86) or
                (a == b and _date_close(pi["date"], pj["date"])) or
                (sim >= 0.93 and _date_close(pi["date"], pj["date"]))
            )
            if same:
                union(i, j)

    for i in idx:
        root = find(i)
        p = projects[root]
        projects[i]["cluster_id"] = f"{p['date']}|{p['norm_name']}"
    return projects


def scans_log_rows(disks: list[dict], projects: list[dict], nodes: list[dict]) -> list[dict]:
    by_disk_valid = defaultdict(int)
    by_disk_unassigned = defaultdict(int)
    by_disk_nodes = defaultdict(int)
    for p in projects:
        if p["status"] == "validated":
            by_disk_valid[p["disk_uuid"]] += 1
        elif p["status"] == "unassigned":
            by_disk_unassigned[p["disk_uuid"]] += 1
    for n in nodes:
        by_disk_nodes[n["disk_uuid"]] += 1
    rows = []
    for d in disks:
        u = d["uuid"]
        rows.append({
            "disk_uuid": u, "scan_date": d.get("last_scan"), "used_bytes": d.get("used_bytes"),
            "valid_count": by_disk_valid[u], "unassigned_count": by_disk_unassigned[u],
            "node_count": by_disk_nodes[u],
        })
    return rows


# ── wrangler-I/O (dünn) ──────────────────────────────────────────────

def _wrangler(args: list[str], remote: bool) -> str:
    loc = "--remote" if remote else "--local"
    cmd = ["wrangler", "d1"] + args[:1] + [DB_NAME, loc] + args[1:]
    r = subprocess.run(cmd, capture_output=True, text=True,
                       cwd=os.path.dirname(os.path.abspath(__file__)))
    if r.returncode != 0:
        raise RuntimeError(f"wrangler fehlgeschlagen: {r.stderr[-400:]}")
    return r.stdout


def query(sql: str, remote: bool) -> list[dict]:
    out = _wrangler(["execute", "--json", "--command", sql], remote)
    data = json.loads(out)
    # wrangler --json: Liste von {results: [...]} oder direkt results
    if isinstance(data, list) and data and "results" in data[0]:
        return data[0]["results"]
    return data.get("results", []) if isinstance(data, dict) else []


def _sql_val(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def exec_sql(statements: list[str], remote: bool) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False, dir=os.path.dirname(os.path.abspath(__file__))) as f:
        f.write("\n".join(statements))
        path = f.name
    try:
        _wrangler(["execute", "--file", os.path.basename(path)], remote)
    finally:
        os.unlink(path)


def derive(remote: bool = True) -> dict:
    disks = query("SELECT uuid, name, used_bytes, last_scan FROM disks", remote)
    nodes = query("SELECT disk_uuid, rel_path, size_bytes, file_count, mtime FROM folder_tree WHERE depth=1", remote)

    projects = [classify(n) for n in nodes]
    projects = assign_clusters(projects)
    logs = scans_log_rows(disks, projects, query("SELECT disk_uuid FROM folder_tree", remote))

    stmts = ["DELETE FROM projects;", "DELETE FROM scans_log;"]
    for p in projects:
        cols = ["disk_uuid", "rel_path", "name", "date", "project_name", "norm_name", "type", "status", "size_bytes", "file_count", "mtime", "cluster_id"]
        stmts.append("INSERT INTO projects(" + ",".join(cols) + ") VALUES(" + ",".join(_sql_val(p.get(c)) for c in cols) + ");")
    for r in logs:
        cols = ["disk_uuid", "scan_date", "used_bytes", "valid_count", "unassigned_count", "node_count"]
        stmts.append("INSERT INTO scans_log(" + ",".join(cols) + ") VALUES(" + ",".join(_sql_val(r.get(c)) for c in cols) + ");")

    exec_sql(stmts, remote)
    return {"disks": len(disks), "nodes": len(nodes), "projects": len(projects),
            "validated": sum(1 for p in projects if p["status"] == "validated"),
            "clusters": len({p["cluster_id"] for p in projects if p["cluster_id"]})}


if __name__ == "__main__":
    remote = "--local" not in sys.argv
    res = derive(remote=remote)
    print(f"Ableitung fertig ({'remote' if remote else 'local'}): {res}")
