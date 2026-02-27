"""Notion Sync – Liest einen Scan-Report und synchronisiert ihn mit Notion.

Erstellt zwei verknüpfte Datenbanken (Datenträger + Projekte) und aktualisiert
bestehende Einträge bei erneutem Scan.
"""

import argparse
import json
import shutil
import sys
from pathlib import Path

import httpx

from .utils import bytes_to_gb
from .paths import CONFIG_PATH

API = "https://api.notion.com/v1"


def _get_headers() -> dict:
    """Baut die Notion-API-Header mit dem Token aus der Config."""
    config = load_config()
    token = config.get("notion_token", "")
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def _get_parent_page_id() -> str:
    """Liest die Parent Page ID aus der Config. Extrahiert ID aus URL falls nötig."""
    import re
    config = load_config()
    raw = config.get("notion_parent_page_id", "")
    match = re.search(r"([a-f0-9]{32})", raw)
    return match.group(1) if match else raw


# ── API-Hilfsfunktionen ────────────────────────────────────────────

def api_post(endpoint: str, body: dict) -> dict:
    resp = httpx.post(f"{API}/{endpoint}", headers=_get_headers(), json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


def api_patch(endpoint: str, body: dict) -> dict:
    resp = httpx.patch(f"{API}/{endpoint}", headers=_get_headers(), json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


def api_get(endpoint: str) -> dict:
    resp = httpx.get(f"{API}/{endpoint}", headers=_get_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


# ── Config (speichert DB-IDs + user_name zwischen Runs) ──────────

def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}


def save_config(config: dict) -> None:
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)


# ── Datenbanken erstellen ───────────────────────────────────────────

def create_hdd_database() -> str:
    db = api_post("databases", {
        "parent": {"type": "page_id", "page_id": _get_parent_page_id()},
        "title": [{"type": "text", "text": {"content": "Datenträger"}}],
        "properties": {
            "Name": {"title": {}},
            "Kapazität (GB)": {"number": {"format": "number"}},
            "Belegt (GB)": {"number": {"format": "number"}},
            "Gültige Projekte": {"number": {"format": "number"}},
            "Unassigned": {"number": {"format": "number"}},
            "Letzter Scan": {"date": {}},
            "Volume UUID": {"rich_text": {}},
            "Zuletzt genutzt von": {"rich_text": {}},
        },
    })
    print(f"  Datenträger-Datenbank erstellt: {db['id']}")
    return db["id"]


def create_projects_database(hdd_db_id: str) -> str:
    db = api_post("databases", {
        "parent": {"type": "page_id", "page_id": _get_parent_page_id()},
        "title": [{"type": "text", "text": {"content": "Projekte"}}],
        "properties": {
            "Name": {"title": {}},
            "Projektname": {"rich_text": {}},
            "Datum": {"date": {}},
            "Typ": {
                "select": {
                    "options": [
                        {"name": "FOOTAGE", "color": "blue"},
                        {"name": "PHOTOS", "color": "green"},
                        {"name": "WORKING", "color": "orange"},
                        {"name": "BTS", "color": "yellow"},
                        {"name": "PROJECT", "color": "purple"},
                        {"name": "PROXIES", "color": "pink"},
                    ]
                }
            },
            "Größe (GB)": {"number": {"format": "number"}},
            "Dateien": {"number": {"format": "number"}},
            "HDD": {"relation": {"database_id": hdd_db_id, "type": "single_property", "single_property": {}}},
            "Status": {
                "select": {
                    "options": [
                        {"name": "Valid", "color": "green"},
                        {"name": "Unassigned", "color": "red"},
                    ]
                }
            },
            "Absoluter Pfad": {"rich_text": {}},
            "Letzte Änderung": {"date": {}},
        },
    })
    db_id = db["id"]

    # Self-Relation für PROJECT-Unterordner (muss nach Erstellung hinzugefügt werden)
    api_patch(f"databases/{db_id}", {
        "properties": {
            "Projekt": {"relation": {"database_id": db_id, "type": "single_property", "single_property": {}}},
        }
    })

    print(f"  Projekte-Datenbank erstellt: {db_id}")
    return db_id


def _find_existing_databases() -> tuple[str | None, str | None]:
    """Sucht bestehende Datenbanken unter der Parent Page."""
    parent_id = _get_parent_page_id()
    if not parent_id:
        return None, None

    hdd_db_id = None
    projects_db_id = None

    try:
        body = {"filter": {"property": "object", "value": "database"}}
        resp = api_post(f"search", body)
        for result in resp.get("results", []):
            if result.get("object") != "database":
                continue
            # Parent Page prüfen
            parent = result.get("parent", {})
            result_parent_id = parent.get("page_id", "").replace("-", "")
            if result_parent_id != parent_id.replace("-", ""):
                continue
            # Name prüfen
            title_parts = result.get("title", [])
            title = title_parts[0]["plain_text"] if title_parts else ""
            if title == "Datenträger":
                hdd_db_id = result["id"]
            elif title == "Projekte":
                projects_db_id = result["id"]
    except Exception:
        pass

    return hdd_db_id, projects_db_id


def ensure_databases() -> tuple[str, str]:
    config = load_config()
    hdd_db_id = config.get("hdd_db_id")
    projects_db_id = config.get("projects_db_id")

    if hdd_db_id:
        try:
            api_get(f"databases/{hdd_db_id}")
        except httpx.HTTPStatusError:
            hdd_db_id = None
            projects_db_id = None

    if projects_db_id:
        try:
            api_get(f"databases/{projects_db_id}")
        except httpx.HTTPStatusError:
            projects_db_id = None

    # Bestehende Datenbanken suchen bevor neue erstellt werden
    if not hdd_db_id or not projects_db_id:
        found_hdd, found_projects = _find_existing_databases()
        if not hdd_db_id and found_hdd:
            hdd_db_id = found_hdd
        if not projects_db_id and found_projects:
            projects_db_id = found_projects

    if not hdd_db_id:
        print("Erstelle Notion-Datenbanken...")
        hdd_db_id = create_hdd_database()
        projects_db_id = create_projects_database(hdd_db_id)
    elif not projects_db_id:
        projects_db_id = create_projects_database(hdd_db_id)

    config["hdd_db_id"] = hdd_db_id
    config["projects_db_id"] = projects_db_id
    save_config(config)

    migrate_schema(hdd_db_id, projects_db_id)

    return hdd_db_id, projects_db_id


def migrate_schema(hdd_db_id: str, projects_db_id: str) -> None:
    """Stellt sicher, dass alle Properties existieren (idempotent)."""
    try:
        api_patch(f"databases/{hdd_db_id}", {
            "properties": {
                "Kapazität (GB)": {"number": {"format": "number"}},
                "Belegt (GB)": {"number": {"format": "number"}},
                "Volume UUID": {"rich_text": {}},
                "Zuletzt genutzt von": {"rich_text": {}},
            }
        })
    except httpx.HTTPStatusError:
        pass

    try:
        api_patch(f"databases/{projects_db_id}", {
            "properties": {
                "Größe (GB)": {"number": {"format": "number"}},
                "Absoluter Pfad": {"rich_text": {}},
                "Letzte Änderung": {"date": {}},
                "Projekt": {"relation": {"database_id": projects_db_id, "type": "single_property", "single_property": {}}},
            }
        })
    except httpx.HTTPStatusError:
        pass

    # Typ-Option "PROJECT" hinzufügen
    try:
        api_patch(f"databases/{projects_db_id}", {
            "properties": {
                "Typ": {"select": {"options": [
                    {"name": "BTS", "color": "yellow"},
                    {"name": "PROJECT", "color": "purple"},
                    {"name": "PROXIES", "color": "pink"},
                ]}},
            }
        })
    except httpx.HTTPStatusError:
        pass


# ── Notion-Abfragen ────────────────────────────────────────────────

def query_database(db_id: str, filter_obj: dict | None = None) -> list[dict]:
    """Alle Ergebnisse aus einer DB-Query laden (mit Pagination)."""
    results = []
    body = {}
    if filter_obj:
        body["filter"] = filter_obj

    while True:
        response = api_post(f"databases/{db_id}/query", body)
        results.extend(response["results"])
        if not response["has_more"]:
            break
        body["start_cursor"] = response["next_cursor"]

    return results


def find_page_by_title(db_id: str, title: str) -> str | None:
    pages = query_database(db_id, {"property": "Name", "title": {"equals": title}})
    return pages[0]["id"] if pages else None


# ── Hilfsfunktionen ────────────────────────────────────────────────

def get_disk_info(path: str) -> dict:
    usage = shutil.disk_usage(path)
    return {
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
    }


def _upsert_page(db_id: str, existing: dict, name: str, properties: dict) -> str:
    """Erstellt oder aktualisiert eine Seite und gibt die Page-ID zurück."""
    if name in existing:
        page_id = existing[name]
        api_patch(f"pages/{page_id}", {"properties": properties})
        return page_id
    else:
        page = api_post("pages", {"parent": {"database_id": db_id}, "properties": properties})
        return page["id"]


# ── Sync-Logik ──────────────────────────────────────────────────────

def sync_hdd(hdd_db_id: str, report: dict, disk_info: dict, user_name: str = "") -> str:
    scan_info = report["scan_info"]
    hdd_name = Path(scan_info["scanned_path"]).name

    properties = {
        "Name": {"title": [{"text": {"content": hdd_name}}]},
        "Kapazität (GB)": {"number": bytes_to_gb(disk_info["total_bytes"])},
        "Belegt (GB)": {"number": bytes_to_gb(disk_info["used_bytes"])},
        "Gültige Projekte": {"number": scan_info["valid_folders"]},
        "Unassigned": {"number": scan_info["unassigned_folders"]},
        "Letzter Scan": {"date": {"start": scan_info["scan_date"]}},
        "Volume UUID": {"rich_text": [{"text": {"content": scan_info.get("volume_uuid", "")}}]},
        "Zuletzt genutzt von": {"rich_text": [{"text": {"content": user_name}}]},
    }

    existing_id = find_page_by_title(hdd_db_id, hdd_name)

    if existing_id:
        api_patch(f"pages/{existing_id}", {"properties": properties})
        print(f"  Datenträger aktualisiert: {hdd_name}")
        return existing_id
    else:
        page = api_post("pages", {"parent": {"database_id": hdd_db_id}, "properties": properties})
        print(f"  Datenträger erstellt: {hdd_name}")
        return page["id"]


def sync_projects(projects_db_id: str, report: dict, hdd_page_id: str) -> None:
    # Bestehende Einträge für diese HDD laden
    existing_pages = query_database(
        projects_db_id,
        {"property": "HDD", "relation": {"contains": hdd_page_id}},
    )
    existing = {}
    for page in existing_pages:
        title = page["properties"]["Name"]["title"]
        if title:
            existing[title[0]["plain_text"]] = page["id"]

    synced_names = set()

    # Gültige Projekte syncen
    for project in report["projects"]:
        name = project["name"]
        synced_names.add(name)

        properties = {
            "Name": {"title": [{"text": {"content": name}}]},
            "Projektname": {"rich_text": [{"text": {"content": project.get("project_name", "")}}]},
            "Typ": {"select": {"name": project["type"]}},
            "Größe (GB)": {"number": bytes_to_gb(project["size_bytes"])},
            "Dateien": {"number": project["file_count"]},
            "HDD": {"relation": [{"id": hdd_page_id}]},
            "Status": {"select": {"name": "Valid"}},
            "Absoluter Pfad": {"rich_text": [{"text": {"content": project.get("absolute_path", "")}}]},
        }
        if project.get("date"):
            properties["Datum"] = {"date": {"start": project["date"]}}
        if project.get("last_modified"):
            properties["Letzte Änderung"] = {"date": {"start": project["last_modified"][:10]}}

        parent_page_id = _upsert_page(projects_db_id, existing, name, properties)

        # Children syncen (Unterordner von PROJECT-Ordnern)
        for child in project.get("children", []):
            child_name = child["name"]
            synced_names.add(child_name)

            child_props = {
                "Name": {"title": [{"text": {"content": child_name}}]},
                "Größe (GB)": {"number": bytes_to_gb(child["size_bytes"])},
                "Dateien": {"number": child["file_count"]},
                "HDD": {"relation": [{"id": hdd_page_id}]},
                "Projekt": {"relation": [{"id": parent_page_id}]},
                "Absoluter Pfad": {"rich_text": [{"text": {"content": child.get("absolute_path", "")}}]},
            }
            if child.get("type"):
                child_props["Typ"] = {"select": {"name": child["type"]}}
                child_props["Status"] = {"select": {"name": "Valid"}}
            else:
                child_props["Status"] = {"select": {"name": "Unassigned"}}
            if child.get("project_name"):
                child_props["Projektname"] = {"rich_text": [{"text": {"content": child["project_name"]}}]}
            if child.get("date"):
                child_props["Datum"] = {"date": {"start": child["date"]}}
            if child.get("last_modified"):
                child_props["Letzte Änderung"] = {"date": {"start": child["last_modified"][:10]}}

            _upsert_page(projects_db_id, existing, child_name, child_props)

    # Unassigned-Ordner syncen
    for folder in report["unassigned"]:
        name = folder["name"]
        synced_names.add(name)

        properties = {
            "Name": {"title": [{"text": {"content": name}}]},
            "Projektname": {"rich_text": [{"text": {"content": ""}}]},
            "Größe (GB)": {"number": bytes_to_gb(folder["size_bytes"])},
            "Dateien": {"number": folder["file_count"]},
            "HDD": {"relation": [{"id": hdd_page_id}]},
            "Status": {"select": {"name": "Unassigned"}},
            "Absoluter Pfad": {"rich_text": [{"text": {"content": folder.get("absolute_path", "")}}]},
        }
        if folder.get("last_modified"):
            properties["Letzte Änderung"] = {"date": {"start": folder["last_modified"][:10]}}

        _upsert_page(projects_db_id, existing, name, properties)

    # Ordner die nicht mehr existieren archivieren
    for name, page_id in existing.items():
        if name not in synced_names:
            api_patch(f"pages/{page_id}", {"archived": True})
            print(f"  Archiviert (gelöscht vom Datenträger): {name}")

    total = len(report["projects"]) + len(report["unassigned"])
    print(f"  {total} Ordner synchronisiert")


# ── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Synchronisiert einen Storage-Scanner-Report mit Notion."
    )
    parser.add_argument("report", help="Pfad zur JSON-Report-Datei")
    args = parser.parse_args()

    report_path = Path(args.report).resolve()
    if not report_path.exists():
        print(f"Fehler: Report nicht gefunden: {report_path}", file=sys.stderr)
        sys.exit(1)

    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)

    scanned_path = report["scan_info"]["scanned_path"]

    if Path(scanned_path).exists():
        disk_info = get_disk_info(scanned_path)
    else:
        print(f"Hinweis: {scanned_path} nicht gemountet – Speicherplatz-Infos nicht verfügbar.")
        disk_info = {"total_bytes": 0, "used_bytes": 0, "free_bytes": 0}

    config = load_config()
    user_name = config.get("user_name", "")

    print("Notion-Sync gestartet...")
    hdd_db_id, projects_db_id = ensure_databases()

    hdd_page_id = sync_hdd(hdd_db_id, report, disk_info, user_name)
    sync_projects(projects_db_id, report, hdd_page_id)

    print("\nSync abgeschlossen!")


def run_sync(report_path: str) -> None:
    """Programmatischer Einstiegspunkt für Notion-Sync (ohne argparse/sys.exit).

    Args:
        report_path: Pfad zur JSON-Report-Datei.

    Raises:
        FileNotFoundError: Wenn der Report nicht existiert.
    """
    path = Path(report_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Report nicht gefunden: {path}")

    with open(path, encoding="utf-8") as f:
        report = json.load(f)

    scanned_path = report["scan_info"]["scanned_path"]

    if Path(scanned_path).exists():
        disk_info = get_disk_info(scanned_path)
    else:
        disk_info = {"total_bytes": 0, "used_bytes": 0, "free_bytes": 0}

    config = load_config()
    user_name = config.get("user_name", "")

    hdd_db_id, projects_db_id = ensure_databases()
    hdd_page_id = sync_hdd(hdd_db_id, report, disk_info, user_name)
    sync_projects(projects_db_id, report, hdd_page_id)


if __name__ == "__main__":
    main()
