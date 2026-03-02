"""Notion Sync – Liest einen Scan-Report und synchronisiert ihn mit Notion.

Erstellt vier verknüpfte Datenbanken (Datenträger + Speicherungen + Projekte + Log)
und aktualisiert bestehende Einträge bei erneutem Scan.
"""

import argparse
import json
import shutil
import sys
from collections import defaultdict
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
            "Zuletzt genutzt von": {"select": {}},
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
            "Letzter Scan": {"date": {}},
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


def create_aggregated_projects_database(hdd_db_id: str, speicherungen_db_id: str) -> str:
    """Erstellt die aggregierte Projekte-Datenbank (eine Zeile pro Projekt)."""
    db = api_post("databases", {
        "parent": {"type": "page_id", "page_id": _get_parent_page_id()},
        "title": [{"type": "text", "text": {"content": "Projekte"}}],
        "properties": {
            "Name": {"title": {}},
            "Projektname": {"rich_text": {}},
            "Datum": {"date": {}},
            "Typen": {
                "multi_select": {
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
            "HDDs": {"relation": {"database_id": hdd_db_id, "type": "single_property", "single_property": {}}},
            "Speicherungen": {"relation": {"database_id": speicherungen_db_id, "type": "single_property", "single_property": {}}},
            "Gesamtgröße (GB)": {"number": {"format": "number"}},
            "Übersicht": {"rich_text": {}},
            "Mismatch": {"checkbox": {}},
            "Backup-Status": {
                "select": {
                    "options": [
                        {"name": "Nicht gesichert", "color": "red"},
                        {"name": "Teilweise", "color": "orange"},
                        {"name": "Vollständig", "color": "green"},
                    ]
                }
            },
            "Letzter Scan": {"date": {}},
        },
    })
    print(f"  Projekte-Datenbank (aggregiert) erstellt: {db['id']}")
    return db["id"]


def create_log_database(aggregated_db_id: str) -> str:
    """Erstellt die Log-Datenbank für Mismatch-Warnungen."""
    db = api_post("databases", {
        "parent": {"type": "page_id", "page_id": _get_parent_page_id()},
        "title": [{"type": "text", "text": {"content": "Log"}}],
        "properties": {
            "Name": {"title": {}},
            "Typ": {
                "select": {
                    "options": [
                        {"name": "MISSING_BACKUP", "color": "red"},
                        {"name": "SIZE_MISMATCH", "color": "orange"},
                        {"name": "INCOMPLETE_BACKUP", "color": "orange"},
                        {"name": "EXCESS_COPIES", "color": "gray"},
                    ]
                }
            },
            "Priorität": {
                "select": {
                    "options": [
                        {"name": "Kritisch", "color": "red"},
                        {"name": "Warnung", "color": "orange"},
                        {"name": "Info", "color": "gray"},
                    ]
                }
            },
            "Projekt": {"relation": {"database_id": aggregated_db_id, "type": "single_property", "single_property": {}}},
            "Ordnertyp": {
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
            "Details": {"rich_text": {}},
            "HDDs": {"rich_text": {}},
            "Differenz (GB)": {"number": {"format": "number"}},
            "Status": {
                "select": {
                    "options": [
                        {"name": "Open", "color": "red"},
                        {"name": "Resolved", "color": "green"},
                        {"name": "Umgesetzt", "color": "blue"},
                    ]
                }
            },
            "Erkannt am": {"date": {}},
            "Letzter Scan": {"date": {}},
        },
    })
    print(f"  Log-Datenbank erstellt: {db['id']}")
    return db["id"]


def _find_existing_databases() -> tuple[str | None, str | None]:
    """Sucht bestehende Kern-Datenbanken (Datenträger + Speicherungen) unter der Parent Page.

    Erkennt auch die alte "Projekte"-DB (ohne Mismatch-Property) als Speicherungen-DB.
    """
    parent_id = _get_parent_page_id()
    if not parent_id:
        return None, None

    hdd_db_id = None
    projects_db_id = None

    try:
        body = {"filter": {"property": "object", "value": "database"}}
        resp = api_post("search", body)
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
            props = result.get("properties", {})
            if title == "Datenträger":
                hdd_db_id = result["id"]
            elif title == "Speicherungen":
                projects_db_id = result["id"]
            elif title == "Projekte" and "Mismatch" not in props:
                # Alte "Projekte"-DB (vor Umbenennung zu "Speicherungen")
                projects_db_id = result["id"]
    except Exception:
        pass

    return hdd_db_id, projects_db_id


def _find_new_databases() -> dict:
    """Sucht die neuen Datenbanken (aggregierte Projekte + Log) unter der Parent Page.

    Unterscheidet neue "Projekte" (mit Mismatch-Property) von alter "Projekte"/Speicherungen.
    """
    parent_id = _get_parent_page_id()
    if not parent_id:
        return {}

    found = {}

    try:
        body = {"filter": {"property": "object", "value": "database"}}
        resp = api_post("search", body)
        for result in resp.get("results", []):
            if result.get("object") != "database":
                continue
            parent = result.get("parent", {})
            result_parent_id = parent.get("page_id", "").replace("-", "")
            if result_parent_id != parent_id.replace("-", ""):
                continue
            title_parts = result.get("title", [])
            title = title_parts[0]["plain_text"] if title_parts else ""
            props = result.get("properties", {})
            if title == "Projekte" and "Mismatch" in props:
                found["aggregated_projects_db_id"] = result["id"]
            elif title == "Log":
                found["log_db_id"] = result["id"]
    except Exception:
        pass

    return found


def ensure_databases() -> tuple[str, str, str, str]:
    """Stellt sicher, dass alle 4 Datenbanken existieren.

    Returns:
        Tuple of (hdd_db_id, projects_db_id, aggregated_db_id, log_db_id)
        where projects_db_id = Speicherungen-DB (abwärtskompatibel).
    """
    config = load_config()
    hdd_db_id = config.get("hdd_db_id")
    projects_db_id = config.get("projects_db_id")
    aggregated_db_id = config.get("aggregated_projects_db_id")
    log_db_id = config.get("log_db_id")

    # Bestehende Kern-DBs validieren
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

    if aggregated_db_id:
        try:
            api_get(f"databases/{aggregated_db_id}")
        except httpx.HTTPStatusError:
            aggregated_db_id = None

    if log_db_id:
        try:
            api_get(f"databases/{log_db_id}")
        except httpx.HTTPStatusError:
            log_db_id = None

    # Bestehende Kern-Datenbanken suchen bevor neue erstellt werden
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

    # DB-Titel-Migration: "Projekte" → "Speicherungen" (idempotent)
    if not config.get("db_title_migrated"):
        try:
            db_info = api_get(f"databases/{projects_db_id}")
            title_parts = db_info.get("title", [])
            title = title_parts[0]["plain_text"] if title_parts else ""
            if title == "Projekte":
                api_patch(f"databases/{projects_db_id}", {
                    "title": [{"type": "text", "text": {"content": "Speicherungen"}}]
                })
                print("  DB umbenannt: 'Projekte' → 'Speicherungen'")
        except Exception:
            pass
        config["db_title_migrated"] = True

    # Neue Datenbanken suchen
    if not aggregated_db_id or not log_db_id:
        found_new = _find_new_databases()
        if not aggregated_db_id:
            aggregated_db_id = found_new.get("aggregated_projects_db_id")
        if not log_db_id:
            log_db_id = found_new.get("log_db_id")

    # Neue Datenbanken erstellen falls nötig
    if not aggregated_db_id:
        aggregated_db_id = create_aggregated_projects_database(hdd_db_id, projects_db_id)
    if not log_db_id:
        log_db_id = create_log_database(aggregated_db_id)

    config["hdd_db_id"] = hdd_db_id
    config["projects_db_id"] = projects_db_id
    config["aggregated_projects_db_id"] = aggregated_db_id
    config["log_db_id"] = log_db_id
    save_config(config)

    migrate_schema(hdd_db_id, projects_db_id, log_db_id, aggregated_db_id)

    return hdd_db_id, projects_db_id, aggregated_db_id, log_db_id


def migrate_schema(hdd_db_id: str, projects_db_id: str, log_db_id: str = "", aggregated_db_id: str = "") -> None:
    """Stellt sicher, dass alle Properties existieren (idempotent)."""
    try:
        api_patch(f"databases/{hdd_db_id}", {
            "properties": {
                "Kapazität (GB)": {"number": {"format": "number"}},
                "Belegt (GB)": {"number": {"format": "number"}},
                "Volume UUID": {"rich_text": {}},
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
                "Letzter Scan": {"date": {}},
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

    # Log: Schema-Migration (neue Typen, Priorität, Status-Option "Umgesetzt")
    # Separate Calls nötig: Notion lehnt Farb-Änderungen bestehender Optionen ab.
    if log_db_id:
        try:
            api_patch(f"databases/{log_db_id}", {
                "properties": {
                    "Status": {"select": {"options": [
                        {"name": "Umgesetzt", "color": "blue"},
                    ]}},
                    "Priorität": {"select": {"options": [
                        {"name": "Kritisch", "color": "red"},
                        {"name": "Warnung", "color": "orange"},
                        {"name": "Info", "color": "gray"},
                    ]}},
                }
            })
        except httpx.HTTPStatusError:
            pass
        try:
            api_patch(f"databases/{log_db_id}", {
                "properties": {
                    "Typ": {"select": {"options": [
                        {"name": "MISSING_BACKUP", "color": "red"},
                        {"name": "INCOMPLETE_BACKUP", "color": "orange"},
                        {"name": "EXCESS_COPIES", "color": "gray"},
                    ]}},
                }
            })
        except httpx.HTTPStatusError:
            pass

    # Projekte (aggregiert): Backup-Status hinzufügen
    if aggregated_db_id:
        try:
            api_patch(f"databases/{aggregated_db_id}", {
                "properties": {
                    "Backup-Status": {"select": {"options": [
                        {"name": "Nicht gesichert", "color": "red"},
                        {"name": "Teilweise", "color": "orange"},
                        {"name": "Vollständig", "color": "green"},
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
        "Zuletzt genutzt von": {"select": {"name": user_name}} if user_name else {"select": None},
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


def sync_projects(projects_db_id: str, report: dict, hdd_page_id: str, scan_date: str = "") -> None:
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
        if scan_date:
            properties["Letzter Scan"] = {"date": {"start": scan_date}}
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
            if scan_date:
                child_props["Letzter Scan"] = {"date": {"start": scan_date}}
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
        if scan_date:
            properties["Letzter Scan"] = {"date": {"start": scan_date}}
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


# ── Aggregation + Mismatch ──────────────────────────────────────────

def _has_size_mismatch(entries: list[dict]) -> bool:
    """Prüft ob Größenunterschiede bei gleichen Typen auf verschiedenen HDDs existieren.

    Threshold: 0% — jeder Unterschied wird gemeldet.
    Typen mit nur 1 Kopie werden ignoriert.
    """
    by_type = defaultdict(list)
    for e in entries:
        if e["type"]:
            by_type[e["type"]].append(e["size_gb"])

    for sizes in by_type.values():
        if len(sizes) >= 2 and len(set(sizes)) > 1:
            return True
    return False


def _compute_backup_status(entries: list[dict]) -> str:
    """Berechnet den Backup-Status eines Projekts.

    Returns:
        "Nicht gesichert" — mindestens 1 Typ nur auf 1 HDD
        "Teilweise" — alle Typen auf 2+ HDDs, aber Größen stimmen nicht überein
        "Vollständig" — alle Typen auf 2+ HDDs, alle Größen identisch
    """
    by_type = defaultdict(list)
    for e in entries:
        if e["type"]:
            by_type[e["type"]].append(e)

    for type_entries in by_type.values():
        unique_hdds = set(e["hdd_id"] for e in type_entries if e["hdd_id"])
        if len(unique_hdds) < 2:
            return "Nicht gesichert"

    for type_entries in by_type.values():
        sizes = set(e["size_gb"] for e in type_entries)
        if len(sizes) > 1:
            return "Teilweise"

    return "Vollständig"


def _upsert_log_entry(
    log_db_id: str,
    log_map: dict,
    log_name: str,
    log_type: str,
    priority: str,
    details: str,
    hdd_names_str: str,
    scan_date: str,
    agg_page_id: str | None = None,
    folder_type: str | None = None,
    diff_gb: float | None = None,
) -> None:
    """Erstellt oder aktualisiert einen Log-Eintrag (gemeinsam für alle 4 Typen).

    - "Umgesetzt" wird nie überschrieben
    - Existing + Resolved → Wiedereröffnen, Erkannt am aktualisieren
    - Existing + Open → Details aktualisieren
    - Neu → Erstellen mit Erkannt am
    """
    existing = log_map.get(log_name)

    if existing and existing["status"] == "Umgesetzt":
        return

    properties = {
        "Name": {"title": [{"text": {"content": log_name}}]},
        "Typ": {"select": {"name": log_type}},
        "Priorität": {"select": {"name": priority}},
        "Details": {"rich_text": [{"text": {"content": details[:2000]}}]},
        "HDDs": {"rich_text": [{"text": {"content": hdd_names_str}}]},
        "Status": {"select": {"name": "Open"}},
    }
    if scan_date:
        properties["Letzter Scan"] = {"date": {"start": scan_date}}
    if agg_page_id:
        properties["Projekt"] = {"relation": [{"id": agg_page_id}]}
    if folder_type:
        properties["Ordnertyp"] = {"select": {"name": folder_type}}
    if diff_gb is not None:
        properties["Differenz (GB)"] = {"number": diff_gb}

    if existing:
        if existing["status"] == "Resolved" and scan_date:
            properties["Erkannt am"] = {"date": {"start": scan_date}}
        api_patch(f"pages/{existing['id']}", {"properties": properties})
    else:
        if scan_date:
            properties["Erkannt am"] = {"date": {"start": scan_date}}
        api_post("pages", {"parent": {"database_id": log_db_id}, "properties": properties})
        print(f"  Log: {log_name}")


def _resolve_log_entry(
    log_map: dict,
    log_name: str,
    scan_date: str,
) -> None:
    """Setzt einen offenen Log-Eintrag auf Resolved."""
    existing = log_map.get(log_name)
    if not existing or existing["status"] != "Open":
        return

    resolve_props: dict = {
        "Status": {"select": {"name": "Resolved"}},
    }
    if scan_date:
        resolve_props["Letzter Scan"] = {"date": {"start": scan_date}}
    api_patch(f"pages/{existing['id']}", {"properties": resolve_props})
    print(f"  Log resolved: {log_name}")


def sync_aggregated_projects(
    aggregated_db_id: str,
    projects_db_id: str,
    hdd_db_id: str,
    scan_date: str,
) -> list[dict]:
    """Aggregiert alle Speicherungen zu Projekt-Übersichten.

    Liest ALLE Valid-Einträge aus der Speicherungen-DB, gruppiert nach
    Datum + Projektname und upserted aggregierte Zeilen.

    Returns:
        Liste der Projektgruppen für Mismatch-Analyse.
    """
    # 1. Alle Valid-Einträge aus Speicherungen laden
    all_entries = query_database(projects_db_id, {
        "property": "Status",
        "select": {"equals": "Valid"},
    })

    # 2. Alle HDD-Seiten laden (für Name-Lookup)
    hdd_pages = query_database(hdd_db_id)
    hdd_names = {}
    for page in hdd_pages:
        title = page["properties"]["Name"]["title"]
        if title:
            hdd_names[page["id"]] = title[0]["plain_text"]

    # 3. Bestehende aggregierte Seiten laden
    existing_agg = query_database(aggregated_db_id)
    existing_map = {}
    for page in existing_agg:
        title = page["properties"]["Name"]["title"]
        if title:
            existing_map[title[0]["plain_text"]] = page["id"]

    # 4. Nach Aggregations-Key gruppieren (Datum_Projektname)
    groups = defaultdict(list)
    for entry in all_entries:
        props = entry["properties"]

        # Datum extrahieren
        date_prop = props.get("Datum", {}).get("date")
        date_str = date_prop["start"] if date_prop else None

        # Projektname extrahieren
        pname_parts = props.get("Projektname", {}).get("rich_text", [])
        project_name = pname_parts[0]["plain_text"] if pname_parts else ""

        # Typ extrahieren
        type_prop = props.get("Typ", {}).get("select")
        folder_type = type_prop["name"] if type_prop else None

        # Größe extrahieren
        size_gb = props.get("Größe (GB)", {}).get("number", 0) or 0

        # HDD-Relation extrahieren
        hdd_rel = props.get("HDD", {}).get("relation", [])
        hdd_id = hdd_rel[0]["id"] if hdd_rel else None
        hdd_name = hdd_names.get(hdd_id, "?") if hdd_id else "?"

        # Ist Kind eines PROJECT-Ordners? (hat Projekt self-relation)
        projekt_rel = props.get("Projekt", {}).get("relation", [])
        is_child = len(projekt_rel) > 0

        if not date_str or not project_name:
            continue

        agg_key = f"{date_str}_{project_name}"
        groups[agg_key].append({
            "page_id": entry["id"],
            "date": date_str,
            "project_name": project_name,
            "type": folder_type,
            "size_gb": size_gb,
            "hdd_id": hdd_id,
            "hdd_name": hdd_name,
            "is_child": is_child,
        })

    # 5. Aggregierte Zeilen upserten
    project_groups = []
    for agg_key, entries in groups.items():
        date_str = entries[0]["date"]
        project_name = entries[0]["project_name"]

        # Typen, HDDs, Speicherungen-IDs sammeln
        types = sorted(set(e["type"] for e in entries if e["type"]))
        hdd_ids = sorted(set(e["hdd_id"] for e in entries if e["hdd_id"]))
        speicherung_ids = [e["page_id"] for e in entries]

        # Gesamtgröße: nur Top-Level-Einträge zählen (keine Children, um Doppelzählung zu vermeiden)
        total_size = round(sum(e["size_gb"] for e in entries if not e["is_child"]), 2)

        # Übersicht-String bauen: "TYPE: HDD (size) | TYPE: HDD (size)"
        by_type = defaultdict(list)
        for e in entries:
            if e["type"]:
                by_type[e["type"]].append(f"{e['hdd_name']} ({e['size_gb']} GB)")
        overview_parts = []
        for t in sorted(by_type.keys()):
            copies = ", ".join(by_type[t])
            overview_parts.append(f"{t}: {copies}")
        overview = " | ".join(overview_parts)

        # Notion rich_text Limit: 2000 Zeichen
        if len(overview) > 2000:
            overview = overview[:1997] + "..."

        # Mismatch + Backup-Status prüfen
        has_mismatch = _has_size_mismatch(entries)
        backup_status = _compute_backup_status(entries)

        properties = {
            "Name": {"title": [{"text": {"content": agg_key}}]},
            "Projektname": {"rich_text": [{"text": {"content": project_name}}]},
            "Datum": {"date": {"start": date_str}},
            "Typen": {"multi_select": [{"name": t} for t in types]},
            "HDDs": {"relation": [{"id": hid} for hid in hdd_ids]},
            "Speicherungen": {"relation": [{"id": sid} for sid in speicherung_ids]},
            "Gesamtgröße (GB)": {"number": total_size},
            "Übersicht": {"rich_text": [{"text": {"content": overview}}]},
            "Mismatch": {"checkbox": has_mismatch},
            "Backup-Status": {"select": {"name": backup_status}},
        }
        if scan_date:
            properties["Letzter Scan"] = {"date": {"start": scan_date}}

        _upsert_page(aggregated_db_id, existing_map, agg_key, properties)

        project_groups.append({
            "agg_key": agg_key,
            "date": date_str,
            "project_name": project_name,
            "entries": entries,
            "has_mismatch": has_mismatch,
        })

    # Aggregierte Seiten archivieren, deren Key keine Einträge mehr hat
    for name, page_id in existing_map.items():
        if name not in groups:
            api_patch(f"pages/{page_id}", {"archived": True})
            print(f"  Aggregiertes Projekt archiviert: {name}")

    print(f"  {len(groups)} aggregierte Projekte synchronisiert")
    return project_groups


def sync_log(
    log_db_id: str,
    aggregated_db_id: str,
    project_groups: list[dict],
    scan_date: str,
) -> None:
    """Erstellt/aktualisiert Log-Einträge für alle 4 Log-Typen.

    - MISSING_BACKUP (Kritisch): Ordnertyp nur auf 1 HDD
    - SIZE_MISMATCH (Warnung): Gleicher Typ, verschiedene Größen
    - INCOMPLETE_BACKUP (Warnung): HDD hat Projekt aber nicht alle Typen
    - EXCESS_COPIES (Info): Ordnertyp auf 3+ HDDs
    """
    # Bestehende Log-Einträge laden
    existing_logs = query_database(log_db_id)
    log_map = {}  # name → {id, status}
    for page in existing_logs:
        title = page["properties"]["Name"]["title"]
        if title:
            name = title[0]["plain_text"]
            status_prop = page["properties"].get("Status", {}).get("select")
            status = status_prop["name"] if status_prop else None
            log_map[name] = {"id": page["id"], "status": status}

    # Aggregierte Projekt-Seiten für Relation-Lookup laden
    agg_pages = query_database(aggregated_db_id)
    agg_page_map = {}
    for page in agg_pages:
        title = page["properties"]["Name"]["title"]
        if title:
            agg_page_map[title[0]["plain_text"]] = page["id"]

    for group in project_groups:
        agg_key = group["agg_key"]
        entries = group["entries"]
        agg_page_id = agg_page_map.get(agg_key)

        # Nach Typ gruppieren
        by_type = defaultdict(list)
        for e in entries:
            if e["type"]:
                by_type[e["type"]].append(e)

        # Alle Typen im Projekt + welche HDDs welche Typen haben
        all_types = set(by_type.keys())
        hdds_with_types: dict[str, set[str]] = defaultdict(set)
        for e in entries:
            if e["type"] and e["hdd_name"]:
                hdds_with_types[e["hdd_name"]].add(e["type"])

        # 1. MISSING_BACKUP — Typ nur auf 1 HDD
        for type_name, type_entries in by_type.items():
            unique_hdds = set(e["hdd_name"] for e in type_entries if e["hdd_name"])
            log_name = f"MISSING_BACKUP: {agg_key} {type_name}"

            if len(unique_hdds) < 2:
                entry = type_entries[0]
                size_str = f"{entry['size_gb']} GB"
                hdd_name = next(iter(unique_hdds)) if unique_hdds else "?"
                details = f"Nur auf {hdd_name} ({size_str})"
                _upsert_log_entry(
                    log_db_id, log_map, log_name,
                    log_type="MISSING_BACKUP", priority="Kritisch",
                    details=details, hdd_names_str=hdd_name,
                    scan_date=scan_date, agg_page_id=agg_page_id,
                    folder_type=type_name,
                )
            else:
                _resolve_log_entry(log_map, log_name, scan_date)

        # 2. SIZE_MISMATCH — Typ auf 2+ HDDs, verschiedene Größen
        for type_name, type_entries in by_type.items():
            unique_hdds = set(e["hdd_name"] for e in type_entries if e["hdd_name"])
            if len(unique_hdds) < 2:
                continue

            log_name = f"MISMATCH: {agg_key} {type_name}"
            sizes = [e["size_gb"] for e in type_entries]
            has_diff = len(set(sizes)) > 1

            if has_diff:
                details_parts = [f"{e['hdd_name']}: {e['size_gb']} GB" for e in type_entries]
                details = " vs ".join(details_parts)
                diff = round(max(sizes) - min(sizes), 2)
                details += f" (diff: {diff} GB)"
                hdd_names_str = ", ".join(e["hdd_name"] for e in type_entries)
                _upsert_log_entry(
                    log_db_id, log_map, log_name,
                    log_type="SIZE_MISMATCH", priority="Warnung",
                    details=details, hdd_names_str=hdd_names_str,
                    scan_date=scan_date, agg_page_id=agg_page_id,
                    folder_type=type_name, diff_gb=diff,
                )
            else:
                _resolve_log_entry(log_map, log_name, scan_date)

        # 3. INCOMPLETE_BACKUP — HDD hat Projekt aber nicht alle Typen
        for hdd_name, hdd_types in hdds_with_types.items():
            log_name = f"INCOMPLETE: {agg_key} {hdd_name}"
            missing_types = all_types - hdd_types

            if missing_types:
                # Größen der fehlenden Typen von anderen HDDs sammeln
                missing_parts = []
                for mt in sorted(missing_types):
                    sizes_for_type = [e["size_gb"] for e in by_type[mt]]
                    avg_size = round(sum(sizes_for_type) / len(sizes_for_type), 1)
                    missing_parts.append(f"{mt} ({avg_size} GB)")
                details = "Fehlt: " + ", ".join(missing_parts)
                _upsert_log_entry(
                    log_db_id, log_map, log_name,
                    log_type="INCOMPLETE_BACKUP", priority="Warnung",
                    details=details, hdd_names_str=hdd_name,
                    scan_date=scan_date, agg_page_id=agg_page_id,
                )
            else:
                _resolve_log_entry(log_map, log_name, scan_date)

        # 4. EXCESS_COPIES — Typ auf 3+ HDDs
        for type_name, type_entries in by_type.items():
            unique_hdds = sorted(set(e["hdd_name"] for e in type_entries if e["hdd_name"]))
            log_name = f"EXCESS: {agg_key} {type_name}"

            if len(unique_hdds) >= 3:
                copies_parts = []
                for e in type_entries:
                    copies_parts.append(f"{e['hdd_name']} ({e['size_gb']} GB)")
                details = f"{len(unique_hdds)} Kopien: " + ", ".join(copies_parts)
                hdd_names_str = ", ".join(unique_hdds)
                _upsert_log_entry(
                    log_db_id, log_map, log_name,
                    log_type="EXCESS_COPIES", priority="Info",
                    details=details, hdd_names_str=hdd_names_str,
                    scan_date=scan_date, agg_page_id=agg_page_id,
                    folder_type=type_name,
                )
            else:
                _resolve_log_entry(log_map, log_name, scan_date)


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
    hdd_db_id, projects_db_id, aggregated_db_id, log_db_id = ensure_databases()

    scan_date = report["scan_info"].get("scan_date", "")
    hdd_page_id = sync_hdd(hdd_db_id, report, disk_info, user_name)
    sync_projects(projects_db_id, report, hdd_page_id, scan_date)

    # Aggregation + Mismatch-Log
    project_groups = sync_aggregated_projects(aggregated_db_id, projects_db_id, hdd_db_id, scan_date)
    sync_log(log_db_id, aggregated_db_id, project_groups, scan_date)

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

    scan_date = report["scan_info"].get("scan_date", "")
    hdd_db_id, projects_db_id, aggregated_db_id, log_db_id = ensure_databases()
    hdd_page_id = sync_hdd(hdd_db_id, report, disk_info, user_name)
    sync_projects(projects_db_id, report, hdd_page_id, scan_date)


def run_analysis() -> None:
    """Führt nur die Aggregation + Mismatch-Log-Analyse aus (ohne Scan).

    Liest alle Daten aus den bestehenden Notion-Datenbanken und aktualisiert
    die Projekte-Übersicht sowie das Log. Wird manuell über das Menü ausgelöst.
    """
    from datetime import date

    hdd_db_id, projects_db_id, aggregated_db_id, log_db_id = ensure_databases()
    scan_date = date.today().isoformat()

    project_groups = sync_aggregated_projects(aggregated_db_id, projects_db_id, hdd_db_id, scan_date)
    sync_log(log_db_id, aggregated_db_id, project_groups, scan_date)

    print("\nAuswertung abgeschlossen!")


if __name__ == "__main__":
    main()
