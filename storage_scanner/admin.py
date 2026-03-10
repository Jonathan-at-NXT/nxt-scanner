"""Admin-only Features – Fullfilment Sync.

Matcht Projekte aus der Scanner-Projekte-DB (aggregiert) gegen die Fullfilment-DB
und verlinkt die zugehörigen Datenträger als Relation.
Setzt außerdem eine "Fullfilment"-Checkbox in der Projekte-DB.
Nutzt eine In-Memory-SQLite-DB für schnelles Matching.
"""

import sqlite3
import time

from .notion_sync import (
    load_config,
    api_get,
    api_patch,
    query_database,
)


def _api_patch_retry(endpoint: str, body: dict, retries: int = 3) -> dict:
    """api_patch mit Retry und Rate-Limiting."""
    for attempt in range(retries):
        try:
            result = api_patch(endpoint, body)
            time.sleep(0.35)
            return result
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def run_fullfilment_sync() -> dict:
    """Hauptfunktion für Fullfilment-Sync.

    Returns:
        Dict mit Statistiken: {total_fullfilment, total_projekte, matched, updated, skipped}
    """
    config = load_config()

    fullfilment_db_id = config.get("fullfilment_db_id")
    if not fullfilment_db_id:
        raise ValueError("fullfilment_db_id nicht in Config gesetzt")

    aggregated_db_id = config.get("aggregated_projects_db_id")
    datentraeger_db_id = config.get("hdd_db_id")
    if not aggregated_db_id or not datentraeger_db_id:
        raise ValueError("aggregated_projects_db_id oder hdd_db_id nicht in Config gesetzt")

    # 1. Properties sicherstellen
    _ensure_fullfilment_property(fullfilment_db_id, datentraeger_db_id)
    _ensure_projekte_checkbox(aggregated_db_id)

    # 2. Daten aus Notion ziehen
    print("  Fullfilment Sync: Lade Datenträger...")
    dt_map = _pull_datentraeger_names(datentraeger_db_id)

    print("  Fullfilment Sync: Lade Projekte...")
    projekte = _pull_projekte(aggregated_db_id, dt_map)

    print("  Fullfilment Sync: Lade Fullfilment...")
    fullfilment_entries = _pull_fullfilment(fullfilment_db_id)

    # 3. SQLite-Matching
    print("  Fullfilment Sync: Matching...")
    matches = _match_in_sqlite(projekte, fullfilment_entries)
    matched_project_names = {m["project_name"] for m in matches}

    # 4. Fullfilment-DB: Datenträger verlinken
    print(f"  Fullfilment Sync: {len(matches)} Matches, verlinke Datenträger...")
    updated_ff = _write_matches_to_fullfilment(matches)

    # 5. Projekte-DB: Checkbox setzen/entfernen
    print("  Fullfilment Sync: Aktualisiere Projekte-Checkbox...")
    updated_proj = _update_projekte_checkbox(projekte, matched_project_names)

    return {
        "total_fullfilment": len(fullfilment_entries),
        "total_projekte": len(projekte),
        "matched": len(matches),
        "updated_fullfilment": updated_ff,
        "updated_projekte": updated_proj,
    }


def _ensure_fullfilment_property(db_id: str, hdd_db_id: str) -> None:
    """Erstellt 'Datenträger' Relation in Fullfilment-DB falls nicht vorhanden.

    Migriert bestehende rich_text-Property zu Relation falls nötig.
    """
    db_info = api_get(f"databases/{db_id}")
    props = db_info.get("properties", {})
    dt_prop = props.get("Datenträger")

    if not dt_prop:
        # Neu erstellen als Relation
        api_patch(f"databases/{db_id}", {
            "properties": {
                "Datenträger": {
                    "relation": {"database_id": hdd_db_id, "type": "single_property", "single_property": {}},
                },
            }
        })
        print("  Fullfilment DB: 'Datenträger' Relation erstellt")
    elif dt_prop.get("type") == "rich_text":
        # Migration: rich_text → Relation (alte Property löschen, neue erstellen)
        api_patch(f"databases/{db_id}", {"properties": {"Datenträger": None}})
        api_patch(f"databases/{db_id}", {
            "properties": {
                "Datenträger": {
                    "relation": {"database_id": hdd_db_id, "type": "single_property", "single_property": {}},
                },
            }
        })
        print("  Fullfilment DB: 'Datenträger' von Text zu Relation migriert")


def _ensure_projekte_checkbox(db_id: str) -> None:
    """Erstellt 'Fullfilment' checkbox Property in Projekte-DB falls nicht vorhanden."""
    db_info = api_get(f"databases/{db_id}")
    if "Fullfilment" not in db_info.get("properties", {}):
        api_patch(f"databases/{db_id}", {
            "properties": {
                "Fullfilment": {"checkbox": {}},
            }
        })
        print("  Projekte DB: 'Fullfilment' Checkbox erstellt")


def _pull_datentraeger_names(datentraeger_db_id: str) -> dict[str, str]:
    """Gibt Mapping von Datenträger page_id → Name zurück."""
    pages = query_database(datentraeger_db_id)
    dt_map = {}
    for page in pages:
        title = page["properties"].get("Name", {}).get("title", [])
        if title:
            dt_map[page["id"]] = title[0]["plain_text"]
    return dt_map


def _pull_projekte(aggregated_db_id: str, dt_map: dict[str, str]) -> list[dict]:
    """Lädt alle Projekte mit page_id, Projektname, Datenträger-IDs und aktuellem Checkbox-Wert."""
    pages = query_database(aggregated_db_id)

    results = []
    for page in pages:
        props = page["properties"]

        pname_parts = props.get("Projektname", {}).get("rich_text", [])
        project_name = pname_parts[0]["plain_text"].strip() if pname_parts else ""
        if not project_name:
            continue

        # Datenträger-Relation auflösen (Property heißt "Datenträger", Fallback auf "HDDs")
        dt_rel = props.get("Datenträger", props.get("HDDs", {})).get("relation", [])
        dt_ids = [rel["id"] for rel in dt_rel]
        dt_names = sorted(dt_map.get(rid, "") for rid in dt_ids)
        dt_names = [n for n in dt_names if n]

        # Aktueller Checkbox-Wert
        has_fullfilment = props.get("Fullfilment", {}).get("checkbox", False)

        results.append({
            "page_id": page["id"],
            "project_name": project_name,
            "datentraeger_ids": dt_ids,
            "datentraeger_str": ", ".join(dt_names) if dt_names else "",
            "has_fullfilment": has_fullfilment,
        })

    return results


def _pull_fullfilment(fullfilment_db_id: str) -> list[dict]:
    """Lädt alle Einträge aus der Fullfilment-DB."""
    pages = query_database(fullfilment_db_id)

    results = []
    for page in pages:
        props = page["properties"]

        title_parts = props.get("TITLE", {}).get("title", [])
        title = "".join(t["plain_text"] for t in title_parts).strip() if title_parts else ""
        if not title:
            continue

        # Bestehende Datenträger-Relation lesen
        dt_rel = props.get("Datenträger", {}).get("relation", [])
        existing_dt_ids = sorted(r["id"] for r in dt_rel)

        results.append({
            "page_id": page["id"],
            "title": title,
            "existing_datentraeger_ids": existing_dt_ids,
        })

    return results


def _match_in_sqlite(
    projekte: list[dict],
    fullfilment_entries: list[dict],
) -> list[dict]:
    """Exaktes Matching per In-Memory-SQLite (case-insensitive)."""
    # Lookup für Datenträger-IDs nach Projektname
    proj_dt_ids = {}
    for e in projekte:
        proj_dt_ids.setdefault(e["project_name"].upper(), e["datentraeger_ids"])

    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE projekte (
            project_name TEXT COLLATE NOCASE
        )
    """)
    cur.execute("""
        CREATE TABLE fullfilment (
            page_id TEXT,
            title TEXT COLLATE NOCASE
        )
    """)

    cur.executemany(
        "INSERT INTO projekte VALUES (?)",
        [(e["project_name"],) for e in projekte],
    )
    cur.executemany(
        "INSERT INTO fullfilment VALUES (?, ?)",
        [(e["page_id"], e["title"]) for e in fullfilment_entries],
    )

    cur.execute("""
        SELECT DISTINCT
            f.page_id,
            f.title,
            p.project_name
        FROM fullfilment f
        INNER JOIN projekte p
            ON f.title = p.project_name
    """)

    # Lookup für bestehende Datenträger-IDs in Fullfilment
    ff_existing = {e["page_id"]: e["existing_datentraeger_ids"] for e in fullfilment_entries}

    matches = []
    for row in cur.fetchall():
        project_name = row[2]
        matches.append({
            "page_id": row[0],
            "title": row[1],
            "project_name": project_name,
            "datentraeger_ids": proj_dt_ids.get(project_name.upper(), []),
            "existing_datentraeger_ids": ff_existing.get(row[0], []),
        })

    conn.close()
    return matches


def _write_matches_to_fullfilment(matches: list[dict]) -> int:
    """Verlinkt Datenträger in Fullfilment-DB. Überspringt unveränderte."""
    updated = 0
    for match in matches:
        new_ids = sorted(match["datentraeger_ids"])
        existing_ids = sorted(match["existing_datentraeger_ids"])
        if new_ids == existing_ids:
            continue

        if not new_ids:
            continue

        _api_patch_retry(f"pages/{match['page_id']}", {
            "properties": {
                "Datenträger": {
                    "relation": [{"id": hid} for hid in new_ids],
                },
            },
        })
        updated += 1
        print(f"  {match['title']} -> {len(new_ids)} Datenträger verlinkt")

    return updated


def _update_projekte_checkbox(projekte: list[dict], matched_names: set[str]) -> int:
    """Setzt/entfernt Fullfilment-Checkbox in der Projekte-DB."""
    matched_upper = {n.upper() for n in matched_names}
    updated = 0
    for proj in projekte:
        should_be = proj["project_name"].upper() in matched_upper
        if proj["has_fullfilment"] == should_be:
            continue

        _api_patch_retry(f"pages/{proj['page_id']}", {
            "properties": {
                "Fullfilment": {"checkbox": should_be},
            },
        })
        updated += 1

    return updated
