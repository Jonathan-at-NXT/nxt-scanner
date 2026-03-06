"""Admin-only Features – Fullfilment Sync.

Matcht Projekte aus der Scanner-Projekte-DB (aggregiert) gegen die Fullfilment-DB
und schreibt die zugehörigen Datenträger-Namen als Property zurück.
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
    _ensure_fullfilment_property(fullfilment_db_id)
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

    # 4. Fullfilment-DB: Datenträger-Namen schreiben
    print(f"  Fullfilment Sync: {len(matches)} Matches, schreibe Datenträger...")
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


def _ensure_fullfilment_property(db_id: str) -> None:
    """Erstellt 'Datenträger' rich_text Property in Fullfilment-DB falls nicht vorhanden."""
    db_info = api_get(f"databases/{db_id}")
    if "Datenträger" not in db_info.get("properties", {}):
        api_patch(f"databases/{db_id}", {
            "properties": {
                "Datenträger": {"rich_text": {}},
            }
        })
        print("  Fullfilment DB: 'Datenträger' Property erstellt")


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
        title = page["properties"]["Name"]["title"]
        if title:
            dt_map[page["id"]] = title[0]["plain_text"]
    return dt_map


def _pull_projekte(aggregated_db_id: str, dt_map: dict[str, str]) -> list[dict]:
    """Lädt alle Projekte mit page_id, Projektname, Datenträger und aktuellem Checkbox-Wert."""
    pages = query_database(aggregated_db_id)

    results = []
    for page in pages:
        props = page["properties"]

        pname_parts = props.get("Projektname", {}).get("rich_text", [])
        project_name = pname_parts[0]["plain_text"].strip() if pname_parts else ""
        if not project_name:
            continue

        # HDDs-Relation → Datenträger-Namen auflösen
        hdds_rel = props.get("HDDs", {}).get("relation", [])
        dt_names = sorted(dt_map.get(rel["id"], "") for rel in hdds_rel)
        dt_names = [n for n in dt_names if n]

        # Aktueller Checkbox-Wert
        has_fullfilment = props.get("Fullfilment", {}).get("checkbox", False)

        results.append({
            "page_id": page["id"],
            "project_name": project_name,
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

        dt_parts = props.get("Datenträger", {}).get("rich_text", [])
        existing_dt = dt_parts[0]["plain_text"] if dt_parts else ""

        results.append({
            "page_id": page["id"],
            "title": title,
            "existing_datentraeger": existing_dt,
        })

    return results


def _match_in_sqlite(
    projekte: list[dict],
    fullfilment_entries: list[dict],
) -> list[dict]:
    """Exaktes Matching per In-Memory-SQLite (case-insensitive)."""
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE projekte (
            project_name TEXT COLLATE NOCASE,
            datentraeger_str TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE fullfilment (
            page_id TEXT,
            title TEXT COLLATE NOCASE,
            existing_datentraeger TEXT
        )
    """)

    cur.executemany(
        "INSERT INTO projekte VALUES (?, ?)",
        [(e["project_name"], e["datentraeger_str"]) for e in projekte],
    )
    cur.executemany(
        "INSERT INTO fullfilment VALUES (?, ?, ?)",
        [(e["page_id"], e["title"], e["existing_datentraeger"]) for e in fullfilment_entries],
    )

    cur.execute("""
        SELECT
            f.page_id,
            f.title,
            p.datentraeger_str,
            f.existing_datentraeger,
            p.project_name
        FROM fullfilment f
        INNER JOIN projekte p
            ON f.title = p.project_name
    """)

    matches = []
    for row in cur.fetchall():
        matches.append({
            "page_id": row[0],
            "title": row[1],
            "datentraeger_str": row[2],
            "existing_datentraeger": row[3] or "",
            "project_name": row[4],
        })

    conn.close()
    return matches


def _write_matches_to_fullfilment(matches: list[dict]) -> int:
    """Schreibt Datenträger-Namen in Fullfilment-DB. Überspringt unveränderte."""
    updated = 0
    for match in matches:
        new_value = match["datentraeger_str"]
        if new_value == match["existing_datentraeger"]:
            continue

        _api_patch_retry(f"pages/{match['page_id']}", {
            "properties": {
                "Datenträger": {
                    "rich_text": [{"text": {"content": new_value}}],
                },
            },
        })
        updated += 1
        print(f"  {match['title']} -> {new_value}")

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
