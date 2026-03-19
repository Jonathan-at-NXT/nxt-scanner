"""Admin-only Features – Fullfilment Sync.

Matcht Projekte aus der Scanner-Projekte-DB (aggregiert) gegen die Fullfilment-DB
und verlinkt die zugehörigen Datenträger als Relation.
Verlinkt Projekte per Relation (nicht Checkbox) mit der Fullfilment-DB.
Bestehende manuelle Verlinkungen werden nicht überschrieben.
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
    _ensure_fullfilment_relation(aggregated_db_id, fullfilment_db_id)

    # 2. Daten aus Notion ziehen
    print("  Fullfilment Sync: Lade Datenträger...")
    dt_map = _pull_datentraeger_names(datentraeger_db_id)

    print("  Fullfilment Sync: Lade Projekte...")
    projekte = _pull_projekte(aggregated_db_id, dt_map)

    print("  Fullfilment Sync: Lade Fullfilment...")
    fullfilment_entries = _pull_fullfilment(fullfilment_db_id)

    # 3. Bereits verlinkte Projekte zählen
    already_linked = sum(1 for p in projekte if p["fullfilment_id"])

    # 4. SQLite-Matching (nur für unverlinkte Projekte)
    print("  Fullfilment Sync: Matching...")
    matches = _match_in_sqlite(projekte, fullfilment_entries)

    # 5. Neue Matches: Fullfilment-Relation in Projekte-DB setzen
    print(f"  Fullfilment Sync: {len(matches)} neue Matches...")
    new_linked = _write_new_matches(matches)

    # Projekte-Liste aktualisieren (neu verlinkte Projekte)
    matched_ff_ids = {m["proj_page_id"]: m["ff_page_id"] for m in matches if m["proj_page_id"]}
    for proj in projekte:
        if proj["page_id"] in matched_ff_ids:
            proj["fullfilment_id"] = matched_ff_ids[proj["page_id"]]

    # 6. Datenträger für ALLE verlinkten Projekte in Fullfilment-DB syncen
    print("  Fullfilment Sync: Datenträger aktualisieren...")
    updated_ff = _sync_datentraeger_to_fullfilment(projekte, fullfilment_entries)

    total_linked = already_linked + new_linked
    return {
        "total_fullfilment": len(fullfilment_entries),
        "total_projekte": len(projekte),
        "already_linked": already_linked,
        "new_linked": new_linked,
        "total_linked": total_linked,
        "updated_fullfilment": updated_ff,
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


def _ensure_fullfilment_relation(db_id: str, fullfilment_db_id: str) -> None:
    """Erstellt 'Fullfilment' Relation in Projekte-DB falls nicht vorhanden.

    Migriert bestehende Checkbox zu Relation falls nötig.
    """
    db_info = api_get(f"databases/{db_id}")
    props = db_info.get("properties", {})
    ff_prop = props.get("Fullfilment")

    if not ff_prop:
        # Neu erstellen als Relation
        api_patch(f"databases/{db_id}", {
            "properties": {
                "Fullfilment": {
                    "relation": {"database_id": fullfilment_db_id, "type": "single_property", "single_property": {}},
                },
            }
        })
        print("  Projekte DB: 'Fullfilment' Relation erstellt")
    elif ff_prop.get("type") == "checkbox":
        # Migration: Checkbox → Relation (alte Property löschen, neue erstellen)
        api_patch(f"databases/{db_id}", {"properties": {"Fullfilment": None}})
        api_patch(f"databases/{db_id}", {
            "properties": {
                "Fullfilment": {
                    "relation": {"database_id": fullfilment_db_id, "type": "single_property", "single_property": {}},
                },
            }
        })
        print("  Projekte DB: 'Fullfilment' von Checkbox zu Relation migriert")


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
    """Lädt alle Projekte mit page_id, Projektname, Datenträger-IDs und Fullfilment-Relation."""
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

        # Bestehende Fullfilment-Relation lesen
        ff_rel = props.get("Fullfilment", {}).get("relation", [])
        fullfilment_id = ff_rel[0]["id"] if ff_rel else None

        results.append({
            "page_id": page["id"],
            "project_name": project_name,
            "datentraeger_ids": dt_ids,
            "datentraeger_str": ", ".join(dt_names) if dt_names else "",
            "fullfilment_id": fullfilment_id,
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
    """Exaktes Matching per In-Memory-SQLite (case-insensitive).

    Nur Projekte OHNE bestehende Fullfilment-Relation werden gematcht.
    """
    # Nur Projekte ohne bestehende Verlinkung matchen
    unlinked = [e for e in projekte if not e["fullfilment_id"]]

    # Lookup für Datenträger-IDs nach Projektname
    proj_dt_ids = {}
    for e in projekte:
        proj_dt_ids.setdefault(e["project_name"].upper(), e["datentraeger_ids"])

    # Lookup für page_id nach Projektname (für Relation-Schreibung)
    proj_page_ids = {}
    for e in projekte:
        proj_page_ids.setdefault(e["project_name"].upper(), e["page_id"])

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
        [(e["project_name"],) for e in unlinked],
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
            "ff_page_id": row[0],
            "title": row[1],
            "project_name": project_name,
            "proj_page_id": proj_page_ids.get(project_name.upper()),
            "datentraeger_ids": proj_dt_ids.get(project_name.upper(), []),
            "existing_datentraeger_ids": ff_existing.get(row[0], []),
        })

    conn.close()
    return matches


def _write_new_matches(matches: list[dict]) -> int:
    """Schreibt Fullfilment-Relation in die Projekte-DB für neue Matches."""
    updated = 0
    for match in matches:
        if not match["proj_page_id"] or not match["ff_page_id"]:
            continue
        _api_patch_retry(f"pages/{match['proj_page_id']}", {
            "properties": {
                "Fullfilment": {"relation": [{"id": match["ff_page_id"]}]},
            },
        })
        updated += 1
        print(f"  Verlinkt: {match['project_name']} → {match['title']}")
    return updated


def _sync_datentraeger_to_fullfilment(projekte: list[dict], fullfilment_entries: list[dict]) -> int:
    """Synct Datenträger-Relationen für alle verlinkten Projekte in die Fullfilment-DB.

    Für jedes Projekt mit Fullfilment-Relation: Schreibt die Datenträger des
    Projekts als Relation in den verlinkten Fullfilment-Eintrag.
    """
    # Lookup: Fullfilment page_id → bestehende Datenträger-IDs
    ff_existing_dt = {e["page_id"]: e["existing_datentraeger_ids"] for e in fullfilment_entries}

    updated = 0
    for proj in projekte:
        ff_id = proj["fullfilment_id"]
        if not ff_id:
            continue

        new_ids = sorted(proj["datentraeger_ids"])
        existing_ids = sorted(ff_existing_dt.get(ff_id, []))

        if new_ids == existing_ids or not new_ids:
            continue

        _api_patch_retry(f"pages/{ff_id}", {
            "properties": {
                "Datenträger": {
                    "relation": [{"id": hid} for hid in new_ids],
                },
            },
        })
        updated += 1
        print(f"  {proj['project_name']} → {len(new_ids)} Datenträger in Fullfilment aktualisiert")

    return updated
