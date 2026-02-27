"""Einmalige Migration – Räumt die Notion-Datenbanken auf.

1. Benennt "HDDs" → "Datenträger" um
2. Rechnet bestehende Größen-Strings in GB-Zahlen um
3. Entfernt alte Spalten
"""

import json
from pathlib import Path

import httpx

from .paths import CONFIG_PATH

API = "https://api.notion.com/v1"


def _get_headers() -> dict:
    config = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {}
    token = config.get("notion_token", "")
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def api_post(endpoint, body):
    resp = httpx.post(f"{API}/{endpoint}", headers=_get_headers(), json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


def api_patch(endpoint, body):
    resp = httpx.patch(f"{API}/{endpoint}", headers=_get_headers(), json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


def query_all(db_id):
    results, body = [], {}
    while True:
        resp = api_post(f"databases/{db_id}/query", body)
        results.extend(resp["results"])
        if not resp["has_more"]:
            break
        body["start_cursor"] = resp["next_cursor"]
    return results


def read_rich_text(page, prop_name):
    """Liest einen rich_text-Wert als String."""
    prop = page["properties"].get(prop_name)
    if not prop or not prop.get("rich_text"):
        return ""
    parts = prop["rich_text"]
    return parts[0]["plain_text"] if parts else ""


def read_number(page, prop_name):
    """Liest einen number-Wert."""
    prop = page["properties"].get(prop_name)
    if not prop:
        return None
    return prop.get("number")


def parse_size_to_gb(size_str):
    """Wandelt '194.6 GB' oder '1.8 TB' in eine GB-Zahl um."""
    if not size_str or not size_str.strip():
        return None
    parts = size_str.strip().split()
    if len(parts) != 2:
        return None
    try:
        value = float(parts[0])
    except ValueError:
        return None
    unit = parts[1].upper()
    multipliers = {"B": 1 / (1024**3), "KB": 1 / (1024**2), "MB": 1 / 1024, "GB": 1, "TB": 1024}
    factor = multipliers.get(unit)
    if factor is None:
        return None
    return round(value * factor, 2)


def bytes_to_gb(size_bytes):
    if size_bytes is None:
        return None
    return round(size_bytes / (1024**3), 2)


def main():
    config = json.loads(CONFIG_PATH.read_text())
    hdd_db_id = config["hdd_db_id"]
    projects_db_id = config["projects_db_id"]

    # ── 1. Neue Spalten sicherstellen ────────────────────────────────
    print("Schema aktualisieren...")

    api_patch(f"databases/{hdd_db_id}", {
        "title": [{"type": "text", "text": {"content": "Datenträger"}}],
        "properties": {
            "Kapazität (GB)": {"number": {"format": "number"}},
            "Belegt (GB)": {"number": {"format": "number"}},
            "Volume UUID": {"rich_text": {}},
        }
    })
    print("  Datenträger-DB: umbenannt + neue Spalten angelegt")

    api_patch(f"databases/{projects_db_id}", {
        "properties": {
            "Größe (GB)": {"number": {"format": "number"}},
            "Absoluter Pfad": {"rich_text": {}},
            "Letzte Änderung": {"date": {}},
        }
    })
    print("  Projekte-DB: neue Spalten angelegt")

    # ── 2. Datenträger-Einträge migrieren ────────────────────────────
    print("\nDatenträger migrieren...")
    hdd_pages = query_all(hdd_db_id)

    for page in hdd_pages:
        title_parts = page["properties"]["Name"]["title"]
        name = title_parts[0]["plain_text"] if title_parts else "?"

        updates = {}

        # Kapazität: aus "Gesamtkapazität" (rich_text) → Kapazität (GB) (number)
        if read_number(page, "Kapazität (GB)") is None:
            raw = read_rich_text(page, "Gesamtkapazität")
            gb = parse_size_to_gb(raw)
            if gb is not None:
                updates["Kapazität (GB)"] = {"number": gb}

        # Belegt: aus "Belegt" (rich_text) → Belegt (GB) (number)
        if read_number(page, "Belegt (GB)") is None:
            raw = read_rich_text(page, "Belegt")
            gb = parse_size_to_gb(raw)
            if gb is not None:
                updates["Belegt (GB)"] = {"number": gb}

        if updates:
            api_patch(f"pages/{page['id']}", {"properties": updates})
            print(f"  {name}: {', '.join(updates.keys())} gesetzt")
        else:
            print(f"  {name}: bereits aktuell")

    # ── 3. Projekte-Einträge migrieren ───────────────────────────────
    print("\nProjekte migrieren...")
    project_pages = query_all(projects_db_id)

    for page in project_pages:
        title_parts = page["properties"]["Name"]["title"]
        name = title_parts[0]["plain_text"] if title_parts else "?"

        # Größe (GB): aus "Größe (Bytes)" (number) umrechnen
        if read_number(page, "Größe (GB)") is None:
            size_bytes = read_number(page, "Größe (Bytes)")
            gb = bytes_to_gb(size_bytes)
            if gb is not None:
                api_patch(f"pages/{page['id']}", {
                    "properties": {"Größe (GB)": {"number": gb}}
                })
                print(f"  {name}: Größe (GB) = {gb}")
            else:
                # Fallback: aus "Größe" (rich_text) parsen
                raw = read_rich_text(page, "Größe")
                gb = parse_size_to_gb(raw)
                if gb is not None:
                    api_patch(f"pages/{page['id']}", {
                        "properties": {"Größe (GB)": {"number": gb}}
                    })
                    print(f"  {name}: Größe (GB) = {gb} (aus Text)")
                else:
                    print(f"  {name}: keine Größe gefunden")
        else:
            print(f"  {name}: bereits aktuell")

    # ── 4. Alte Spalten entfernen ────────────────────────────────────
    print("\nAlte Spalten entfernen...")

    api_patch(f"databases/{hdd_db_id}", {
        "properties": {
            "Gesamtkapazität": None,
            "Belegt": None,
            "Verfügbar": None,
            "Frei (GB)": None,
        }
    })
    print("  Datenträger: Gesamtkapazität, Belegt, Verfügbar, Frei (GB) entfernt")

    api_patch(f"databases/{projects_db_id}", {
        "properties": {
            "Größe": None,
            "Größe (Bytes)": None,
        }
    })
    print("  Projekte: Größe, Größe (Bytes) entfernt")

    print("\nMigration abgeschlossen!")


if __name__ == "__main__":
    main()
