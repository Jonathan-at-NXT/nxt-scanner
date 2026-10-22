-- Dashboard: menschliche Eingaben (alles additiv, Scanner-Daten bleiben unberührt).

-- Datenträger: Speicherstufe, Standort, wer sie hat (überschreibt last_user), Notiz
CREATE TABLE IF NOT EXISTS disk_meta (
    disk_uuid TEXT PRIMARY KEY,
    tier TEXT,                   -- SSD | HDD | NAS | CLOUD
    tier_source TEXT,            -- manual | notion | guess
    holder TEXT,
    location TEXT,
    note TEXT,
    separate_disk INTEGER,       -- 1 = trotz gleichem Namen eigene physische Platte (zählt als Kopie)
    updated_by TEXT,
    updated_at TEXT
);

-- Sicherungsregel je Projekt. rule_key = "DATUM|NORMNAME" eines Mitglieds
-- (stabil, auch wenn sich das Projekt-Cluster neu bildet).
CREATE TABLE IF NOT EXISTS project_rule (
    rule_key TEXT PRIMARY KEY,
    sicherung TEXT,              -- loeschbar | einfach | doppelt | dreifach
    require_tiers TEXT,          -- kommagetrennt, z. B. "NAS" oder "NAS,SSD"
    note TEXT,
    updated_by TEXT,
    updated_at TEXT
);

-- Soll-Name je Ordner (wie Notion "Soll-Name"): ordnet falsch benannte Ordner einem
-- Projekt zu und erzeugt einen Umbenennen-Befund.
CREATE TABLE IF NOT EXISTS folder_override (
    disk_uuid TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    soll_name TEXT,
    updated_by TEXT,
    updated_at TEXT,
    PRIMARY KEY (disk_uuid, rel_path)
);

-- Befund-Status, den Menschen setzen (Offen/Erledigt ergibt sich aus den Daten)
CREATE TABLE IF NOT EXISTS finding_state (
    finding_key TEXT PRIMARY KEY,
    status TEXT,                 -- umgesetzt | ignoriert
    note TEXT,
    updated_by TEXT,
    updated_at TEXT
);

-- Wer hat wann was geändert (Pflicht bei Team-Bearbeitung)
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    who TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_at ON audit_log(at);

-- Kleine Schlüssel/Wert-Ablage (z. B. Stand der letzten Ableitung)
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Ableitung kennt manuelle Zuordnungen
ALTER TABLE projects ADD COLUMN soll_name TEXT;
