-- NXT Scanner — zentrales D1-Schema
-- Rohdaten (von Scannern gepusht): disks + folder_tree
-- Abgeleitet (derive.py): projects + scans_log

CREATE TABLE IF NOT EXISTS disks (
    uuid TEXT PRIMARY KEY,
    name TEXT,
    fs_type TEXT,
    is_network INTEGER,
    used_bytes INTEGER,
    capacity_bytes INTEGER,
    last_scan TEXT,
    last_user TEXT,
    fs_used_bytes INTEGER        -- echte Belegung laut Dateisystem (Notion "Belegt")
);

CREATE TABLE IF NOT EXISTS folder_tree (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    disk_uuid TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    depth INTEGER,
    parent_rel_path TEXT,
    size_bytes INTEGER,
    file_count INTEGER,
    mtime TEXT,
    abs_path TEXT,               -- realer Pfad inkl. ausgeblendeter Container (NXT STUDIOS/)
    UNIQUE(disk_uuid, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_tree_disk_depth ON folder_tree(disk_uuid, depth);

-- Zentral abgeleitet (nicht von Scannern beschrieben)
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    disk_uuid TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    name TEXT,
    date TEXT,
    project_name TEXT,
    norm_name TEXT,
    type TEXT,
    status TEXT,                 -- validated | unassigned | container
    size_bytes INTEGER,
    file_count INTEGER,
    mtime TEXT,
    cluster_id TEXT,
    soll_name TEXT,              -- manueller Soll-Name (folder_override), gilt vor dem echten Namen
    UNIQUE(disk_uuid, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_projects_cluster ON projects(cluster_id);
CREATE INDEX IF NOT EXISTS idx_projects_norm ON projects(norm_name);

CREATE TABLE IF NOT EXISTS scans_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    disk_uuid TEXT NOT NULL,
    scan_date TEXT,
    used_bytes INTEGER,
    valid_count INTEGER,
    unassigned_count INTEGER,
    node_count INTEGER
);
CREATE INDEX IF NOT EXISTS idx_scanslog_disk ON scans_log(disk_uuid, scan_date);

-- Phase A: vom Menschen angelegte Projekte + Ingest-Sessions (Live-DIT-Board).
-- Von Scannern via /ingest/project bzw. /ingest/session gepusht (Bearer-Auth).
CREATE TABLE IF NOT EXISTS project_meta (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    date TEXT,
    client TEXT,
    ff_notion_url TEXT,
    created_by TEXT,
    created_at TEXT,
    status TEXT DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS ingest_session (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    card_label TEXT,
    card_uuid TEXT,
    source_bytes INTEGER,
    source_files INTEGER,
    target_a_disk TEXT,
    target_b_disk TEXT,
    copies_done INTEGER DEFAULT 0,
    verify_status TEXT DEFAULT 'pending',
    state TEXT DEFAULT 'detected',
    started_at TEXT,
    finished_at TEXT,
    host TEXT
);
CREATE INDEX IF NOT EXISTS idx_session_project ON ingest_session(project_id);

-- Notion-Spiegel (Worker-Cron): welcher Scan je Platte zuletzt nach Notion ging
CREATE TABLE IF NOT EXISTS notion_sync_state (
    disk_uuid TEXT PRIMARY KEY,
    synced_scan TEXT,
    hdd_page_id TEXT,
    synced_at TEXT,
    last_error TEXT
);

-- ── Dashboard (migrations/0003_dashboard.sql) ──

-- Datenträger: Speicherstufe, Standort, wer sie hat (überschreibt last_user), Notiz
CREATE TABLE IF NOT EXISTS disk_meta (
    disk_uuid TEXT PRIMARY KEY,
    tier TEXT,                   -- SSD | HDD | NAS | CLOUD
    tier_source TEXT,            -- manual | notion | guess
    holder TEXT,
    location TEXT,
    note TEXT,
    separate_disk INTEGER,       -- 1 = trotz gleichem Namen eigene physische Platte (zählt als Kopie)
    enclosure TEXT,              -- Gehäuse: mehrere Platten an einem Anschluss
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


-- ── Platten-Check (migrations/0004_disk_health.sql) ──
CREATE TABLE IF NOT EXISTS disk_health (
    disk_uuid TEXT PRIMARY KEY,
    checked_at TEXT,
    host TEXT,
    verdict TEXT,              -- ok | warnung | defekt | unbekannt
    smart_status TEXT,
    smart_reason TEXT,
    model TEXT,
    wear_pct INTEGER,
    spare_pct INTEGER,
    media_errors INTEGER,
    power_on_hours INTEGER,
    temperature INTEGER,
    fs_status TEXT,
    fs_reason TEXT,
    read_status TEXT,
    read_errors INTEGER,
    read_reason TEXT
);

-- ── Fortschritt (migrations/0006_progress.sql) ──
CREATE TABLE IF NOT EXISTS job_progress (
    job_id TEXT PRIMARY KEY,
    host TEXT, kind TEXT, disk_uuid TEXT, disk_name TEXT,
    started_at TEXT, updated_at TEXT,
    done_files INTEGER, done_bytes INTEGER, total_files INTEGER, total_bytes INTEGER,
    state TEXT, message TEXT
);
