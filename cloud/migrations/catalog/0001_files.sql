-- nxt-catalog (eigene D1, EU-Jurisdiktion): jede Datei jedes Datenträgers (nur Metadaten, kein Inhalt)
CREATE TABLE IF NOT EXISTS files (
    disk_uuid TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    name TEXT,
    size INTEGER,
    mtime REAL,
    kind TEXT,
    PRIMARY KEY (disk_uuid, rel_path)
) WITHOUT ROWID;
-- Inhalts-Abgleich über Platten: gleicher Dateiname + gleiche Bytezahl
CREATE INDEX IF NOT EXISTS idx_files_name_size ON files(name, size);
-- Stand je Datenträger (für Anzeige und Kontrolle)
CREATE TABLE IF NOT EXISTS disk_sync (
    disk_uuid TEXT PRIMARY KEY,
    files INTEGER,
    bytes INTEGER,
    synced_at TEXT,
    host TEXT
);
