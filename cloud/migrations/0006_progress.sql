-- Fortschritt laufender Arbeiten (Scan, Inventur, Hash, Kopie) – nur Anzeige
CREATE TABLE IF NOT EXISTS job_progress (
    job_id TEXT PRIMARY KEY,
    host TEXT, kind TEXT, disk_uuid TEXT, disk_name TEXT,
    started_at TEXT, updated_at TEXT,
    done_files INTEGER, done_bytes INTEGER, total_files INTEGER, total_bytes INTEGER,
    state TEXT, message TEXT
);
