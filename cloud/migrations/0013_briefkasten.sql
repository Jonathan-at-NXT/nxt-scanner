-- Upload-Briefkasten: Links zum Hochladen (ohne Konto) → R2 (EU) → Abholer schreibt aufs NAS und löscht erst danach.
CREATE TABLE IF NOT EXISTS upload_link (
    token TEXT PRIMARY KEY,
    name TEXT NOT NULL,           -- Anzeigename, zugleich Ordner unter _EINGANG
    created_by TEXT, created_at TEXT, expires_at TEXT,
    max_bytes INTEGER,            -- Obergrenze für alle Dateien dieses Links
    note TEXT,
    active INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS upload_file (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    size INTEGER NOT NULL,
    last_modified INTEGER,
    r2_key TEXT, upload_id TEXT, part_size INTEGER,
    state TEXT DEFAULT 'laeuft',  -- laeuft | hochgeladen | abgeholt | fehler | abgebrochen
    created_at TEXT, uploaded_at TEXT, pulled_at TEXT, nas_path TEXT, error TEXT
);
CREATE INDEX IF NOT EXISTS idx_upload_file_token ON upload_file(token, rel_path);
CREATE INDEX IF NOT EXISTS idx_upload_file_state ON upload_file(state);
-- Teile mit Prüfsumme aus dem Browser (SHA-256), damit der Abholer Ende-zu-Ende prüfen kann
CREATE TABLE IF NOT EXISTS upload_part (
    file_id INTEGER NOT NULL, n INTEGER NOT NULL, etag TEXT, sha256 TEXT, size INTEGER,
    PRIMARY KEY (file_id, n)
);
