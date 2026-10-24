-- Vorschläge aus dem Inhalts-Abgleich (gleiche Dateien = Name + Größe, Datei-Katalog): Ordner X gehört zu Projekt Y.
-- Wird lokal berechnet (Sicherungs-Mac, ~/NXT-Speicher/inhalts-abgleich.py) und hier abgelegt.
CREATE TABLE IF NOT EXISTS content_suggestion (
    disk_uuid TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    soll_name TEXT NOT NULL,
    target_cluster TEXT,
    target_type TEXT,
    target_label TEXT,
    current_label TEXT,
    kind TEXT,                 -- unbenannt | gleich | teilmenge
    shared_gb REAL,
    frac REAL,                 -- Anteil des Ordners, der im Zielprojekt vorkommt
    target_frac REAL,          -- Anteil des Zielprojekts, der im Ordner vorkommt
    size_bytes INTEGER,        -- Ordnergröße laut folder_tree
    status TEXT DEFAULT 'offen',   -- offen | uebernommen | verworfen
    computed_at TEXT,
    decided_by TEXT,
    decided_at TEXT,
    PRIMARY KEY (disk_uuid, rel_path)
);
