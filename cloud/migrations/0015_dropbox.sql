-- Dropbox wird gekündigt: je Ordner, der nur in der Dropbox liegt, entscheiden (sichern / löschen / ansehen)
CREATE TABLE IF NOT EXISTS dropbox_folder (
    path TEXT PRIMARY KEY,
    gb REAL NOT NULL,
    files INTEGER NOT NULL,
    suggestion TEXT,             -- sichern | accounts | weg
    choice TEXT,                 -- sichern | loeschen | ansehen
    message TEXT,
    updated_by TEXT,
    updated_at TEXT
);
