-- Altprojekte (Nils, Ordner "113-…"): Entscheidung je Projekt (aussortieren | behalten | ansehen)
CREATE TABLE IF NOT EXISTS legacy_decision (
    key TEXT PRIMARY KEY,          -- "<Nummer>|<Name normalisiert>"
    decision TEXT,
    note TEXT,
    updated_by TEXT,
    updated_at TEXT
);
