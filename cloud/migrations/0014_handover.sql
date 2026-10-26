-- Übergabe an Clemens/Paul: deren eigene Entscheidung je Altprojekt (aussortieren | einfach | behalten | ansehen) + Nachricht
CREATE TABLE IF NOT EXISTS handover_decision (
    key TEXT PRIMARY KEY,          -- gleicher Schlüssel wie legacy_decision
    person TEXT NOT NULL,          -- clemens | paul
    choice TEXT,
    message TEXT,
    updated_by TEXT,
    updated_at TEXT
);
