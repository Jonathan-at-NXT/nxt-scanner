-- Rückfragen des Assistenten an Jonathan (im Dashboard beantwortbar, auch unterwegs)
CREATE TABLE IF NOT EXISTS assistant_question (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT,
    host TEXT,
    topic TEXT,                 -- z. B. "KTM zusammenführen"
    question TEXT NOT NULL,
    details TEXT,
    options TEXT,               -- JSON-Liste von Antwortmöglichkeiten
    status TEXT DEFAULT 'offen',   -- offen | beantwortet | zurueckgezogen
    answer TEXT,
    answer_note TEXT,
    answered_by TEXT,
    answered_at TEXT
);
