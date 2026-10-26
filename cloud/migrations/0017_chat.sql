-- Chat mit dem NXT Data Agent (Sicherungs-Mac): Nachrichten aus dem Dashboard, Antworten schreibt der Agent
CREATE TABLE IF NOT EXISTS chat_message (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    author TEXT NOT NULL,
    role TEXT NOT NULL,          -- user | agent
    text TEXT NOT NULL
);
