-- Rollout: welche Scanner-Version hat jede Person zuletzt benutzt (aus dem Scan-Upload abgeleitet)
CREATE TABLE IF NOT EXISTS scanner_seen (
    user TEXT PRIMARY KEY,
    at TEXT NOT NULL,
    generation TEXT NOT NULL,    -- neu (sendet fs_used_bytes / client_version) | alt
    client_version TEXT,
    disk_name TEXT
);
