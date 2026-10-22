-- Notion-Sync im Worker: echter Pfad je Ordner, echte Plattenbelegung, Sync-Stand je Platte.
-- Additiv; alte Scanner (ohne abs_path/fs_used_bytes) funktionieren weiter.
ALTER TABLE folder_tree ADD COLUMN abs_path TEXT;
ALTER TABLE disks ADD COLUMN fs_used_bytes INTEGER;
CREATE TABLE IF NOT EXISTS notion_sync_state (
    disk_uuid TEXT PRIMARY KEY,
    synced_scan TEXT,
    hdd_page_id TEXT,
    synced_at TEXT,
    last_error TEXT
);
