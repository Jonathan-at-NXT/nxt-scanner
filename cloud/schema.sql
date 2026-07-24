-- NXT Scanner — zentrales D1-Schema
-- Rohdaten (von Scannern gepusht): disks + folder_tree
-- Abgeleitet (derive.py): projects + scans_log

CREATE TABLE IF NOT EXISTS disks (
    uuid TEXT PRIMARY KEY,
    name TEXT,
    fs_type TEXT,
    is_network INTEGER,
    used_bytes INTEGER,
    capacity_bytes INTEGER,
    last_scan TEXT,
    last_user TEXT
);

CREATE TABLE IF NOT EXISTS folder_tree (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    disk_uuid TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    depth INTEGER,
    parent_rel_path TEXT,
    size_bytes INTEGER,
    file_count INTEGER,
    mtime TEXT,
    UNIQUE(disk_uuid, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_tree_disk_depth ON folder_tree(disk_uuid, depth);

-- Zentral abgeleitet (nicht von Scannern beschrieben)
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    disk_uuid TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    name TEXT,
    date TEXT,
    project_name TEXT,
    norm_name TEXT,
    type TEXT,
    status TEXT,                 -- validated | unassigned | container
    size_bytes INTEGER,
    file_count INTEGER,
    mtime TEXT,
    cluster_id TEXT,
    UNIQUE(disk_uuid, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_projects_cluster ON projects(cluster_id);
CREATE INDEX IF NOT EXISTS idx_projects_norm ON projects(norm_name);

CREATE TABLE IF NOT EXISTS scans_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    disk_uuid TEXT NOT NULL,
    scan_date TEXT,
    used_bytes INTEGER,
    valid_count INTEGER,
    unassigned_count INTEGER,
    node_count INTEGER
);
CREATE INDEX IF NOT EXISTS idx_scanslog_disk ON scans_log(disk_uuid, scan_date);
