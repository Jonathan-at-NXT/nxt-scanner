-- Platten-Check: letzter Zustand je Datenträger (vom Scanner gemeldet, nur lesend ermittelt)
CREATE TABLE IF NOT EXISTS disk_health (
    disk_uuid TEXT PRIMARY KEY,
    checked_at TEXT,
    host TEXT,
    verdict TEXT,              -- ok | warnung | defekt | unbekannt
    smart_status TEXT,
    smart_reason TEXT,
    model TEXT,
    wear_pct INTEGER,
    spare_pct INTEGER,
    media_errors INTEGER,
    power_on_hours INTEGER,
    temperature INTEGER,
    fs_status TEXT,
    fs_reason TEXT,
    read_status TEXT,
    read_errors INTEGER,
    read_reason TEXT
);
