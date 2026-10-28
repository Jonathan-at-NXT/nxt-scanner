-- Geparkte NAS-Bytes je NAS-Ordner (Bereich + relativer Pfad, Ebene 1–4): das Dashboard zieht sie von den
-- NAS-Kopien ab, bis der nächste NAS-Scan sie ohnehin nicht mehr sieht (Parkordner liegt außerhalb der Bereiche).
CREATE TABLE IF NOT EXISTS nas_loesch_folder (disk_uuid TEXT NOT NULL, rel_path TEXT NOT NULL, gb REAL, PRIMARY KEY (disk_uuid, rel_path)) WITHOUT ROWID;
