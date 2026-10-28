-- NAS-Löschliste: Inhalt des Parkordners "_ZU LOESCHEN (geprüft doppelt)" mit den Gegenkopien je Datei.
-- Wird vom Sicherungs-Mac komplett ersetzt (loeschliste/d1_laden.py), nur zum Nachprüfen vor dem Löschen.
CREATE TABLE IF NOT EXISTS nas_loesch_proj (id TEXT PRIMARY KEY, projekt TEXT, dateien INTEGER, gb REAL, min_geraete INTEGER, geraete TEXT);
CREATE TABLE IF NOT EXISTS nas_loesch_file (proj TEXT NOT NULL, path TEXT NOT NULL, gb REAL, hash TEXT, kopien TEXT, PRIMARY KEY (proj, path)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS nas_loesch_meta (k TEXT PRIMARY KEY, v TEXT);
