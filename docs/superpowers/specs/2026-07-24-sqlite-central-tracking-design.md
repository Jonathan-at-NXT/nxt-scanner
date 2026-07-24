# SQLite + zentrales Tiefen-Tracking für NXT Scanner

**Datum:** 2026-07-24
**Status:** Design freigegeben, bereit für Implementierungsplan

## Ziel

Zusätzlich zum bestehenden Notion-Upload soll jeder Scanner die Ordnerstruktur
seiner Datenträger in eine SQLite-Datenbank schreiben und an eine zentrale
Cloudflare-D1-Datenbank pushen. Damit entsteht eine maschinenlesbare
Datengrundlage über alle Systeme hinweg, die schnelle Auswertungen aus der Ferne
erlaubt (z. B. Einsparpotenzial-/Redundanz-Analysen wie das bestehende
Dashboard) — ohne Notions Größen-/Performance-Grenzen.

Zusätzlich wird die Erfassungstiefe erhöht: statt nur Top-Level-Größen werden
**alle** Top-Level-Ordner (auch die bisher als „unassigned"/Container
ignorierten) inklusive **2 weiterer Unterebenen** als Ordnerbaum erfasst.

## Nicht-Ziele

- **Notion bleibt vollständig unverändert.** Kein bestehender Code-Pfad (Scan,
  Report-JSON, `notion_sync`, Updater) wird ersetzt — alles Neue ist additiv.
- Keine Datei-Ebenen-Erfassung (nur Ordner-Knoten).
- Keine Ablösung der JSON-Reports als Source-of-Truth.
- Kein zweiter Plattendurchlauf pro Scan.

## Leitprinzipien

1. **Additiv & non-blocking:** Der neue Pfad darf den bestehenden Scan-/Notion-
   Ablauf niemals blockieren. DB-Ingest und D1-Push sind best-effort; bei Fehler
   wird geloggt und normal weitergemacht.
2. **Lokal-first:** Die lokale SQLite wird immer geschrieben, unabhängig vom
   Netz. Der D1-Push ist eine Spiegelung.
3. **Dünne Scanner, zentrale Intelligenz:** Scanner speichern nur Rohstruktur
   (`disks` + `folder_tree`). Klassifikation, Fuzzy-Clustering und Trend werden
   zentral abgeleitet.
4. **Ein Plattendurchlauf:** Der Ordnerbaum entsteht im selben Scan-Durchlauf wie
   heute (siehe „Erfassung").

## Architekturüberblick

```
Scanner (Mac)                          Cloudflare
─────────────                          ──────────
scan.run_scan ─► report.json (+ folder_tree-Abschnitt)
      │
      ├─ db.ingest(report) ─► lokale nxt_scanner.db  (disks + folder_tree)
      │
      └─ d1_client.push ──HTTPS POST /ingest──► Worker ──► D1
            {disk_row, folder_tree[]}          (Bearer)   disks + folder_tree

Zentrale Ableitung (Python, geplant/on-demand):
   derive.py ──/query──► D1 (folder_tree, disks)
        · klassifiziert Top-Level-Knoten (rules.validate_folder) → projects
        · reconcile() (difflib) → cluster_id
        · schreibt projects + scans_log ──/exec──► D1
```

## Komponenten

### 1. Erfassung (Änderung an `analyzer.py` / `scan.py`)

- Neue Funktion in `analyzer.py`, die pro Volume den Ordnerbaum bis zu einer
  konfigurierbaren Tiefe in **einem** Durchlauf erfasst — via `du -d <N>` (liefert
  Größen aller Knoten bis Tiefe N) plus ein gebündelter `find`-Pass für die
  Dateizahlen je Knoten (in Python nach Tiefe-N-Vorfahr aggregiert).
- **Tiefe:** Default 3 ab Volume-Root (= Top-Level + 2 Unterebenen). Die
  transparente Auflösung von `NXT STUDIOS`/`OLDER` bleibt erhalten.
- **Netz-/Langsam-Schutz:**
  - Erkennung von Netzlaufwerken über den Mount-Typ (`smbfs`/`nfs`/`afpfs`).
  - Konfigurierbare Tiefe pro Volume-Muster; für Netzlaufwerke automatisch
    flacher (Default 1 = wie heute) und höheres Timeout.
- **Performance-Begründung:** Der bestehende Scan traversiert den Baum bereits
  vollständig (`du -sk` + `find` rekursieren komplett). `du -d 3` läuft denselben
  Plattendurchlauf und gibt nur mehr Zwischenzeilen aus — kein zusätzliches
  IO/Seeks. Bei PROJECT-Ordnern ersetzt der eine `du -d`-Pass die heutigen
  Mehrfach-Scans (Eltern + je Kind) → netto neutral bis schneller.
- Der Baum wird als neuer Abschnitt `folder_tree` in die Report-JSON geschrieben.
  `notion_sync` liest diesen Key nicht → Notion unberührt.

Jeder `folder_tree`-Knoten:
`{rel_path, depth, parent_rel_path, size_bytes, file_count, mtime}`.

### 2. Lokale SQLite (`storage_scanner/db.py`, neu)

- DB-Datei: `~/Library/Application Support/NXT Scanner/nxt_scanner.db`
  (neuer Pfad-Eintrag in `paths.py`).
- **Nur zwei Tabellen auf dem Scanner:**

```sql
disks(
  uuid TEXT PRIMARY KEY, name TEXT, fs_type TEXT, is_network INT,
  used_bytes INT, capacity_bytes INT, last_scan TEXT, last_user TEXT)

folder_tree(
  id INTEGER PRIMARY KEY, disk_uuid TEXT, rel_path TEXT, depth INT,
  parent_rel_path TEXT, size_bytes INT, file_count INT, mtime TEXT,
  UNIQUE(disk_uuid, rel_path))
```

- `ingest_report(report) -> dict`: in einer Transaktion
  1. `disks` upserten (UUID aus `scan_info.volume_uuid`; `capacity_bytes`/
     `fs_type`/`is_network` via `diskutil`, `last_user` aus Config),
  2. `DELETE FROM folder_tree WHERE disk_uuid=?`, dann alle Knoten einfügen.
  - Gibt das Push-Payload zurück (`{disk_row, folder_tree[]}`) zur Weitergabe an
    den D1-Client.
- `rebuild_from_reports()`: liest alle `reports/*.json` und ingestet sie
  (Backfill / lokaler Neuaufbau; DB ist jederzeit aus Reports reproduzierbar).
- Indizes: `folder_tree(disk_uuid, depth)`.

### 3. D1-Client (`storage_scanner/d1_client.py`, neu)

- `push(payload, config) -> bool`: HTTPS-POST an `d1_ingest_url` mit
  `Authorization: Bearer <d1_token>`.
- Große Bäume werden gechunkt und/oder gzip-komprimiert.
- **Best-effort:** Timeouts/Fehler werden geloggt (`auto_scan.log`) und
  schlucken — niemals eine Exception nach außen, die den Scan/Notion stört.

### 4. Worker + D1 (Cloudflare, neues Projekt)

- Endpoint `POST /ingest`: prüft Bearer-Token, schreibt `disks` + `folder_tree`.
  Pro `disk_uuid` delete+insert in einer D1-Transaktion (idempotent).
- D1-Schema: `disks` + `folder_tree` wie oben, **plus** die zentral abgeleiteten
  Tabellen:

```sql
projects(
  id INTEGER PRIMARY KEY, disk_uuid TEXT, rel_path TEXT, name TEXT,
  date TEXT, project_name TEXT, norm_name TEXT, type TEXT,
  status TEXT,            -- validated | unassigned | container
  size_bytes INT, file_count INT, mtime TEXT, cluster_id TEXT,
  UNIQUE(disk_uuid, rel_path))

scans_log(
  id INTEGER PRIMARY KEY, disk_uuid TEXT, scan_date TEXT,
  used_bytes INT, valid_count INT, unassigned_count INT, node_count INT)
```

- Optionaler Lese-Endpoint (`/query` o. ä.) oder Nutzung der Cloudflare-D1-HTTP-
  API für Auswertungen. Details in der Implementierung.

### 5. Zentrale Ableitung (`derive.py`, neu — läuft zentral/on-demand)

- Zieht `folder_tree` + `disks` aus D1.
- Klassifiziert die Top-Level-Knoten (depth-1) mit der bestehenden
  `rules.validate_folder`-Logik → `status` (validated | unassigned | container),
  `date`, `project_name`, `norm_name`, `type`.
- `reconcile()`: die difflib-Union-Find-Logik aus der Analyse (Normalisierung +
  Fuzzy-Schwelle ≥ 0,86 bei gleichem Datum) setzt `cluster_id`.
- Schreibt `projects` + `scans_log` zurück nach D1.
- Bewusst in Python (nicht Worker-JS), damit `rules.py` und die Fuzzy-Logik in
  einer Sprache wiederverwendbar bleiben. Auslösung per Cron oder on-demand.

### 6. Konfiguration & Auth

- Zwei neue Config-Keys in `.notion_config.json` (Admin setzt sie über das
  bestehende Einstellungs-Menü-Muster in `menubar.py`):
  `d1_ingest_url`, `d1_token`.
- Scanner brauchen nur diese zwei Werte. Fehlen sie, wird lokal gespeichert, aber
  kein Push versucht (geloggt, kein Fehler).

### 7. Integration (Hooks)

- `auto_scan.scan_and_sync()` und `menubar._do_scan()`: nach dem bestehenden
  `run_sync(...)` zusätzlich `payload = db.ingest_report(report)` und
  `d1_client.push(payload, config)`. Beide in try/except gekapselt.

## Datenfluss (Zusammenfassung)

1. Scan erzeugt Report **inkl.** `folder_tree` (ein Plattendurchlauf).
2. `notion_sync.run_sync` läuft **zuerst und unverändert**.
3. `db.ingest_report` schreibt lokale SQLite (disks + folder_tree).
4. `d1_client.push` spiegelt nach D1 (best-effort).
5. Zentral: `derive.py` baut aus D1-Rohdaten `projects` + `scans_log`.
6. Auswertungen (Dashboard etc.) laufen gegen D1 / `projects` per
   `GROUP BY cluster_id`.

## Fehlerbehandlung

- Lokaler DB-Fehler: geloggt, Scan/Notion laufen weiter.
- D1-Push-Fehler (Netz/Auth/Timeout): geloggt, kein Abbruch. Der nächste
  erfolgreiche Scan pusht den aktuellen Stand ohnehin komplett (delete+insert).
- Fehlende D1-Config: Push wird übersprungen, lokal wird trotzdem geschrieben.
- Netzlaufwerk zu langsam für Tiefe: gedrosselte Tiefe/Timeout; im Zweifel nur
  Top-Level (wie heute).

## Rollout

1. Cloud zuerst: Worker + D1 aufsetzen und isoliert testen (Fake-Payload), bevor
   Scanner pushen.
2. Client-Logik offline gegen die echten 62 Reports verifizieren
   (`rebuild_from_reports()` + Konsistenz-Checks) — kein Release vorher.
3. Backfill: die 62 Reports einmalig einlesen (lokal) bzw. via Push nach D1
   bringen. Volle 2-Ebenen-Tiefe füllt sich beim nächsten regulären Scan je
   Platte auf.
4. Versionsbump + `release.sh` (arm64 + x86_64), DMG/ZIP, Auto-Updater verteilt.

## Tests

- `analyzer`-Tiefen-Walk: Unit-Test gegen ein Fixture-Verzeichnis (bekannte
  Struktur/Größen, Tiefen-Cap, Netz-Drosselung simuliert).
- `db.ingest_report`: Idempotenz (zweimal ingesten → gleiche Zeilen),
  delete+insert je disk, Payload-Form.
- `rebuild_from_reports`: läuft fehlerfrei über alle 62 echten Reports; erwartete
  Disk-/Knotenzahlen.
- `d1_client`: Best-effort-Verhalten (Fehler wird geschluckt, Rückgabewert), mit
  gemocktem HTTP.
- Worker `/ingest`: Auth-Prüfung, Idempotenz (delete+insert je disk_uuid).
- `derive`: Klassifikation + Fuzzy-Cluster gegen ein bekanntes Sample
  (reproduziert die Kennzahlen der bestehenden Analyse plausibel).

## Offene Detailpunkte für die Implementierung

- Genaues Chunking/Kompression des D1-Payloads (Größe pro Request).
- Konkreter Lese-Weg für Auswertungen (Worker-`/query` vs. D1-HTTP-API).
- Cron-Trigger für `derive.py` (Cloudflare Cron vs. on-demand).
