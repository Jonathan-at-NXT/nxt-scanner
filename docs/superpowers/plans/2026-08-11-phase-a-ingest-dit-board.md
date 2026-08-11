# Phase A — Ingest + Live-DIT-Board Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** SD-Karte anstecken → in ein zuvor angelegtes Projekt doppelt kopieren, verifizieren, flach in NXT-Konvention sortieren, und ein live-aktualisiertes DIT-Board zeigt pro Karte den 2×-Sicherungsstatus.

**Architecture:** Neues Paket `storage_scanner/ingest/` mit isolierten, einzeln testbaren Modulen (camera_map → organize → verify → copier → store → orchestrator → device_monitor). Menubar bindet UI ein. Worker bekommt `/board` + `/board.json` (VIEW_TOKEN-gated) und Ingest-Routen für `project_meta`/`ingest_session`. Reine Logik (camera_map/organize/verify) ist mit stdlib + tmp_path unit-testbar; fs/UI/Hardware pragmatisch.

**Tech Stack:** Python 3 (stdlib only: `hashlib`, `sqlite3`, `shutil`, `pathlib`, `subprocess`), `rumps` (Menubar), `httpx` (D1-Push, vorhanden), Cloudflare Worker (JS) + D1.

## Global Constraints

- **Kein Key im Download/Repo.** `INGEST_TOKEN`/`VIEW_TOKEN`/Notion-Keys nie ins PyInstaller-Bundle oder GitHub. Tokens zur Laufzeit aus lokaler Config (`CONFIG_PATH = ~/Library/Application Support/NXT Scanner/.notion_config.json`) bzw. CF-Secret.
- **Alle sensiblen Endpunkte token-gated** (Muster `/dashboard.json`: `url.searchParams.get("key") !== env.VIEW_TOKEN` → 401).
- **Keine neuen PyInstaller-Dependencies.** Fingerprint via stdlib `hashlib.blake2b`, kein `xxhash`.
- **Datenverlust-Invariante:** Nie überschreiben; erst 2 verifizierte Kopien, dann darf formatiert werden (Board erst dann grün). Beim Flatten Kollisionen eindeutig suffixen, nie mergen.
- **UI-Sprache Deutsch** (konsistent mit bestehender Menubar: „D1-Sync einrichten…", „Angemeldet als:"). **Keine Emojis** in UI-Text (typografische Zeichen ok).
- **NXT-Naming-Konvention** aus `storage_scanner/rules.py`: `YYYY-MM-DD_Projekt_TYP`, TYP ∈ {FOOTAGE, PHOTOS, BTS, WORKING, …}.
- Test-Runner in dieser Umgebung: `<scratchpad>/venv/bin/python -m pytest` (pytest nicht im System-Python).

---

## File Structure

- `storage_scanner/ingest/__init__.py` — Paket-Init.
- `storage_scanner/ingest/camera_map.py` — Kartenstruktur-Map, Hersteller-Detection, Media-Discovery, „CAMERA INITIALEN"-Ableitung.
- `storage_scanner/ingest/organize.py` — Zielpfad-Bau (`TYP/DATE/CAMERA`), kollisions-sichere Flatten-Zuordnung.
- `storage_scanner/ingest/verify.py` — stdlib-Fingerprint + Kopie-Verifikation.
- `storage_scanner/ingest/copier.py` — chunked Copy Quelle→2 Ziele, resumable, Progress-Callback.
- `storage_scanner/ingest/store.py` — `project_meta`/`ingest_session` (SQLite lokal) + Payload-Bau für D1.
- `storage_scanner/ingest/d1_projects.py` — Push von Projekt/Session an Worker (`push_project`, `push_session`).
- `storage_scanner/ingest/device_monitor.py` — gemountete Wechselmedien erkennen/klassifizieren (Karte vs. Ziel-Platte).
- `storage_scanner/ingest/orchestrator.py` — State-Machine `run_ingest(...)`: detect→copy→verify→organize→done, aktualisiert Session.
- `storage_scanner/db.py` — SCHEMA um `project_meta`/`ingest_session` erweitern (additiv).
- `storage_scanner/menubar.py` — UI: Neues Projekt / Projekt wählen / Karte einlesen / Fortschritt.
- `cloud/schema.sql` + `cloud/src/index.js` — Tabellen + `/board`, `/board.json`, `/ingest/project`, `/ingest/session`.
- `nxt_scanner.spec` — hiddenimports der neuen Module.
- `tests/test_ingest_*.py` — Unit-Tests je Modul.

---

## Task 1: Camera-Map (Struktur, Detection, Media-Discovery, Namensableitung)

**Files:** Create `storage_scanner/ingest/__init__.py`, `storage_scanner/ingest/camera_map.py`; Test `tests/test_ingest_camera_map.py`.

**Interfaces — Produces:**
- `VIDEO_EXTS: set[str]`, `PHOTO_EXTS: set[str]` (lowercase, mit Punkt, z. B. `.mp4`, `.jpg`).
- `detect_vendor(root: Path) -> str | None` — erkennt Hersteller aus vorhandenen Root-Ordnern (`PRIVATE/M4ROOT`→"Sony", `DCIM/100GOPRO`→"GoPro", `DCIM/DJI_*`→"DJI", sonst None).
- `discover_media(root: Path) -> dict` — `{"video": [Path...], "photo": [Path...]}`, rekursiv über die ganze Karte, klassifiziert nach Extension (nicht nach Ordner — deckt tiefe/unbekannte Strukturen ab).
- `DEFAULT_PREFIXES: tuple[str,...]` — Kamera-Defaults (`C`, `MVI_`, `DSC`, `DJI_`, `GX`, `GH`, `GOPR`, `IMG_`, `A`, `CLIP`).
- `infer_camera_name(files: list[Path]) -> str | None` — gemeinsames Präfix der Dateinamen (bis zum ersten Ziffernblock); gibt Namen nur zurück, wenn er KEIN reines Default ist, sonst None (→ UI fragt nach).

- [ ] **Step 1:** Test schreiben — `discover_media` findet ein `.mp4` tief unter `PRIVATE/M4ROOT/CLIP/` als video und ein `.jpg` unter `DCIM/100MSDCF/` als photo; `detect_vendor` liefert "Sony".
- [ ] **Step 2:** Test läuft, schlägt fehl (Modul fehlt).
- [ ] **Step 3:** Minimal implementieren (EXT-Sets, os.walk, Root-Signaturen).
- [ ] **Step 4:** Test grün.
- [ ] **Step 5:** Test `infer_camera_name(["FX3 JO00001.MP4","FX3 JO00002.MP4"]) == "FX3 JO"` und `infer_camera_name(["C0001.MP4","C0002.MP4"]) is None` (Default). Implementieren, grün.
- [ ] **Step 6:** Commit `feat(ingest): camera-map — vendor detect, media discovery, name inference`.

## Task 2: Organize — Zielpfad + kollisions-sicheres Flatten

**Files:** Create `storage_scanner/ingest/organize.py`; Test `tests/test_ingest_organize.py`.

**Interfaces — Consumes:** camera_map media-Listen. **Produces:**
- `target_dir(dest_root: Path, project_slug: str, date: str, media_type: str, camera: str) -> Path` — baut `dest_root / f"{date}_{project_slug}_{TYP}" / date / camera`, TYP = FOOTAGE (video) / PHOTOS (photo).
- `unique_name(dest_dir: Path, filename: str, src: Path, seen: dict) -> str` — kollisions-sicher: existiert Ziel mit gleichem Namen und **anderem** Fingerprint → suffixen (`C0001.MP4`→`C0001__2.MP4`); gleicher Fingerprint → gleicher Name (Dedup-Signal).
- `plan_flatten(media: list[Path], dest_dir: Path) -> list[tuple[Path,Path]]` — Liste (src, final_dst) ohne zu kopieren; nutzt `unique_name`.

- [ ] **Step 1:** Test — zwei Quelldateien `100MSDCF/C0001.MP4` und `101MSDCF/C0001.MP4` mit **unterschiedlichem** Inhalt → `plan_flatten` erzeugt Ziele `C0001.MP4` und `C0001__2.MP4` (kein Overwrite).
- [ ] **Step 2:** Fail.
- [ ] **Step 3:** Implementieren (Fingerprint via `verify.fingerprint` — Abhängigkeit erst Task 3; für Test zunächst size+name-basiert, dann in Task 3 auf Fingerprint umstellen). *Reihenfolge:* Task 3 vor 2 mergen, falls Fingerprint gebraucht — hier zunächst Größe+Name als Kollisionskriterium, in Task 3-Step-6 auf `verify.fingerprint` heben.
- [ ] **Step 4:** Grün. **Step 5:** `target_dir`-Test (Pfad-String exakt). **Step 6:** Commit `feat(ingest): organize — TYP/DATE/CAMERA target + collision-safe flatten`.

## Task 3: Verify — Fingerprint + Kopie-Prüfung

**Files:** Create `storage_scanner/ingest/verify.py`; Test `tests/test_ingest_verify.py`.

**Interfaces — Produces:**
- `fingerprint(path: Path, sample_bytes: int = 4*1024*1024) -> str` — `hashlib.blake2b` über Größe + head/mid/tail-Sample (schnell, für Board ausreichend; volle Hashes = Phase B).
- `verify_copy(src: Path, dst: Path) -> bool` — Größe gleich UND `fingerprint(src)==fingerprint(dst)`.

- [ ] **Step 1:** Test — identische Bytes → `verify_copy` True; ein geändertes Byte in der Mitte → False.
- [ ] **Step 2:** Fail. **Step 3:** Implementieren. **Step 4:** Grün.
- [ ] **Step 5:** organize (Task 2) auf `verify.fingerprint` als Kollisionskriterium umstellen; Task-2-Tests weiter grün.
- [ ] **Step 6:** Commit `feat(ingest): verify — blake2b sampled fingerprint + copy check`.

## Task 4: Copier — chunked Copy zu 2 Zielen, resumable

**Files:** Create `storage_scanner/ingest/copier.py`; Test `tests/test_ingest_copier.py`.

**Interfaces — Consumes:** verify. **Produces:**
- `copy_file(src: Path, dst: Path, chunk: int = 8*1024*1024, progress=None) -> None` — legt Parent an, kopiert chunked in `dst.tmp`, dann atomarer `rename`; existiert `dst` bereits verifiziert → skip (resumable).
- `copy_to_targets(pairs_a: list[tuple[Path,Path]], pairs_b: list[tuple[Path,Path]], progress=None) -> dict` — kopiert dieselben Quellen in Ziel A und B, gibt `{"copied": n, "verified_a": bool, "verified_b": bool}`.

- [ ] **Step 1:** Test — `copy_file` erzeugt bytegleiche Kopie; erneuter Aufruf bei vorhandener valider Kopie kopiert nicht neu (mtime unverändert). **Step 2:** Fail. **Step 3:** Implementieren (tmp+rename, skip-if-verified). **Step 4:** Grün.
- [ ] **Step 5:** Test `copy_to_targets` schreibt in zwei Zielbäume, beide verifiziert True. **Step 6:** Commit `feat(ingest): copier — chunked dual-target copy, resumable`.

## Task 5: Store — project_meta + ingest_session (SQLite)

**Files:** Modify `storage_scanner/db.py` (SCHEMA additiv); Create `storage_scanner/ingest/store.py`; Test `tests/test_ingest_store.py`.

**Interfaces — Produces (store.py):**
- `create_project(name, date, client=None, ff_notion_url=None, created_by=None, conn=None) -> str` (id = uuid4 hex).
- `list_projects(status="active", conn=None) -> list[dict]`.
- `create_session(project_id, card_label, source_bytes, source_files, host=None, conn=None) -> str`.
- `update_session(session_id, conn=None, **fields) -> None` (state, copies_done, verify_status, target_a_disk, target_b_disk, finished_at).
- `board_rows(conn=None) -> list[dict]` — je Session Projektname + Kartenstatus (ungesichert/1×/2×verifiziert/Fehler) abgeleitet aus copies_done+verify_status.
- `project_payload(project_id, conn)` / `session_payload(session_id, conn)` — dicts für D1-Push.

DB-Schema (additiv zu `db.SCHEMA`): `project_meta(id PK, name, date, client, ff_notion_url, created_by, created_at, status DEFAULT 'active')`, `ingest_session(id PK, project_id, card_label, card_uuid, source_bytes, source_files, target_a_disk, target_b_disk, copies_done, verify_status, state, started_at, finished_at, host)`.

- [ ] **Step 1:** Test — `create_project` + `list_projects` roundtrip; FF-URL persistiert. **Step 2:** Fail. **Step 3:** SCHEMA erweitern + implementieren. **Step 4:** Grün.
- [ ] **Step 5:** Test `board_rows`: Session mit copies_done=2, verify_status='verified' → status `"2x_verified"`; copies_done=1 → `"1x"`; 0 → `"unsecured"`. **Step 6:** Commit `feat(ingest): store — project_meta/ingest_session + board rows`.

## Task 6: Orchestrator — Ingest-State-Machine

**Files:** Create `storage_scanner/ingest/orchestrator.py`; Test `tests/test_ingest_orchestrator.py`.

**Interfaces — Consumes:** camera_map, organize, verify, copier, store. **Produces:**
- `run_ingest(project, card_root: Path, camera: str, date: str, target_a: Path, target_b: Path, conn, host=None, progress=None) -> dict` — discover_media → plan_flatten je TYP für A und B → copy_to_targets → verify → `organize` finalisiert Ziel → `update_session` durch die States `detected→copying→verifying→done` (oder `error`/`mismatch`). Gibt Session-Endzustand.

- [ ] **Step 1:** Integrationstest mit tmp-„Karte" (fake M4ROOT/CLIP + DCIM), 2 tmp-Zielen → nach Lauf: Dateien flach in `…_FOOTAGE/DATE/CAMERA` und `…_PHOTOS/DATE/CAMERA` in **beiden** Zielen; Session `state='done'`, `verify_status='verified'`, `copies_done=2`. **Step 2:** Fail. **Step 3:** Implementieren. **Step 4:** Grün.
- [ ] **Step 5:** Fehlerpfad-Test: Ziel B nicht beschreibbar → `state='error'`, Board zeigt nicht grün, Quelle unberührt. **Step 6:** Commit `feat(ingest): orchestrator — safe ingest state machine`.

## Task 7: Device-Monitor — Karten erkennen/klassifizieren

**Files:** Create `storage_scanner/ingest/device_monitor.py`; Test `tests/test_ingest_device_monitor.py`.

**Interfaces — Produces:**
- `list_volumes() -> list[dict]` — gemountete `/Volumes/*` mit name, path, is_removable (via `diskutil info -plist` bzw. Fallback), total_bytes.
- `classify(vol: dict) -> str` — `"card"` (klein/removable + Kamera-Signatur via camera_map.detect_vendor) vs `"target"` (große Ziel-Platte) vs `"other"`.

- [ ] **Step 1:** Test — `classify` mit gemocktem removable Volume, dessen Pfad Kamera-Signatur hat → `"card"`; großes internes → `"target"`. (diskutil gemockt.) **Step 2:** Fail. **Step 3:** Implementieren. **Step 4:** Grün. **Step 5:** Commit `feat(ingest): device monitor — classify card vs target`.

## Task 8: Worker — Board + Ingest-Routen

**Files:** Modify `cloud/schema.sql` (Tabellen), `cloud/src/index.js` (Routen); Test: manueller `wrangler dev` + curl (im Task dokumentiert).

**Interfaces:** `POST /ingest/project` + `POST /ingest/session` (Bearer INGEST_TOKEN, upsert). `GET /board.json?key=VIEW_TOKEN` → aktive Projekte + Sessions + Kartenstatus. `GET /board` → `BOARD_HTML` (Auto-Refresh, token via `?key=`), Muster `buildDashboard`/`DASHBOARD_HTML`.

- [ ] **Step 1:** `schema.sql` um `project_meta`/`ingest_session` erweitern; `wrangler d1 execute --local` anlegen.
- [ ] **Step 2:** `/ingest/project` + `/ingest/session` implementieren (Bearer-Check wie `/ingest`, `INSERT ... ON CONFLICT DO UPDATE`).
- [ ] **Step 3:** `buildBoard(env)` + `BOARD_HTML` (VIEW_TOKEN-gate exakt wie `/dashboard.json`).
- [ ] **Step 4:** `wrangler dev` + curl: project+session posten, `/board.json?key=…` zeigt Kartenstatus; ohne key → 401.
- [ ] **Step 5:** Deploy `wrangler deploy`, VIEW_TOKEN/INGEST_TOKEN als Secrets prüfen. **Step 6:** Commit `feat(cloud): live DIT board + project/session ingest`.

## Task 9: D1-Push für Projekt/Session

**Files:** Create `storage_scanner/ingest/d1_projects.py`; Test `tests/test_ingest_d1_projects.py`.

**Interfaces — Consumes:** store payloads, `notion_sync.load_config`. **Produces:** `push_project(payload) -> bool`, `push_session(payload) -> bool` (best-effort, Muster `d1_client.push`, `/ingest/project` bzw. `/ingest/session`, Bearer aus Config `d1_token`). `push_safe`-Wrapper schluckt Fehler.

- [ ] **Step 1:** Test mit gemocktem httpx (monkeypatch) → korrekter Endpoint + Bearer; kein Token → False ohne Request. **Step 2:** Fail. **Step 3:** Implementieren. **Step 4:** Grün. **Step 5:** Commit `feat(ingest): d1 push for project/session`.

## Task 10: Menubar-UI + Verdrahtung

**Files:** Modify `storage_scanner/menubar.py`, `nxt_scanner.spec` (hiddenimports); manuelle Verifikation (rumps, kein Unit-Test).

**Interfaces — Consumes:** store, orchestrator, device_monitor, d1_projects.

- [ ] **Step 1:** Menüpunkte: „Neues Projekt…" (rumps.Window: Name; danach Kunde optional; FF-Notion-Link optional; Datum default heute) → `store.create_project` + `d1_projects.push_project`.
- [ ] **Step 2:** „Aktives Projekt: …" Untermenü aus `store.list_projects`.
- [ ] **Step 3:** „Karte einlesen…": Kandidaten aus `device_monitor`; Kamera-Name aus `camera_map.infer_camera_name`, sonst `rumps.Window` „Kamera + Initialen?" (Vorschlag aus `detect_vendor`); Ziel A/B wählen; `orchestrator.run_ingest` in Thread, Progress in Titel; nach Abschluss `push_session`.
- [ ] **Step 4:** hiddenimports in `nxt_scanner.spec` ergänzen (alle `storage_scanner.ingest.*`).
- [ ] **Step 5:** Manueller Rauchtest (Dev-Run) mit tmp-Ordnern als „Karte"/Ziele; Board zeigt Session. **Step 6:** Commit `feat(menubar): project creation + card ingest + progress`.

---

## Self-Review

- **Spec coverage:** project_meta/ingest_session (T5,T8) ✓; Projekt-zuerst-Flow (T10) ✓; Kunde/FF-Link (T5,T10) ✓; Foto/Video-Trennung + Flatten TYP/DATE/CAMERA (T2,T6) ✓; Kamera-Map + tiefe Roots + Namensableitung/Abfrage (T1,T10) ✓; Kollisions-Sicherheit (T2) ✓; 2×-Copy+Verify vor „grün" (T4,T6) ✓; Board token-gated (T8) ✓; kein Key im Bundle (Global Constraints, T9 Config) ✓; Device-Monitor (T7) ✓.
- **Reihenfolge-Hinweis:** Task 2 nutzt `verify.fingerprint` (Task 3) — deshalb Task-2-Kollisionskriterium zunächst size+name, in Task 3-Step-5 auf Fingerprint gehoben. Alternativ Task 3 vor Task 2 ausführen.
- **No new deps:** blake2b (stdlib) statt xxhash ✓.
