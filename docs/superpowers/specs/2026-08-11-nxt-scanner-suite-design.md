# NXT Scanner → All-in-One Data-Management-Suite (CutValet intern absorbieren)

**Datum:** 2026-08-11
**Status:** Brainstorm/Design zur Review (kein Code bis Freigabe)
**Entscheidung des Users:** CutValet **nicht** team-weit installieren. Alle Features
**intern in NXT Scanner** abdecken — ein Tool, das das Team ohnehin schon hat.

## Ausgangslage / vorhandene Assets

NXT Scanner ist bereits: eine team-weit installierte macOS-App (Python/rumps, launchd),
mit **D1 + Cloudflare-Worker**-Backend, **Notion**-Integration, dem **bleibenden
`/dashboard`**, einer **Fleet-Registry** (`fleet_disk`), und der 3-Schicht-Governance-Spec
(Daten/Kontext/Entscheidung, 2×-Invarianten, Retention). Zusätzlich existiert ein
**eigener DaVinci-Resolve-MCP** + tiefe Resolve-Scripting-Expertise. Selber Stack wie
CutValet (Python/PyInstaller/PySide, ffmpeg, xxhash).

CutValet-Sezierung (installiert, `dvea`-Paket, 290 Module) ergab: Ingest (Karte→2 Drives
+ Checksum + Organize), **SHA-256 + xxHash64** je Datei, **Markdown-Sidecars** + **per-Drive
`catalog.db`** (drive-as-truth), Reconcile (new/moved/deleted per Hash), lokale KI
(whisper/torch/SigLIP: Transkript/Beschreibung/Tags/Embeddings/Near-Dup/Best-Frame),
Resolve-Integration, Distribution (YouTube/IG), Device-Monitor (watchdog).

## Ziel / Definition of Done

Eine SD-Karte anstecken → NXT Scanner kopiert doppelt gesichert, verifiziert, sortiert in
die **NXT-Konvention** (`YYYY-MM-DD_Projekt_TYP` mit `FOOTAGE/PHOTOS/BTS/WORKING`),
aktualisiert alle DBs, und zeigt **live**, welche Karte gesichert/ungesichert/1×/2× ist —
der „Macherfestival"-Überblick (8×/Tag × 5 Karten). Darauf sitzt die bestehende
Flotten-Governance (2×, Retention, Dashboard). Kein Fremd-Tool-Install.

## Feature-Coverage-Map (CutValet → NXT Scanner intern)

| CutValet-Fähigkeit | NXT-Scanner-Abdeckung | Aufwand | Anmerkung |
|---|---|---|---|
| Ingest: Karte→2 Drives + Verify + Organize | **NEU:** Ingest-Engine (Device-Monitor → chunked Copy zu 2 Zielen → Verify → Organize) | Hoch | = euer Notion-DIT-Workflow automatisiert |
| SHA-256 + xxHash64 je Datei | **= unsere Phase 1** (Content-Hashing) | Mittel | Vereint Ingest-Verify + Reconcile + Redundanz-Beweis |
| Markdown-Sidecars + per-Drive `catalog.db` | **Muster übernehmen:** `.md`-Sidecars + `.nxt-catalog.db` je Platte (drive-as-truth) | Mittel | **Löst Staleness/Identität** (JONNY-1, Name/UUID-Chaos, Klone) |
| Reconcile (new/moved/deleted per Hash) | **NEU:** Reconcile-Worker (Content-Identität) | Mittel | Löst Umwidmungs-/Staleness-Problem |
| Device-Monitor (watchdog) | **NEU/erweitern:** vorhandene launchd-Mount-Erkennung | Niedrig | |
| KI-Transkription (whisper) | **Optional:** lokales whisper | Hoch (Dep) | torch/whisper = großes Bundle |
| KI-Beschreibung/Tags/Embeddings (SigLIP) | **Optional:** lokale Embeddings → Content-Clustering | Hoch (Dep) | **Löst 44 % Unklassifiziert** semantisch |
| Near-Dup / Best-Frame | aus Embeddings ableitbar | Mittel | „Überschuss" content-basiert statt Größe (fixt BEAT-OF-GARAGE-Fehler) |
| DaVinci Resolve (AI-Cut etc.) | **vorhandenen Resolve-MCP nutzen** | — | eigener Track, Heimspiel |
| Distribution (YouTube/IG) | **out (initial)** | — | später |
| Flotten-2×, Retention, Notion, Dashboard, Manager-Daemon | **schon NXTs** (spezifiziert) | — | das einzigartige Rückgrat |

## Architektur (3-Schicht, jetzt Voll-Lebenszyklus)

- **Client (je Editor-Mac):** NXT-Scanner-App macht **Ingest + Hashing + lokaler Katalog
  + optional KI**, schreibt `.md`-Sidecars + per-Platte `.nxt-catalog.db`, pusht Digest an D1.
- **Backend:** Cloudflare Worker + D1 (zentrale Aggregation, `/dashboard`) — vorhanden.
- **Datenmodell:** per-Platte `catalog.db` (drive-as-truth, reist mit) + Sidecars als Source
  of Truth + zentraler D1-Digest. Notion = Business-Layer.
- **Governance/Entscheidung:** 2×-Policy, Retention, Manager-Daemon — bestehende Spec.

Jede Einheit klar abgegrenzt: Ingest-Engine, Hash-Utils, Katalog-Writer, Reconcile-Worker,
(optional) KI-Worker, Governance-Planer/Executor, Dashboard.

## Scope-Entscheidungen (offene Forks — mit Empfehlung)

1. **KI-Tiefe:** *Voll* (Transkript + Embeddings/Clustering) vs. *Lite* (nur Transkript +
   Basis-Tags) vs. *Erst ohne* (Ingest+Hash+Katalog zuerst, KI später).
   → **Empfehlung: Erst ohne** — KI in eigener späterer Phase, weil torch das Bundle
   vervielfacht (CutValet ist deshalb riesig).
2. **Lokal vs. Cloud-KI:** lokal (privat, keine Kosten/Nutzung, riesiges Bundle) vs. Cloud
   (klein, Kosten, Daten verlassen Haus) vs. hybrid. → **Empfehlung: hybrid** (lokal
   whisper.cpp o. ä. für Transkript; Claude-API optional/BYO für Beschreibungen).
3. **Edit-Seite (Resolve AI-Cut):** jetzt in der Suite vs. separater Track via vorhandenen
   MCP. → **Empfehlung: separater Track später** — Datenmanagement-Kern zuerst.
4. **Ingest bauen vs. CutValet-Ansatz:** → **Empfehlung: selbst bauen** — euer Stack, eure
   Konvention (CutValet organisiert nach `YYYY/MM/DD/Kamera`, ihr nach `YYYY-MM-DD_Projekt_TYP`).

## Phasen (Reihenfolge nach akutem Schmerz)

- **Phase A — Ingest + Live-DIT-Board (Macherfestival):** Device-Monitor → Copy-zu-2 +
  Verify → Organize in NXT-Konvention → **Live-„Sicherungs-Board"** (pro Karte 2×-Status,
  „formatieren ok?"). Höchster Sofortnutzen.
- **Phase B — Hashing + per-Drive-Katalog + Reconcile (Phase 1 vereint):** Content-Identität,
  behebt Staleness/Matching-Fehler; ersetzt das fragile Größen-Matching.
- **Phase C — Flotten-Governance + Retention (bereits spezifiziert):** 2×-Policy, Retention,
  Dashboard-Härtung (striktes Matching in `derive.py`).
- **Phase D — KI-Katalog (optional, gescoped):** Transkript + semantisches Clustering für die
  44 % Unklassifiziert.
- **Phase E — Edit/Publish (später):** vorhandenen Resolve-MCP nutzen; Distribution optional.

## YAGNI-Schnitte

- CutValets KI-**Editing** (Rough-Cut/Music/Graphics) **nicht** nachbauen — kein
  Datenmanagement; bei Bedarf der bestehende Resolve-MCP.
- **Distribution** initial raus.
- **Volle lokale KI** nicht zwingend für den Kern (Phase D, optional).

## Verweise

- 3-Schicht-Governance + 2×-Invarianten + Retention:
  `docs/superpowers/specs/2026-07-27-auto-storage-manager-design.md`
- Naming-Konvention: `storage_scanner/rules.py` (`YYYY-MM-DD_Projekt_TYP`)
- Euer DIT-Workflow (Notion): DOPPELTE SICHERUNG, 🔴-Finder-Tracking, 4 Phasen inkl.
  Datenhygiene (= Retention).
- CutValet-Sezierung (Referenz): `dvea`-Paket, SHA-256+xxHash64, Sidecar `YYYY/MM/DD/Kamera/`,
  per-Drive `.dvea-archive/catalog.db`, SigLIP-Embeddings.

## Phase A — Konkretisierung (Live-DIT + Projekt-Anlage)

**Entscheidungen (User, 2026-08-11):** Projekt-Entity **in D1** (Notion-Mirror erst Phase C).
**Projekt-zuerst-Flow**. Felder: `name`* · `date` (auto, editierbar) · `client` (optional) ·
`ff_notion_url` (optional Link = FF-Marker; leer = kein FF) · `created_by` (auto Mac-User).

### Datenmodell (neu, additiv zum bestehenden Schema)

Getrennt von der abgeleiteten `projects`-Tabelle (Cluster-Sicht aus `folder_tree`).

```sql
CREATE TABLE project_meta (
    id TEXT PRIMARY KEY,           -- ULID/uuid, clientseitig erzeugt
    name TEXT NOT NULL,
    date TEXT,                     -- YYYY-MM-DD, default heute
    client TEXT,                   -- optional, Freitext
    ff_notion_url TEXT,            -- optional; gesetzt = FF-Projekt
    created_by TEXT,               -- Mac-User
    created_at TEXT,
    status TEXT DEFAULT 'active'   -- active | closed
);

CREATE TABLE ingest_session (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,      -- FK project_meta.id
    card_label TEXT,               -- Volume-Name der Karte
    card_uuid TEXT,                -- falls verfügbar
    source_bytes INTEGER,
    source_files INTEGER,
    target_a_disk TEXT,            -- Ziel-Platte 1 (Name/UUID)
    target_b_disk TEXT,            -- Ziel-Platte 2
    copies_done INTEGER,          -- 0 | 1 | 2
    verify_status TEXT,            -- pending | verifying | verified | mismatch
    state TEXT,                    -- detected | copying | verifying | done | error
    started_at TEXT,
    finished_at TEXT,
    host TEXT                      -- welcher Mac
);
```

Board-Status je Karte leitet sich aus `ingest_session` ab: **ungesichert** (0),
**1× (Achtung)**, **2× verifiziert (formatieren ok)**, **Fehler/Mismatch**.

### Komponenten (isoliert, je eine Aufgabe)

- **`ingest/device_monitor`** — erkennt gemountete Wechselmedien (Karte vs. Ziel-Platte),
  meldet Kandidaten. Baut auf vorhandener Mount-Erkennung auf.
- **`ingest/copier`** — chunked Copy Quelle→2 Ziele, Fortschritt, resumable/journaled.
  Invariante: **erst kopieren+verifizieren, dann darf jemand formatieren** (nie vorher grün).
- **`ingest/verify`** — Verify der 2 Kopien gegen Quelle (Phase A: Größe+Dateizahl+xxHash-Sample;
  volle SHA-256 kommt in Phase B). Setzt `verify_status`.
- **`ingest/organize`** — legt Zielordner in NXT-Konvention an: `YYYY-MM-DD_Projekt_TYP`
  mit `FOOTAGE/PHOTOS/BTS/WORKING`, abgeleitet aus `project_meta` + Karten-Typ.
- **`project_store`** — CRUD für `project_meta`/`ingest_session`, lokal + D1-Push
  (gleiche `d1_client`-Mechanik wie Scan-Digest).
- **Worker `/board` + `/board.json`** — Live-DIT-Board (VIEW_TOKEN-gated wie `/dashboard`):
  offene Projekte, je Karte 2×-Status, „formatieren ok?". Auto-refresh.
- **Menubar-UI** — „Neues Projekt" (Name/Kunde/FF-Link/Datum) · offenes Projekt wählen ·
  „Karte einlesen" · Fortschritt/Status. Muster: bestehende rumps-Menüs.

### Flow (Projekt-zuerst)

1. Menubar → „Neues Projekt" (oder aktives wählen). Anlegen: Name*, Datum(auto), Kunde?, FF-Link?.
2. Karte anstecken → `device_monitor` erkennt sie → „In Projekt X einlesen?".
3. `copier` → 2 Ziele → `verify` → `organize` in NXT-Konvention.
4. `ingest_session` wandert `detected→copying→verifying→done`; Push an D1.
5. Board zeigt live: welche Karte 2× sicher = **grün/formatieren ok**.

### YAGNI für Phase A

- Content-Hash-Katalog/Sidecars (Phase B), KI (Phase D), Notion-Mirror (Phase C) bleiben draußen.
- Verify in Phase A: Größe+Count+xxHash-Stichprobe genügt fürs Board; volle Byte-Identität Phase B.

## Offene Punkte für nach der Compaction

- Scope-Forks 1–4 final entscheiden (Empfehlungen oben).
- Test-Ingest hatten wir noch offen (CutValet-Sidecar-/catalog.db-Format an echten Daten) —
  bei „intern nachbauen" nur noch als *Referenz* interessant, nicht als Abhängigkeit.
- Danach: writing-plans für **Phase A (Ingest + Live-DIT-Board)** als erste Umsetzung.
