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

## Offene Punkte für nach der Compaction

- Scope-Forks 1–4 final entscheiden (Empfehlungen oben).
- Test-Ingest hatten wir noch offen (CutValet-Sidecar-/catalog.db-Format an echten Daten) —
  bei „intern nachbauen" nur noch als *Referenz* interessant, nicht als Abhängigkeit.
- Danach: writing-plans für **Phase A (Ingest + Live-DIT-Board)** als erste Umsetzung.
