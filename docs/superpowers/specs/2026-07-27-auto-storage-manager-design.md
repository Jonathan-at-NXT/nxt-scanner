# Automatisierte Speicherverwaltung — Design-Spec

**Datum:** 2026-07-27
**Status:** Design zur Review (Umsetzung startet mit Phase 1 nach Freigabe)

## Vision

Jonathan steckt nur die Platte(n) an, die das System ihm nennt (Push aufs Handy),
und das System sorgt dafür, dass **jedes Projekt genau 2× gesichert** ist — mit
einer belastbaren **Datenverlust-Garantie** über alle eigenen Operationen. Läuft
als Hintergrund-Daemon auf einem dauerhaft laufenden Mac.

## Die Garantie (präzise)

Keine absolute „nichts kann je kaputtgehen"-Garantie (Hardware kann sterben) —
sondern eine **Invariante über alle Operationen des Systems**:

> **INV-1:** Keine Operation reduziert je die Zahl gesunder, lesbarer Kopien eines
> Ordners unter 2. Ist der Ist-Stand < 2, wird nur erhöht, nie gesenkt.
>
> **INV-2:** Gelöscht wird ausschließlich, wenn **≥ 2 inhaltlich verifizierte
> Kopien auf zwei physisch verschiedenen Datenträgern** (verschiedene Volume-UUID)
> **jetzt lesbar** vorliegen.
>
> **INV-3:** Jede Operation ist **journaled** (durable) *vor* Ausführung und
> **resumierbar/idempotent**. Ein Abbruch (Strom, Abziehen) hinterlässt nie einen
> Zustand, der INV-1/INV-2 verletzt.

Daraus folgt die Betriebsregel **Copy → Verify → (erst dann) Delete**; niemals
„move". Verifikation = **Inhalts-Hash-Vergleich**, nicht Größe/Name.

## Nicht-Ziele

- Kein Schutz gegen physisches Sterben einer Platte, die die *einzige* Kopie hält.
  Gegenmaßnahme: Einzelkopien werden **zuerst** dupliziert (Fenster minimieren).
- Kein Cloud-Backup (rein lokale Datenträger-Flotte).
- Kein Umbenennen/Aufräumen der Namenskonvention (separates Hygiene-Thema).

## Kernentscheidung: Content-Fingerprinting als Fundament

Namens-/Datums-Matching ist fragil (Beleg: KTM MAIN FILM — eine anders benannte
oder unklassifizierte 2. Kopie wäre unauffindbar). Deshalb wird **Projekt-Identität
über Inhalt** definiert:

- **Datei-Hash:** BLAKE3 (oder SHA-256) je Datei.
- **Ordner-Manifest:** sortierte Liste `(rel_path, size, mtime, hash)` je Top-Level-
  Ordner; daraus ein **gerollter Manifest-Hash** (Merkle-Wurzel).
- Zwei Ordner sind **dieselben Daten**, wenn ihr Manifest-Hash gleich ist — egal
  wie sie heißen, wo sie liegen, welches Datum im Namen steht.
- **Teil-Identität** (eine Kopie hat Untermenge/Obermenge): über Datei-Hash-
  Schnittmengen erkennbar → findet „leere/kaputte" Kopien exakt.

Das ersetzt die Fuzzy-Cluster-Logik für alle sicherheitsrelevanten Entscheidungen.
Fuzzy-Namensmatching bleibt nur als *Hinweisgeber* fürs Reporting.

## Architektur — 5 Bausteine

```
[1 Fingerprinting]  beim Anstecken: hasht jeden Top-Level-Ordner (read-only)
        │  Manifest je Ordner -> lokale DB + D1
        ▼
[2 Identitäts-Index] gruppiert Ordner nach Manifest-Hash (echte Kopien-Zahl)
        │
        ▼
[3 Planer]  Ziel: jedes Projekt genau 2×. Reihenfolge: erst Einzelkopien sichern,
        │   dann Überschuss löschen. Optimiert Platten-Wechsel-Sequenz. Kapazität.
        ▼
[4 Executor]  Copy→Verify→Delete-Zustandsautomat, journaled, resumierbar (rsync +
        │   unabhängige Re-Hash-Verifikation). Dry-Run + Freigabe pro Session.
        ▼
[5 Daemon + Push]  launchd-Dienst, Mount-Watch; Handy-Push „stecke Platte X an";
            Live-Control-Panel (wie Rollout-View).
```

### Baustein 1 — Fingerprinting (Phase 1, read-only, Start)

- Erweiterung des bestehenden Scanners (`storage_scanner`). Neuer Modul
  `fingerprint.py`: berechnet je Top-Level-Ordner das Manifest.
- **Inkrementell & resumierbar:** Datei wird nur neu gehasht, wenn `(size, mtime)`
  sich änderte (Cache in DB). Abbruch-sicher: pro Datei committen.
- **Performance:** BLAKE3 (mehrere GB/s); paralleles Lesen begrenzt (HDD: seriell,
  SSD: parallel). Netzlaufwerke: optional überspringen (zu langsam).
- Ergebnis geht in neue Tabellen (lokal + D1): `file_hashes`, `folder_manifest`.

### Baustein 2 — Identitäts-Index

- Query: gruppiere `folder_manifest.manifest_hash` → Kopienzahl je Inhalt.
- Erkennt exakt: 0/1/2/n Kopien, **Teilkopien** (Untermengen), **leere Hüllen**.
- Löst die KTM-Frage endgültig: existiert derselbe Inhalt anderswo (beliebiger Name)?

### Baustein 3 — Planer

- Eingaben: Identitäts-Index, Plattenkapazitäten/Belegung, Plattenpaar-Constraints.
- Zielzustand: jedes Manifest genau 2× auf 2 verschiedenen Platten.
- **Priorität 1 (Sicherheit):** alle 1-Kopie-Ordner auf 2 bringen — *vor* jeder
  Löschung. Priorität 2: Überschuss (>2) auf 2 reduzieren. Priorität 3: Konsolidieren.
- **Wechsel-Optimierung:** minimiert Anzahl Anstecken/Abziehen (Mensch = Flaschenhals);
  gruppiert alle Operationen, die mit einem gegebenen Plattensatz möglich sind.
- Ausgabe: geordnete **Session-Liste** — je Session „stecke A (Quelle) + B (Ziel) an",
  dann konkrete Copy/Delete-Schritte. Immer erst als **Dry-Run** einsehbar.

### Baustein 4 — Sicherer Executor

Zustandsautomat je geplanter Operation (im Journal, D1 + lokal gespiegelt):
```
PLANNED → COPYING → COPIED → VERIFYING → VERIFIED → (Quelle löschbar?) → DELETING → DONE
                 └────── bei Fehler/Abbruch: zurück auf PLANNED, Ziel-Reste verwerfen
```
- **Kopieren:** `rsync -a --partial` zwischen zwei gemounteten Volumes.
- **Verifizieren:** *unabhängige* Re-Hash des Ziels, Vergleich mit Quell-Manifest
  (nicht rsync vertrauen). Erst bei Match → VERIFIED.
- **Löschen (INV-2):** nur wenn Ziel-Manifest == Quell-Manifest UND eine **zweite**
  verifizierte Kopie auf anderer UUID existiert und lesbar ist. Löschung erst nach
  expliziter Session-Freigabe; überzählige Kopien wandern zuerst in Status
  `PENDING_DELETE` (nichts verschwindet ungefragt).
- **Resume:** beim Start liest der Executor das Journal; unfertige Kopien werden per
  Hash geprüft und fortgesetzt/neu gemacht; keine Löschung ohne erfüllte Vorbedingung.
- **Abbruch/Abziehen mitten drin:** Ziel-Teilkopie ist nie „gut" (Hash-Mismatch) →
  wird verworfen/fortgesetzt; Quelle bleibt unberührt bis VERIFIED.

### Baustein 5 — Daemon, Push, Control-Panel

- **Daemon:** launchd-Dienst (Muster wie bestehender Scanner), läuft 24/7 auf dem
  neuen Mac. Reagiert auf Mount-Events, treibt den Plan, führt Sessions aus.
- **Handy-Push:** wenn nächste Platte gebraucht wird → Push mit „Stecke *NXT 014*
  an". Kanal: **ntfy.sh** / **Telegram-Bot** / **Pushover** (Entscheidung offen).
- **Control-Panel:** Live-Seite (wie `/rollout`) — aktueller Plan, „nächste Platte",
  Fortschritt je Session, Sicherheitsstatus, Historie.

## Hardware-Modell

- **2-Bay-Dock + optionaler Hub:** zwei Platten gleichzeitig (Quelle + Ziel) für
  direktes Kopieren; große Hub-Platte als Konsolidierungsziel.
- Interne Mac-SSD **nicht** als Stage (zu klein); alle Kopien Volume→Volume.
- Umschichten braucht Slack → ggf. 1–2 große Zielplatten als Puffer.

## Datenmodell (neu, additiv)

```sql
-- je Datei ein Hash (inkrementell via size+mtime-Cache)
file_hashes(disk_uuid, rel_path, size, mtime, hash, hashed_at,
            PRIMARY KEY(disk_uuid, rel_path))
-- je Top-Level-Ordner ein gerolltes Manifest
folder_manifest(disk_uuid, top_rel_path, manifest_hash, file_count,
                total_size, built_at, PRIMARY KEY(disk_uuid, top_rel_path))
-- Operations-Journal (Executor-Zustandsautomat)
ops(id, kind, src_disk, src_path, dst_disk, dst_path, manifest_hash,
    state, planned_at, updated_at, note)
```

## Sicherheitsanalyse / Failure-Modes

| Ereignis | Verhalten |
|---|---|
| Strom weg beim Kopieren | Ziel-Teilkopie unvollständig → Hash-Mismatch → verworfen/fortgesetzt; Quelle unberührt |
| Platte abgezogen beim Kopieren | wie oben; Operation bleibt PLANNED/COPYING, resume später |
| Ziel-Platte defekt nach Copy, vor Verify | Verify schlägt fehl → nicht VERIFIED → Quelle bleibt, keine Löschung |
| rsync meldet Erfolg, Daten aber falsch | unabhängige Re-Hash-Verifikation fängt es → nicht VERIFIED |
| „leere Hülle" als vermeintliche 2. Kopie | Manifest-Hash ≠ → zählt nicht als Kopie → keine Löschung der echten |
| Nur-1-Kopie-Platte stirbt vor Duplizierung | **Restrisiko** — deshalb Einzelkopien zuerst; Fenster minimiert, nicht null |
| Bug im Planer schlägt Löschung vor | Executor prüft INV-2 unabhängig zur Laufzeit; ohne 2 lesbare Kopien kein Delete |

## Phasen / Rollout

1. **Phase 1 — Fingerprinting (read-only, null Risiko):** Manifest-Erfassung im
   Scanner, DB-Tabellen, D1-Push, Identitäts-Index. Liefert sofort: echte
   Kopien-Zahl je Inhalt, leere/kaputte Backups, KTM-Auflösung. **Start hier.**
2. **Phase 2 — Planer (Dry-Run):** berechnet Session-Plan „exakt 2×", zeigt ihn,
   ändert nichts. Review durch Jonathan.
3. **Phase 3 — Executor (Copy-only):** nur Auffüllen 1→2 mit Copy-Verify, kein
   Delete. Journal/Resume. Erst hier werden Daten geschrieben (nur *hinzugefügt*).
4. **Phase 4 — Executor (Delete gated):** Überschuss-Löschung nach INV-2, opt-in,
   `PENDING_DELETE` + finale Freigabe.
5. **Phase 5 — Daemon + Push + Panel:** 24/7-Betrieb, Handy-Push, Live-Panel.

## Offene Entscheidungen

- Hash-Algorithmus: BLAKE3 (schnell, neue Dependency) vs SHA-256 (Stdlib).
- Push-Kanal: ntfy.sh / Telegram / Pushover.
- Gibt es **ungescannte** Platten (Offsite/Backup), die Kopien halten? (KTM-Frage)
- Konsolidierungs-Zielplatten: vorhandene HUBs nutzen oder neue kaufen?
