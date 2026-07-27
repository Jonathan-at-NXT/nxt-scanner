# Automatisierte Speicherverwaltung — Design-Spec (v2)

**Datum:** 2026-07-27
**Status:** Design zur Review (Umsetzung startet mit Phase 1 nach Freigabe)
**v2-Änderung:** 3-Schicht-Architektur mit menschlicher **Kontext-Schicht**
(kuratierter Confirm-Loop); gestuftes Datenmodell; Manager-Mac mit 3 USB-C.

## Vision

Jonathan steckt nur die Platte(n) an, die das System ihm nennt (Push aufs Handy),
und das System sorgt dafür, dass **jedes Projekt genau 2× gesichert** ist — mit
einer belastbaren **Datenverlust-Garantie** über alle eigenen Operationen. Läuft
als Hintergrund-Daemon auf einem dedizierten Manager-Mac (Jonathans alter MacBook,
3 USB-C-Ports).

## Warum drei Schichten (Kernerkenntnis)

Reine Daten (Größen, Dateizahlen, Hashes) können drei Dinge **grundsätzlich nicht**
— belegt durch die KTM/GIROCARD-Analyse:
1. **Identität & Beziehungen** — „MAIN FILM ist Teil der ORANGE-BLOOD-Kampagne";
   „`2024-10-30_GIROCARD 1` und `GIROCARD 1` sind dasselbe".
2. **Wichtigkeit & Disposition** — kritisch (2×) vs. Müll/löschbar (`_KILL`,
   `_TO CHECK`, `DROPBOXSYNC`) vs. Archiv (1× ok).
3. **Physische Welt** — „es gibt eine Backup-Platte, die nie gescannt wurde";
   „diese Platte ist tot/offsite".

Deshalb ist **menschlicher Kontext erstklassiger Systembestandteil**, kein
Nachgedanke.

```
Schicht 1: DATEN        (Größen/Metadaten/Hashes)     — beweist Sicherheit
Schicht 2: KONTEXT      (menschlich, kuratiert)        — liefert Identität/Wichtigkeit/Beziehung
Schicht 3: ENTSCHEIDUNG (Planer + Executor)            — plant & führt aus
```

## Die Garantie (präzise)

Keine absolute „nichts kann je kaputtgehen"-Garantie (Hardware kann sterben) —
sondern eine **Invariante über alle Operationen des Systems**:

> **INV-1:** Keine Operation reduziert je die Zahl gesunder, lesbarer Kopien eines
> Ordners unter 2. Ist der Ist-Stand < 2, wird nur erhöht, nie gesenkt.
>
> **INV-2:** Gelöscht wird ausschließlich, wenn **≥ 2 inhaltlich (per Hash)
> verifizierte Kopien auf zwei physisch verschiedenen Datenträgern** jetzt lesbar
> vorliegen.
>
> **INV-3:** Jede Operation ist **journaled** (durable) *vor* Ausführung und
> **resumierbar/idempotent**. Ein Abbruch verletzt nie INV-1/INV-2.
>
> **INV-4 (Kontext-Sicherheit):** Menschlicher Kontext darf **Intention und
> Priorität** steuern (z. B. „nie unter 2×", „prüfen vor Löschen"), aber **niemals
> das Löschen einer einzigen bewiesenen Kopie autorisieren**. Lösch-Sicherheit ist
> **daten-bewiesen**, nicht label-gesteuert. Ein falsches menschliches Urteil kann
> so nie Daten kosten.

Betriebsregel: **Copy → Verify → (erst dann) Delete.** Nie „move".

## Nicht-Ziele

- Kein Schutz gegen physisches Sterben der *einzigen* Kopie. Gegenmaßnahme:
  Einzelkopien zuerst duplizieren (Fenster minimieren).
- Kein Cloud-Backup (lokale Flotte).
- Kein automatisches Umbenennen (Namenskonvention = separates Hygiene-Thema;
  Aliase werden im Kontext gepflegt, nicht durch Umbenennen erzwungen).

## Schicht 1 — Daten (gestuftes Modell)

Genau so tief wie nötig, nicht tiefer. Drei Stufen, aufsteigender Aufwand:

| Stufe | Kosten | Beantwortet | Wo gespeichert |
|---|---|---|---|
| **1 Ordner-Aggregate** (haben wir) | gratis | „gibt's eine Ganz-Ordner-Kopie?" | lokal + D1 |
| **2 Datei-Metadaten-Manifest** (Pfad, Größe, mtime je Datei) | billig — Verzeichnis-Walk, **kein** Byte-Lesen | Teilmenge/Umgruppierung (MAIN-FILM-in-ORANGE-BLOOD), ~99 % sicher | **lokal voll**, Digest → D1 |
| **3 Content-Hashes** (BLAKE3/SHA-256 je Datei) | teuer (alle Bytes lesen) | Byte-Identität, **Pflicht nur vor Löschung** | lokal, on-demand |

**Optimierung:** Stufe 2 **blanket** (schnell, ganze Flotte, neuer Scan-Standard);
Stufe 3 **nur punktuell** vor einer Löschung (die 2 Überlebenden gegen-hashen).
Es müssen **nie** 480 TB blind gehasht werden.

**Manifest-Digest für D1:** volle Datei-Liste bleibt lokal auf dem Manager-Mac
(SQLite, Millionen Zeilen sind unkritisch); nach D1 geht nur ein kompakter Digest
je Ordner (Datei-Größen-Histogramm-Hash + count + total) — D1 bleibt schlank.

**Matching-Stufen (namensunabhängig):**
- **Strukturell (Stufe 1/2):** zwei Ordner = gleiche Daten, wenn Gesamtgröße ±1 %
  UND Dateizahl ±2 % (Union-Find-Clustering). Löst GIROCARD-Aliase, ORANGE-BLOOD-
  Dopplung. Teilmengen über Datei-Größen-Multimengen (Stufe 2).
- **Beweis (Stufe 3):** identischer Content-Hash = byte-identisch. Nur vor Delete.

## Schicht 2 — Menschlicher Kontext (kuratierter Confirm-Loop)

Eine **Wissensbasis**, die nicht aus Ordnern ableitbar ist, kuratiert aufgebaut:

- **Projekt-Identität:** kanonisches Projekt → Aliase (`GIROCARD 1` ≡
  `2024-10-30_GIROCARD 1`).
- **Beziehungen:** Kampagne ⊃ Teilstücke (ORANGE BLOOD ⊃ MAIN FILM) — wichtig, weil
  „ist gesichert" auf Kampagnenebene ≠ auf Teilstück-Ebene.
- **Disposition** je Projekt: `2x-kritisch` / `archiv-1x` / `löschbar` / `prüfen`.
- **Flotten-Fakten:** existierende, aber ungescannte Platten (offsite/Backup); tote
  Platten; Sonderrollen.

**Mechanismus — Suggest → Confirm:** Das System bildet aus Schicht-1-Daten eine
**Vermutung** und legt sie dem Menschen vor; der bestätigt/korrigiert per Klick;
die Antwort wird Ground Truth und fließt in alle folgenden Entscheidungen.
Beispiele:
- „`GIROCARD 1` und `2024-10-30_GIROCARD 1` (beide 4724 GB) sind dasselbe Projekt?"
  → [Ja] [Nein] [gehören zusammen als …]
- „MAIN FILM (2,6 TB, nur Tower 6) ist eigenständig — als `2x-kritisch` markieren?"
- „`_KILL` (7,6 TB) als `löschbar` markieren?"

**UI: gehostete, kollaborative Team-Website** (Worker + D1, wie der `/rollout`-View,
aber interaktiv mit Schreib-Endpunkten). **Jeder im Team** kann kuratieren, nicht
nur Jonathan:
- **Matchen:** „diese 2 Einträge gehören zusammen" (Alias/Beziehung setzen).
- **Ergänzen:** „das hier lässt sich mit diesem ergänzen" (Teilstück ↔ Kampagne).
- **Gruppieren per Drag & Drop:** Projekte in einen **Überordner/Kampagne ziehen**;
  Aliase zusammenziehen; Hierarchie (Kampagne ⊃ Teilstücke) direkt visuell aufbauen.
  Jeder Drop schreibt eine `relation`/`alias`-Zeile mit Autor.
- **Disposition:** „das braucht nur 1× gesichert" / „2×-kritisch" / „löschbar".
- **Kommentieren:** freie Notizen je Projekt/Match („Rohmaterial liegt offsite bei
  X", „Kunde will Rohdaten behalten").

**Multi-User & nachvollziehbar:** jede Entscheidung und jeder Kommentar trägt
**Autor + Zeit**; Änderungen sind versioniert (Audit-Trail). Schreibzugriff über
Team-Auth (Token/Login); Lesen wie beim Rollout-View. Die Wissensbasis wird so ein
**gemeinsames, lebendes Team-Artefakt** statt Einzelwissen — der bisher flüchtige
Chat-Loop mit Claude wird dauerhaft festgehalten.

**Claude als Kontext-Brücke:** sobald der Kontext strukturiert vorliegt, kann das
LLM die unscharfen Schlüsse ziehen, die der deterministische Matcher nicht kann —
und **Vermutungen für die Team-Website erzeugen** („diese beiden 4724-GB-Ordner
sind vermutlich dasselbe — bestätigen?"), die das Team dann bestätigt/korrigiert.

**Sicherheit:** Kontext steuert Intention/Priorität, nie Datenverlust (INV-4).

## Schicht 3 — Entscheidung (Planer + Executor)

### Planer
- Eingaben: Matching-Ergebnis (Schicht 1) + Kontext (Schicht 2: Aliase,
  Beziehungen, Disposition) + Plattenkapazitäten.
- Zielzustand: jedes Projekt mit Disposition `2x-kritisch`/(default) **genau 2×**
  auf 2 verschiedenen Platten; `archiv-1x` → 1×; `löschbar` → Vorschlag zum Löschen
  (nie automatisch); `prüfen` → blockiert, bis Mensch entscheidet.
- **Priorität 1 (Sicherheit):** alle 1-Kopie-Projekte auf 2 bringen — *vor* jeder
  Löschung. Prio 2: Überschuss (>2) auf 2 reduzieren. Prio 3: Konsolidieren.
- **Wechsel-Optimierung:** minimiert Anstecken/Abziehen; mit 3 USB-C-Ports lassen
  sich Quelle + Ziel + Hub gleichzeitig mounten → viele Operationen pro Plattensatz.
- Ausgabe: geordnete **Session-Liste**, immer erst als **Dry-Run** einsehbar.

### Sicherer Executor
Zustandsautomat je Operation (Journal, lokal + D1-Spiegel):
```
PLANNED → COPYING → COPIED → VERIFYING → VERIFIED → (INV-2 erfüllt?) → DELETING → DONE
        └── Fehler/Abbruch: zurück auf PLANNED, Ziel-Reste verwerfen ──┘
```
- **Kopieren:** `rsync -a --partial` zwischen zwei gemounteten Volumes.
- **Verifizieren:** *unabhängige* Re-Hash (Stufe 3) des Ziels gegen Quell-Manifest —
  nicht rsync vertrauen. Erst bei Match → VERIFIED.
- **Löschen (INV-2 + INV-4):** nur wenn 2 hash-verifizierte Kopien auf 2 UUIDs
  lesbar sind UND Disposition Löschen nicht verbietet. Überschuss wandert erst nach
  `PENDING_DELETE`; endgültig gelöscht nur nach Session-Freigabe.
- **Resume:** Journal beim Start lesen; unfertige Kopien per Hash prüfen/fortsetzen;
  keine Löschung ohne erfüllte Vorbedingung.

## Hardware-Modell

- **Manager-Mac:** Jonathans alter MacBook, **3 USB-C-Ports**, läuft 24/7 als
  Daemon. Jonathan arbeitet parallel am neuen Mac.
- 3 Ports → **Quelle + Ziel + Hub gleichzeitig**; kaum Umstecken pro Kopie, Wechsel
  nur beim Übergang zum nächsten Plattensatz.
- Interne SSD nicht als Stage (zu klein); Kopien Volume→Volume.
- Umschichten braucht Slack → ggf. große Hub-/Zielplatte(n) als Puffer.

## Datenmodell (additiv)

```sql
-- Schicht 1: Stufe-2-Manifest (lokal voll auf Manager-Mac)
file_manifest(disk_uuid, rel_path, size, mtime, hash NULLABLE, hashed_at NULLABLE,
              PRIMARY KEY(disk_uuid, rel_path))
-- je Ordner ein Digest (nach D1 gepusht, D1 bleibt schlank)
folder_digest(disk_uuid, top_rel_path, size_hist_hash, file_count, total_size,
              manifest_hash NULLABLE, built_at, PRIMARY KEY(disk_uuid, top_rel_path))

-- Schicht 2: Kontext (kuratiert, kollaborativ, mit Autor+Zeit)
project(canonical_id, title, campaign_id NULLABLE, disposition, updated_by, updated_at)
alias(canonical_id, folder_signature, source_disk, source_path, confirmed_by, confirmed_at)
relation(parent_id, child_id, kind, created_by, created_at)  -- Kampagne ⊃ Teilstück, "ergänzt"
comment(id, target_kind, target_id, body, author, created_at)  -- freie Team-Notizen
context_audit(id, entity, entity_id, change, author, at)      -- Versionierung/Audit-Trail
fleet_disk(disk_uuid NULLABLE, label, status, note)   -- auch ungescannte/offsite/tote

-- Schicht 3: Operations-Journal
ops(id, kind, src_disk, src_path, dst_disk, dst_path, manifest_hash, state,
    planned_at, updated_at, note)
```

## Sicherheitsanalyse / Failure-Modes

| Ereignis | Verhalten |
|---|---|
| Strom weg / Platte abgezogen beim Kopieren | Ziel-Teilkopie unvollständig → Hash-Mismatch → verworfen/fortgesetzt; Quelle unberührt |
| Ziel defekt nach Copy, vor Verify | Verify schlägt fehl → nicht VERIFIED → Quelle bleibt |
| rsync meldet Erfolg, Daten falsch | unabhängige Re-Hash fängt es |
| „leere Hülle" als 2. Kopie | Hash ≠ → zählt nicht → echte Kopie nicht gelöscht |
| falsches menschliches Label | INV-4: Kontext kann kein Delete der einzigen Kopie auslösen |
| Nur-1-Kopie-Platte stirbt vor Duplizierung | **Restrisiko** — Einzelkopien zuerst; Fenster minimiert, nicht null |

## Phasen / Rollout

1. **Phase 1 — Stufe-2-Manifest + strukturelles Matching (read-only):** Scanner
   erfasst Datei-Metadaten-Manifest (Pfad/Größe/mtime), lokale Tabellen, Digest →
   D1, strukturelles Matching (Größe/Dateizahl-Cluster, Teilmengen). Liefert sofort:
   korrekte namensunabhängige Kopien-Zahl, Teilmengen-Klärung. **Start hier.**
2. **Phase 2 — Kontext-Schicht + kollaborative Team-Website:** Wissensbasis-Tabellen,
   gehostete interaktive Kuratier-UI (Worker+D1) mit Matchen/Ergänzen/Disposition/
   Kommentaren, Multi-User + Autor/Audit; Claude erzeugt Vermutungen. Read-only
   bzgl. Dateien.
3. **Phase 3 — Planer (Dry-Run):** Session-Plan „exakt 2×" unter Nutzung von
   Matching + Kontext; zeigt Plan, ändert nichts.
4. **Phase 4 — Executor Copy-only:** Auffüllen 1→2 mit Copy-Verify (Stufe-3-Hash),
   Journal/Resume. Nur *hinzufügen*, kein Delete.
5. **Phase 5 — Executor Delete (gated):** Überschuss-Löschung nach INV-2/INV-4,
   `PENDING_DELETE` + Freigabe.
6. **Phase 6 — Daemon + Push + Panel:** 24/7 auf Manager-Mac, Handy-Push, Live-Panel.

## Offene Entscheidungen

- Hash-Algorithmus: BLAKE3 (schnell, Dependency) vs SHA-256 (Stdlib).
- Push-Kanal fürs Handy: ntfy.sh / Telegram / Pushover.
- Ungescannte Backup-Platten (KTM-Frage) — via `fleet_disk` erfassen.
- Konsolidierungs-Zielplatten: vorhandene HUBs oder neue kaufen?
- Manifest-Push-Frequenz/Digest-Format (D1-Schlankheit).
- Team-Auth für die Kuratier-Website (einfacher geteilter Token je Person vs.
  richtiges Login); Identität für Autor-Attribution.
