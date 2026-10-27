/**
 * Auswertung für das Dashboard (ersetzt run_analysis()/sync_log() aus notion_sync.py).
 *
 * Eingabe: projects (abgeleitet), disks, disk_meta, project_rule, finding_state.
 * Ausgabe: Projekte (je Cluster, Typen mit Kopien), Datenträger, Befunde, Kennzahlen.
 * Befund-Typen und Prioritäten wie im Notion-Log, ergänzt um Pflicht-Stufen.
 */

export const SICHERUNG = {
  loeschbar: { label: "Löschbar", min: 0 },
  einfach: { label: "Einfach sichern", min: 1 },
  doppelt: { label: "Doppelt sichern", min: 2 },
  dreifach: { label: "Dreifach sichern", min: 3 },
};
export const DEFAULT_SICHERUNG = "doppelt";
export const TIERS = ["SSD", "HDD", "NAS", "CLOUD"];
export const PRIORITY = { MISSING_BACKUP: "kritisch", TIER_MISSING: "kritisch", SIZE_MISMATCH: "warnung",
  INCOMPLETE_BACKUP: "warnung", ZERO_FILES: "warnung", EXCESS_COPIES: "info", PARTIAL_COPY: "info", REF_EXTRA: "info", UMBENENNEN: "info", DUPLICATE_DISK_NAME: "info",
  DISK_DEFECT: "kritisch", DISK_WARN: "warnung" };

const gb = (b) => Math.round(((b || 0) / 1e9) * 100) / 100;
// Anzeige im Befund-Text: deutsch, ab 100 GB ohne Nachkommastellen
const fg = (g) => (g ?? 0).toLocaleString("de-DE", { maximumFractionDigits: g >= 100 ? 0 : g >= 10 ? 1 : 2 }) + " GB";

export function guessTier(name) {
  const n = (name || "").toUpperCase().trim();
  if (/^(TOWER|NH|NXT HUB|HUB)\b/.test(n)) return "HDD";
  if (/^NXT\s*\d+$/.test(n)) return "SSD";
  if (/(NAS|NXTCLOUD|SERVER)/.test(n)) return "NAS";
  return null;
}

/** Datenträger mit Stufe, Halter, Füllstand und Namensdoppel-Hinweis. */
export function buildDisks(disks, metaRows, projects, healthRows = []) {
  const meta = new Map(metaRows.map((m) => [m.disk_uuid, m]));
  const healthBy = new Map(healthRows.map((h) => [h.disk_uuid, h]));
  const byName = new Map();
  for (const d of disks) if (!d.uuid.startsWith("name:")) byName.set(d.name, (byName.get(d.name) || 0) + 1);
  const projCount = new Map(), validBytes = new Map();
  for (const p of projects) {
    if (p.status !== "validated") continue;
    projCount.set(p.disk_uuid, (projCount.get(p.disk_uuid) || 0) + 1);
    validBytes.set(p.disk_uuid, (validBytes.get(p.disk_uuid) || 0) + (p.size_bytes || 0));
  }
  // Gleicher Name, mehrere UUIDs: bis jemand bestätigt, dass es getrennte Platten sind,
  // zählt nur der neueste Eintrag (ältere sind oft Neuformatierungen -> sonst Kopie zu viel).
  const newest = new Map();
  // Entfernt (umkehrbar), solange danach kein neuer Scan kam
  const isRetired = (d) => {
    const m = meta.get(d.uuid) || {};
    return !!m.retired_at && !(d.last_scan && Date.parse(d.last_scan) > Date.parse(m.retired_at));
  };
  for (const d of disks) {
    if (d.uuid.startsWith("name:") || isRetired(d)) continue;
    const cur = newest.get(d.name);
    if (!cur || (d.last_scan || "") > (cur.last_scan || "")) newest.set(d.name, d);
  }
  return disks.map((d) => {
    const m = meta.get(d.uuid) || {};
    const dup = (byName.get(d.name) || 0) > 1;
    const retired = isRetired(d);
    const stale = retired || (dup && newest.get(d.name)?.uuid !== d.uuid && !m.separate_disk);
    const tier = m.tier || guessTier(d.name);
    // Kapazität kleiner als die gezählten Dateien = Überlauf/SMB-Fehlwert (z. B. NAS 3,6 statt 87 TB) → unbekannt
    const cap = d.capacity_bytes && !(d.used_bytes > d.capacity_bytes) ? d.capacity_bytes : null;
    // Plausibilität: statvfs läuft auf macOS bei > 16 TiB über (negative/zu große Werte) → dann used_bytes
    const ok = (v) => v != null && v >= 0 && (!cap || v <= cap);
    // Beide plausibel → der größere (freien Platz nie überschätzen; Überlauf kann auch klein aussehen)
    const usedVals = [d.fs_used_bytes, d.used_bytes].filter(ok);
    const used = usedVals.length ? Math.max(...usedVals) : null;
    return {
      uuid: d.uuid, name: d.name, tier, tier_source: m.tier ? (m.tier_source || "manual") : (tier ? "guess" : null),
      holder: m.holder || d.last_user || null, holder_source: m.holder ? "manual" : "scan",
      location: m.location || null, note: m.note || null, enclosure: m.enclosure || null,
      capacity_gb: cap ? gb(cap) : null, used_gb: used != null ? gb(used) : null,
      free_gb: cap && used != null ? Math.max(0, gb(cap - used)) : null,
      fill: cap && used != null ? Math.min(1, used / cap) : null,
      bad_projects: 0, only_here: 0, only_here_gb: 0,   // werden nach der Projekt-Auswertung gefüllt
      health: healthBy.get(d.uuid) || null,
      last_scan: d.last_scan, last_user: d.last_user,
      projects: projCount.get(d.uuid) || 0, project_gb: gb(validBytes.get(d.uuid) || 0),
      duplicate_name: dup, separate_disk: !!m.separate_disk,
      stale,                     // zählt nicht als Kopie, bis als eigene Platte bestätigt
      fallback: d.uuid.startsWith("name:"),
      retired, retired_at: m.retired_at || null, retired_reason: m.retired_reason || null,
    };
  });
}

/** Projekte je Cluster inkl. Regel, Kopien je Typ, Befunde. */
export function analyze({ projects, disks, diskMeta = [], rules = [], states = [], health = [], reserveMonths = DEFAULT_RESERVE_MONTHS, legacyGb = 0, zeroFiles = [], zeroMissing = null }) {
  const diskList = buildDisks(disks, diskMeta, projects, health);
  const disk = new Map(diskList.map((d) => [d.uuid, d]));
  const ruleBy = new Map(rules.map((r) => [r.rule_key, r]));
  const phys = (u) => (String(u).startsWith("nas:") ? String(u).split("/")[0] : u);
  for (const d of diskList) if (d.uuid.startsWith("nas:") && !disk.has(phys(d.uuid)))
    disk.set(phys(d.uuid), { ...d, uuid: phys(d.uuid), name: "NAS", tier: "NAS" });
  // Für "vollständige Kopie" zählen nur leere Dateien, deren Inhalt anderswo intakt liegt, aber nicht in dieser Kopie
  // (zeroMissing). Überall leere Clips sind verloren und machen keine Kopie schlechter als die andere.
  const zeroIn = new Map();
  for (const z of zeroMissing ?? zeroFiles) {
    const parts = String(z.rel_path).split("/");
    const seen = new Set();
    for (let k = 0; k < Math.min(3, parts.length - 1); k++) {
      for (let j = k + 1; j < parts.length; j++) {
        const key = `${z.disk_uuid}|${parts.slice(k, j).join("/")}`;
        if (seen.has(key)) continue;
        seen.add(key); zeroIn.set(key, (zeroIn.get(key) || 0) + 1);
      }
    }
  }
  const stateBy = new Map(states.map((s) => [s.finding_key, s]));
  const findings = [];
  const add = (f) => {
    const st = stateBy.get(f.key);
    findings.push({ ...f, priority: PRIORITY[f.type], status: st?.status || "offen", state_note: st?.note || null,
                    state_by: st?.updated_by || null });
  };

  const clusters = new Map();
  for (const p of projects) {
    if (p.status !== "validated" || !p.cluster_id) continue;
    if (p.disk_uuid.startsWith("name:")) continue; // veraltete Fallback-Records zählen nie als Kopie
    if (disk.get(p.disk_uuid)?.stale) continue;     // älterer Eintrag bei Namensdoppel
    if (!clusters.has(p.cluster_id)) clusters.set(p.cluster_id, []);
    clusters.get(p.cluster_id).push(p);
  }

  const out = [];
  for (const [cid, members] of clusters) {
    // Regel: erste passende Mitglieds-Regel (Schlüssel DATUM|NORMNAME), sonst Standard
    const keys = [...new Set(members.map((m) => `${m.date}|${m.norm_name}`))].sort();
    const rk = keys.find((k) => ruleBy.has(k));
    const rule = rk ? ruleBy.get(rk) : null;
    const sicherung = rule?.sicherung && SICHERUNG[rule.sicherung] ? rule.sicherung : DEFAULT_SICHERUNG;
    const min = SICHERUNG[sicherung].min;
    const requireTiers = (rule?.require_tiers || "").split(",").map((s) => s.trim()).filter(Boolean);
    let refCopies = {};
    try { refCopies = JSON.parse(rule?.ref_copies || "{}") || {}; } catch { refCopies = {}; }
    let typeRule = {};
    try { typeRule = JSON.parse(rule?.type_sicherung || "{}") || {}; } catch { typeRule = {}; }

    const byType = new Map();
    for (const m of members) {
      const t = m.type || "?";
      if (!byType.has(t)) byType.set(t, []);
      byType.get(t).push(m);
    }
    const types = [];
    let minCopies = Infinity, sizeGb = 0;
    for (const [t, rows] of [...byType].sort()) {
      // Regel je Ordnertyp (z. B. PHOTOS löschbar) vor der Projektregel
      const tsich = typeRule[t] && SICHERUNG[typeRule[t]] ? typeRule[t] : sicherung;
      const tmin = SICHERUNG[tsich].min;
      // Volle Kopie = Platte hat (Summe ihrer Ordner dieses Typs) >= FULL_COPY_RATIO der vollständigsten Platte.
      // Tages-Teilkopien (z. B. SSD mit nur einem Drehtag) zählen nicht als Kopie, sondern als "teilweise".
      // Je physischem Gerät: alle NAS-Ordner (nas:NXTCLOUD/…) sind EIN Gerät = höchstens eine Kopie.
      // Mehrere Ordner auf derselben Platte werden addiert (Tagesordner), auf dem NAS zählt der größte Ordnerstand.
      const sub = new Map(), zeroDisk = new Map();
      for (const r of rows) {
        sub.set(r.disk_uuid, (sub.get(r.disk_uuid) || 0) + gb(r.size_bytes));
        const ph = phys(r.disk_uuid);
        zeroDisk.set(ph, (zeroDisk.get(ph) || 0) + (zeroIn.get(`${r.disk_uuid}|${r.rel_path}`) || 0));
      }
      const perDisk = new Map();
      for (const [u, g] of sub) { const ph = phys(u); perDisk.set(ph, Math.max(perDisk.get(ph) || 0, g)); }
      const fullest = Math.max(0, ...perDisk.values());
      // Sollstand: manuell gewählte Kopie (z. B. ohne bewusst gelöschte Recorder-Daten), sonst die vollständigste
      const ref = refCopies[t] && perDisk.has(phys(refCopies[t])) ? phys(refCopies[t]) : null;
      const refGb = ref ? perDisk.get(ref) : fullest;
      // Vollständig nur ohne leere Dateien (0 Byte = Inhalt fehlt) und mit >= 95 % des Sollstands
      const isFull = (u) => !zeroDisk.get(u) && (refGb < SMALL_COPY_GB || perDisk.get(u) >= refGb * FULL_COPY_RATIO);
      const disksOf = [...perDisk.keys()].filter(isFull);
      const partialDisks = [...perDisk.keys()].filter((u) => !isFull(u));
      const copies = rows.map((r) => ({
        disk_uuid: r.disk_uuid, disk: disk.get(r.disk_uuid)?.name || "?", tier: disk.get(r.disk_uuid)?.tier || null,
        holder: disk.get(r.disk_uuid)?.holder || null, last_scan: disk.get(r.disk_uuid)?.last_scan || null,
        rel_path: r.rel_path, name: r.name, soll_name: r.soll_name || null,
        size_gb: gb(r.size_bytes), files: r.file_count, full: isFull(phys(r.disk_uuid)), ref: r.disk_uuid === ref,
        zero: zeroIn.get(`${r.disk_uuid}|${r.rel_path}`) || 0,
      })).sort((a, b) => (b.full - a.full) || a.disk.localeCompare(b.disk, "de", { numeric: true }));
      const sizes = disksOf.map((u) => Math.round(perDisk.get(u) * 100) / 100);
      const maxGb = Math.round(refGb * 100) / 100;
      sizeGb += maxGb;
      types.push({ type: t, copies, disks: disksOf.length, size_gb: maxGb, partial: partialDisks.length, ref,
                   min: tmin, sicherung: tsich, type_rule: typeRule[t] || null,
                   mismatch: disksOf.length >= 2 && new Set(sizes).size > 1 });

      const label = `${members[0].date} ${members[0].project_name}`;
      const core = (CORE_TYPES.has(t) || ![...byType.keys()].some((x) => CORE_TYPES.has(x))) && tsich !== "loeschbar";
      if (core) minCopies = Math.min(minCopies, disksOf.length);
      types[types.length - 1].core = core;
      if (disksOf.length < tmin && core) {
        add({ key: `MISSING_BACKUP|${cid}|${t}`, type: "MISSING_BACKUP", cluster: cid, project: label, folder_type: t,
              disks: disksOf, size_gb: maxGb,
              details: (disksOf.length ? `Vollständig nur auf ${disksOf.map((u) => disk.get(u)?.name || "?").join(", ")} (${fg(maxGb)}), Soll: ${tmin}×`
                                       : `Keine vollständige Kopie, Soll: ${tmin}×`) +
                       (partialDisks.length ? ` · Teilkopien: ${partialDisks.map((u) => `${disk.get(u)?.name || "?"} (${fg(perDisk.get(u))}${zeroDisk.get(u) ? `, ${zeroDisk.get(u)} leere Dateien` : ""})`).join(", ")}` : "") });
      }
      if (disksOf.length >= 2 && new Set(sizes).size > 1) {
        const diff = Math.round((Math.max(...sizes) - Math.min(...sizes)) * 100) / 100;
        add({ key: `SIZE_MISMATCH|${cid}|${t}`, type: "SIZE_MISMATCH", cluster: cid, project: label, folder_type: t,
              disks: disksOf, diff_gb: diff, size_gb: maxGb,
              details: disksOf.map((u) => `${disk.get(u)?.name || "?"}: ${fg(perDisk.get(u))}`).join(" vs ") + ` (Differenz ${fg(diff)})` });
      }
      // Teilkopien sind überflüssig, sobald die Regel mit vollständigen Kopien erfüllt ist (Löschbar zählt eigens)
      if (partialDisks.length && tsich !== "loeschbar" && disksOf.length >= Math.max(tmin, 1)) {
        const pgb = partialDisks.reduce((s, u) => s + perDisk.get(u), 0);
        add({ key: `PARTIAL_COPY|${cid}|${t}`, type: "PARTIAL_COPY", cluster: cid, project: label, folder_type: t,
              disks: partialDisks, size_gb: maxGb, savings_gb: Math.round(pgb * 100) / 100,
              details: `${disksOf.length}× vollständig, dazu Teilkopien: ` + partialDisks.map((u) => `${disk.get(u)?.name || "?"} (${fg(perDisk.get(u))})`).join(", ") });
      }
      // Mehr als der Sollstand: bewusst entferntes Material (z. B. Recorder-Daten) liegt auf größeren Kopien noch drin
      if (ref) {
        const extra = disksOf.map((u) => [u, perDisk.get(u) - refGb]).filter(([, x]) => x > Math.max(SMALL_COPY_GB, refGb * (1 - FULL_COPY_RATIO)));
        if (extra.length) add({ key: `REF_EXTRA|${cid}|${t}`, type: "REF_EXTRA", cluster: cid, project: label, folder_type: t,
              disks: extra.map(([u]) => u), size_gb: maxGb, savings_gb: Math.round(extra.reduce((s, [, x]) => s + x, 0) * 100) / 100,
              details: `Sollstand ${fg(refGb)} (${disk.get(ref)?.name || "?"}); mehr Material auf: ` + extra.map(([u, x]) => `${disk.get(u)?.name || "?"} (+${fg(x)})`).join(", ") });
      }
      if (disksOf.length > Math.max(tmin, 2) && tsich !== "einfach" && tsich !== "loeschbar") {
        const extra = disksOf.length - Math.max(tmin, 2);
        add({ key: `EXCESS_COPIES|${cid}|${t}`, type: "EXCESS_COPIES", cluster: cid, project: label, folder_type: t,
              disks: disksOf, size_gb: maxGb, savings_gb: Math.round(maxGb * extra * 100) / 100,
              details: `${disksOf.length} vollständige Kopien (Soll ${Math.max(tmin, 2)}): ` + disksOf.map((u) => `${disk.get(u)?.name || "?"} (${fg(perDisk.get(u))})`).join(", ") });
      }
    }

    // Unvollständige Kopie: eine Platte hat das Projekt, aber nicht alle Ordnertypen
    const allTypes = new Set(types.filter((x) => x.core).map((x) => x.type));
    if (min >= 2 && allTypes.size > 1) {
      const typesOnDisk = new Map();
      for (const m of members) {
        if (!typesOnDisk.has(m.disk_uuid)) typesOnDisk.set(m.disk_uuid, new Set());
        typesOnDisk.get(m.disk_uuid).add(m.type || "?");
      }
      for (const [du, ts] of typesOnDisk) {
        const missing = [...allTypes].filter((t) => !ts.has(t)).sort();
        if (missing.length) {
          add({ key: `INCOMPLETE_BACKUP|${cid}|${du}`, type: "INCOMPLETE_BACKUP", cluster: cid,
                project: `${members[0].date} ${members[0].project_name}`, disks: [du],
                details: `${disk.get(du)?.name || du}: fehlt ` + missing.map((t) => `${t} (${fg(types.find((x) => x.type === t).size_gb)})`).join(", ") });
        }
      }
    }

    // Pflicht-Stufen (z. B. "muss auf dem NAS liegen")
    const tiersPresent = new Set(members.map((m) => disk.get(m.disk_uuid)?.tier).filter(Boolean));
    for (const t of requireTiers) {
      if (!tiersPresent.has(t)) {
        add({ key: `TIER_MISSING|${cid}|${t}`, type: "TIER_MISSING", cluster: cid,
              project: `${members[0].date} ${members[0].project_name}`, disks: [],
              details: `Soll auch auf ${t} liegen, liegt aber nur auf: ${[...tiersPresent].join(", ") || "unbekannt"}` });
      }
    }

    // Umbenennen: Ordner mit Soll-Name, der vom echten Namen abweicht
    for (const m of members) {
      if (m.soll_name && m.soll_name !== m.name) {
        add({ key: `UMBENENNEN|${m.disk_uuid}|${m.rel_path}`, type: "UMBENENNEN", cluster: cid,
              project: `${members[0].date} ${members[0].project_name}`, disks: [m.disk_uuid],
              details: `${disk.get(m.disk_uuid)?.name || "?"}: „${m.name}“ → „${m.soll_name}“` });
      }
    }

    const copiesOverall = minCopies === Infinity ? 0 : minCopies;
    const coreTypes = types.filter((t) => t.core);
    const maxCopies = Math.max(0, ...coreTypes.map((t) => t.disks));
    // Fehlender Teil: nur Rohmaterial-Typen unter ihrer Regel zählen (WORKING etc. eigens als "soft")
    const missingGb = Math.round(coreTypes.filter((t) => t.disks < t.min).reduce((s, t) => s + t.size_gb, 0) * 100) / 100;
    const okAll = coreTypes.every((t) => t.disks >= t.min);
    const projMin = Math.max(0, ...coreTypes.map((t) => t.min));
    // Sicherungsstand: einfach = kein Teil hat eine zweite Kopie, teilweise = gemischt
    const level = projMin <= 1 ? (okAll ? "ok" : "einfach")
      : okAll ? (coreTypes.length && coreTypes.every((t) => t.disks >= 3) ? "mehrfach" : "ok")
      : maxCopies >= 2 ? "teilweise" : "einfach";
    out.push({
      id: cid, rule_key: rk || keys[0], date: members[0].date,
      name: members.map((m) => m.project_name).sort((a, b) => b.length - a.length)[0],
      aliases: [...new Set(members.map((m) => m.project_name))],
      types, size_gb: Math.round(sizeGb * 100) / 100,
      disks: [...new Set([...new Set(members.map((m) => m.disk_uuid))].map((u) => disk.get(u)?.name || u))],
      tiers: [...tiersPresent].sort(),
      copies: copiesOverall, copies_max: maxCopies, level, missing_gb: missingGb,
      sicherung, sicherung_min: projMin, require_tiers: requireTiers, type_rules: typeRule,
      rule_note: rule?.note || null, rule_by: rule?.updated_by || null, decided: !!rule,
      ok: okAll && requireTiers.every((t) => tiersPresent.has(t)),
    });
  }

  // Je Platte: Projekte mit Problemen (Regel nicht erfüllt) und Projekte, deren einziger
  // Ordner eines Typs hier liegt (fällt die Platte aus, ist das weg)
  for (const p of out) {
    const onDisks = new Set(p.types.flatMap((t) => t.copies.map((c) => c.disk_uuid)));
    if (!p.ok) for (const u of onDisks) { const d = disk.get(u); if (d) d.bad_projects++; }
    const onlyHere = new Map();
    for (const t of p.types) if (t.disks === 1) {
      const u = t.copies.find((c) => c.full).disk_uuid;
      onlyHere.set(u, (onlyHere.get(u) || 0) + t.size_gb);
    }
    for (const [u, g] of onlyHere) { const d = disk.get(u); if (d) { d.only_here++; d.only_here_gb = Math.round((d.only_here_gb + g) * 100) / 100; } }
  }

  // Platten-Check: defekte/auffällige Platten; dringend, wenn dort einzige Kopien liegen
  for (const d of diskList) {
    const h = d.health;
    if (!h || d.fallback || !["defekt", "warnung"].includes(h.verdict)) continue;
    const reasons = [h.smart_reason, h.fs_status === "defekt" ? h.fs_reason : null, h.read_errors ? h.read_reason : null]
      .filter((x) => x && x !== "unauffällig").join("; ");
    add({ key: `${h.verdict === "defekt" ? "DISK_DEFECT" : "DISK_WARN"}|${d.uuid}`, type: h.verdict === "defekt" ? "DISK_DEFECT" : "DISK_WARN",
          cluster: null, project: null, disks: [d.uuid],
          details: `${d.name}: ${reasons || h.verdict}` +
            (d.only_here ? ` · ${d.only_here} Projekte (${fg(d.only_here_gb)}) liegen nur hier → sofort sichern` : " · alles darauf hat eine 2. Kopie") });
  }

  // Leere Mediendateien (0 Byte = kaputte Kopie) je Datenträger, aus dem Datei-Katalog
  const zeroBy = new Map();
  for (const z of zeroFiles) {
    if (!zeroBy.has(z.disk_uuid)) zeroBy.set(z.disk_uuid, new Map());
    const parts = String(z.rel_path).split("/");
    const folder = parts.slice(0, Math.min(3, parts.length - 1)).join("/") || "(oberste Ebene)";
    const m = zeroBy.get(z.disk_uuid); m.set(folder, (m.get(folder) || 0) + 1);
  }
  for (const d of diskList) {
    const m = zeroBy.get(d.uuid);
    d.zero_files = m ? [...m.values()].reduce((s, x) => s + x, 0) : 0;
    if (!m || d.fallback || d.stale) continue;
    add({ key: `ZERO_FILES|${d.uuid}`, type: "ZERO_FILES", cluster: null, project: null, disks: [d.uuid],
          details: `${d.name}: ${d.zero_files} leere Mediendateien (0 Byte, Inhalt fehlt) – ` +
            [...m].sort((a, b) => b[1] - a[1]).slice(0, 4).map(([f, n]) => `${f} (${n})`).join(", ") +
            (m.size > 4 ? ` und ${m.size - 4} weitere Ordner` : "") });
  }

  // Flotte: gleiche Namen bei verschiedenen Platten
  const seen = new Map();
  for (const d of diskList) if (!d.fallback && !d.retired) { if (!seen.has(d.name)) seen.set(d.name, []); seen.get(d.name).push(d); }
  for (const [name, ds] of seen) {
    if (ds.length > 1 && ds.some((d) => d.stale)) add({ key: `DUPLICATE_DISK_NAME|${name}`, type: "DUPLICATE_DISK_NAME", cluster: null, project: null,
      disks: ds.map((d) => d.uuid),
      details: `${ds.length} Einträge heißen „${name}“. Gezählt wird nur der neueste. Sind es wirklich getrennte Platten, unter Datenträger bestätigen: ` +
        ds.map((d) => `${d.uuid.slice(0, 8)}… (Scan ${String(d.last_scan || "").slice(0, 10)}${d.stale ? ", nicht gezählt" : ""})`).join(", ") });
  }

  out.sort((a, b) => (b.date || "").localeCompare(a.date || "") || a.name.localeCompare(b.name));
  return { projects: out, disks: diskList, findings, overview: overview(out, diskList, findings),
           cleanup: cleanupPlan(out, diskList, findings, Date.now(), reserveMonths, legacyGb) };
}

function overview(projects, disks, findings) {
  const realDisks = disks.filter((d) => !d.fallback && !d.retired);
  const open = findings.filter((f) => f.status === "offen");
  const byPrio = { kritisch: 0, warnung: 0, info: 0 };
  for (const f of open) byPrio[f.priority]++;
  // Sicherungsgrad: nur-1× (gar keine zweite Kopie), teilweise, 2×, 3+× (nach Mindest-Kopien je Typ)
  const buckets = { einfach: 0, teilweise: 0, "2": 0, "3+": 0 };
  const bucketGb = { einfach: 0, teilweise: 0, "2": 0, "3+": 0 };
  for (const p of projects) {
    const k = p.copies >= 3 ? "3+" : p.copies >= 2 ? "2" : p.copies_max >= 2 ? "teilweise" : "einfach";
    buckets[k]++; bucketGb[k] += p.size_gb;
  }
  const tierGb = {};
  for (const d of realDisks) { const t = d.tier || "unbekannt"; tierGb[t] = (tierGb[t] || 0) + (d.used_gb || 0); }
  // je Stufe: Anzahl, belegt, Kapazität/frei (nur Platten mit bekannter Kapazität; veraltete Doubletten zählen nicht)
  const tb = (g) => Math.round(g / 10) / 100;
  // NAS-Freigaben (nas:NXTCLOUD/<Freigabe>) sind ein Gerät
  const tierStats = {}, seen = new Set();
  for (const d of realDisks.filter((x) => !x.stale)) {
    const t = d.tier || "unbekannt", dev = d.uuid.startsWith("nas:") ? d.uuid.split("/")[0] : d.uuid;
    const s = (tierStats[t] ||= { disks: 0, used_gb: 0, cap_gb: 0, free_gb: 0, cap_known: 0, fill_used_gb: 0 });
    if (!seen.has(dev)) { seen.add(dev); s.disks++; }
    s.used_gb += d.used_gb || 0;
    if (d.capacity_gb) { s.cap_known++; s.cap_gb += d.capacity_gb; s.free_gb += d.free_gb || 0; s.fill_used_gb += d.used_gb || 0; }
  }
  const unsecured = projects.filter((p) => p.missing_gb > 0);
  return {
    disks: realDisks.length,
    used_tb: Math.round(realDisks.reduce((s, d) => s + (d.used_gb || 0), 0) / 10) / 100,
    capacity_known: realDisks.filter((d) => d.capacity_gb).length,
    projects: projects.length,
    ok: projects.filter((p) => p.ok).length,
    copies: buckets, copies_tb: Object.fromEntries(Object.entries(bucketGb).map(([k, v]) => [k, Math.round(v / 10) / 100])),
    unsecured_tb: Math.round(unsecured.reduce((s, p) => s + p.missing_gb, 0) / 10) / 100,
    unsecured_full: projects.filter((p) => p.level === "einfach").length,
    disks_defect: realDisks.filter((d) => d.health?.verdict === "defekt").length,
    disks_warn: realDisks.filter((d) => d.health?.verdict === "warnung").length,
    disks_checked: realDisks.filter((d) => d.health).length,
    unsecured_partial: projects.filter((p) => p.level === "teilweise").length,
    savings_tb: Math.round(open.filter((f) => ["EXCESS_COPIES", "PARTIAL_COPY", "REF_EXTRA"].includes(f.type)).reduce((s, f) => s + (f.savings_gb || 0), 0) / 10) / 100,
    tier_stats: Object.fromEntries(Object.entries(tierStats).map(([k, s]) => {
      if (k === "NAS" && !s.cap_known) { s.cap_gb = NAS_CAP_TB * 1000; s.free_gb = Math.max(0, s.cap_gb - s.used_gb); s.cap_known = s.disks; s.fill_used_gb = s.used_gb; }
      const price = PRICE_EUR_PER_TB[k];
      return [k, { disks: s.disks, used_tb: tb(s.used_gb), cap_tb: tb(s.cap_gb), free_tb: tb(s.free_gb), cap_known: s.cap_known,
                   fill: s.cap_gb ? Math.min(1, s.fill_used_gb / s.cap_gb) : null,   // nur Platten mit bekannter Kapazität
                   value_eur: price ? Math.round((tb(s.cap_gb) || tb(s.used_gb)) * price / 100) * 100 : null }];
    })),
    prices_eur_per_tb: PRICE_EUR_PER_TB,
    findings: byPrio, tier_tb: Object.fromEntries(Object.entries(tierGb).map(([k, v]) => [k, Math.round(v / 10) / 100])),
    next: unsecured.sort((a, b) => b.missing_gb - a.missing_gb).slice(0, 8)
      .map((p) => ({ id: p.id, name: `${p.date} ${p.name}`, size_gb: p.size_gb, missing_gb: p.missing_gb, level: p.level,
                     copies: p.copies, min: p.sicherung_min, disks: p.disks,
                     missing_types: p.types.filter((t) => t.core && t.disks < t.min).map((t) => t.type) })),
  };
}


// ── Aufräum-Plan ──────────────────────────────────────────────────
// Eine Kopie zählt nur, wenn sie mind. 95 % der vollständigsten Kopie hat (sonst "teilweise").
export const FULL_COPY_RATIO = 0.95;
// Gesichert = Rohmaterial (FOOTAGE/PHOTOS; PROJECT = nicht aufgelöster Gesamtordner) erfüllt die Regel.
// WORKING/PROXIES/BTS: sollten mit, entscheiden aber nicht über "gesichert".
export const CORE_TYPES = new Set(["FOOTAGE", "PHOTOS", "PROJECT"]);
const SMALL_COPY_GB = 1;             // darunter (leere/winzige Ordner) zählt jede Kopie
export const STALE_DAYS = 60;        // älter = für den Überblick neu scannen
// Phase 1 = einmal alles mit dem neuen Scanner (Datei-Katalog) erfassen: alles davor gilt als offen
export const PHASE1_SINCE = "2026-10-22T00:00:00+02:00";
const MARGIN = 0.05;                 // Ziele nie über 95 % füllen
const SMALL_TB = 6;                  // Platten bis zu dieser Größe kommen in den Runden zuletzt
const PORTS = 3;                     // Anschlüsse; ein Gehäuse mit mehreren Platten belegt nur einen
// Neupreis je TB Kapazität (Stand 10/2026, grob): externe HDD ~20 €, T7-Shield-Klasse ~90 €,
// NAS inkl. Gehäuse und Paritätsplatte ~60 € je nutzbarem TB. Für „Wert“ und „Kosten je GB“ in der Übersicht.
export const PRICE_EUR_PER_TB = { HDD: 20, SSD: 90, NAS: 60 };
// Nutzbare NAS-Kapazität (df auf der Freigabe: 87 TiB); über SMB kommt pro Freigabe keine verlässliche Zahl an
export const NAS_CAP_TB = 95.7;
export const DEFAULT_RESERVE_MONTHS = 3;   // Platz für neue Drehs, die in dieser Zeit dazukommen (einstellbar)

/**
 * Plan in 4 Phasen, live aus den Daten:
 * 1 Überblick: jede Platte (und das NAS) aktuell gescannt
 * 2 Entscheiden: Projekte, die ihre Regel nicht erfüllen und noch keine Entscheidung haben
 * 3 Vervollständigen: Kopieraufgaben (nur entschiedene 2×/3×-Projekte), gebündelt in Anschluss-Runden
 * 4 Freigeben: Platz aus überzähligen Kopien und "Löschbar"-Projekten
 */
export function cleanupPlan(projects, disks, findings, now = Date.now(), reserveMonths = DEFAULT_RESERVE_MONTHS, legacyGb = 0) {
  const real = disks.filter((d) => !d.fallback && !d.stale);
  const ageDays = (d) => (d.last_scan ? (now - Date.parse(d.last_scan)) / 86400000 : Infinity);

  // Phase 1: Überblick
  const phase1Since = Date.parse(PHASE1_SINCE);
  const toScan = real.filter((d) => ageDays(d) > STALE_DAYS || !d.last_scan || Date.parse(d.last_scan) < phase1Since)
    .sort((a, b) => ageDays(b) - ageDays(a));
  const hasNas = real.some((d) => d.tier === "NAS");
  const byHolder = new Map();
  for (const d of toScan) {
    const h = d.holder || "unbekannt";
    if (!byHolder.has(h)) byHolder.set(h, []);
    byHolder.get(h).push(d);
  }
  // Runden: je Halter:in gebündelt, max. PORTS Anschlüsse; Platten eines Gehäuses = ein Anschluss
  // (dann werden auch die aktuellen Platten des Gehäuses mitgescannt – sie hängen ja eh dran)
  const scanRounds = [];
  const pack = (d) => ({ uuid: d.uuid, name: d.name, tier: d.tier, enclosure: d.enclosure || null, capacity_gb: d.capacity_gb,
    last_scan: d.last_scan, age_days: Number.isFinite(ageDays(d)) ? Math.round(ageDays(d)) : null });
  for (const [holder, ds] of [...byHolder].sort((a, b) => b[1].length - a[1].length)) {
    const units = new Map();
    for (const d of ds) {
      const key = d.enclosure ? `enc:${d.enclosure}` : `disk:${d.uuid}`;
      if (!units.has(key)) units.set(key, []);
      units.get(key).push(d);
    }
    // Große Kaliber zuerst: Einheiten nach Kapazität (Gehäuse = Summe), kleine (<= SMALL_TB) ans Ende
    const capOf = (u) => u.reduce((x, d) => x + (d.capacity_gb || d.used_gb || 0), 0);
    const list = [...units.values()].sort((a, b) => capOf(b) - capOf(a));
    for (let i = 0; i < list.length; i += PORTS) {
      const group = list.slice(i, i + PORTS);
      const capGb = group.reduce((x, u) => x + capOf(u), 0);
      scanRounds.push({ holder, ports: group.length, cap_gb: Math.round(capGb),
                        small: group.every((u) => capOf(u) <= SMALL_TB * 1000), disks: group.flat().map(pack) });
    }
  }
  scanRounds.sort((a, b) => (a.small - b.small) || b.cap_gb - a.cap_gb);
  const phase1 = { total: real.length + (hasNas ? 0 : 1), fresh: real.length - toScan.length,
                   nas_missing: !hasNas, rounds: scanRounds };

  // Phase 2: Entscheiden
  const open = projects.filter((p) => !p.ok && !p.decided);
  const phase2 = { open: open.length, open_gb: round(open.reduce((s, p) => s + p.size_gb, 0)),
                   decided: projects.filter((p) => p.decided).length };

  // Phase 3: Kopieraufgaben (nur bewusst entschiedene Projekte mit >= 2 Kopien)
  const free = new Map(real.filter((d) => d.capacity_gb && d.free_gb != null)
    .map((d) => [d.uuid, Math.max(0, d.free_gb - d.capacity_gb * MARGIN)]));
  const targetsPref = real.filter((d) => free.has(d.uuid) && ["HDD", "NAS"].includes(d.tier));
  const tasks = [];
  for (const p of projects) {
    if (!p.decided || p.ok) continue;
    for (const t of p.types) {
      if (!t.core || t.min < 2 || t.disks >= t.min || !t.copies.length) continue;
      tasks.push({ project: p.id, name: `${p.date} ${p.name}`, type: t.type, gb: t.size_gb,
                   source: t.copies[0].disk_uuid, source_name: t.copies[0].disk,
                   has: new Set(t.copies.map((c) => c.disk_uuid)), need: t.min - t.disks });
    }
  }
  // Je Quellplatte eine Runde; Ziele mit dem meisten Platz zuerst, Platz wird reserviert
  const bySource = new Map();
  for (const t of tasks) { if (!bySource.has(t.source)) bySource.set(t.source, []); bySource.get(t.source).push(t); }
  const copyRounds = [];
  let unplacedGb = 0;
  for (const [src, ts] of [...bySource].sort((a, b) => sum(b[1]) - sum(a[1]))) {
    const assign = new Map();
    for (const t of ts.sort((a, b) => b.gb - a.gb)) {
      for (let k = 0; k < t.need; k++) {
        const cand = targetsPref.filter((d) => d.uuid !== src && !t.has.has(d.uuid) && free.get(d.uuid) >= t.gb)
          .sort((a, b) => (assign.has(b.uuid) - assign.has(a.uuid)) || free.get(b.uuid) - free.get(a.uuid))[0];
        if (!cand) { unplacedGb += t.gb; continue; }
        free.set(cand.uuid, free.get(cand.uuid) - t.gb);
        t.has.add(cand.uuid);
        if (!assign.has(cand.uuid)) assign.set(cand.uuid, []);
        assign.get(cand.uuid).push({ name: t.name, type: t.type, gb: t.gb, project: t.project });
      }
    }
    if (assign.size) copyRounds.push({ source: { uuid: src, name: ts[0].source_name },
      targets: [...assign].map(([u, items]) => ({ uuid: u, name: real.find((d) => d.uuid === u)?.name || u,
        gb: round(items.reduce((s, i) => s + i.gb, 0)), items })),
      gb: round([...assign.values()].flat().reduce((s, i) => s + i.gb, 0)) });
  }
  const phase3 = { tasks: tasks.length, gb: round(tasks.reduce((s, t) => s + t.gb * t.need, 0)),
                   rounds: copyRounds, unplaced_gb: round(unplacedGb) };

  // Phase 4: Freigeben
  const sumOpen = (type) => findings.filter((f) => f.type === type && f.status === "offen").reduce((s, f) => s + (f.savings_gb || 0), 0);
  const excessGb = sumOpen("EXCESS_COPIES"), partialGb = sumOpen("PARTIAL_COPY"), refExtraGb = sumOpen("REF_EXTRA");
  // Löschbar / 1× reicht gelten je Ordnertyp (Projektregel oder Typ-Regel, z. B. nur PHOTOS löschbar)
  const deletable = projects.filter((p) => p.types.some((t) => t.sicherung === "loeschbar"));
  const deletableGb = projects.reduce((s, p) => s + p.types.filter((t) => t.sicherung === "loeschbar")
    .reduce((x, t) => x + t.copies.reduce((y, c) => y + c.size_gb, 0), 0), 0);
  const singleExtraGb = projects.reduce((s, p) => s + p.types.filter((t) => t.sicherung === "einfach")
    .reduce((x, t) => x + Math.max(0, t.disks - 1) * t.size_gb, 0), 0);
  const phase4 = { excess_gb: round(excessGb), partial_gb: round(partialGb), ref_extra_gb: round(refExtraGb),
                   deletable_gb: round(deletableGb), single_extra_gb: round(singleExtraGb), legacy_gb: round(legacyGb),
                   total_gb: round(excessGb + partialGb + refExtraGb + deletableGb + singleExtraGb + legacyGb), deletable_projects: deletable.length };

  // Platzbilanz (Ziel-Platz auf HDD/NAS ohne 5 % Reserve)
  const freeHdd = real.filter((d) => ["HDD", "NAS"].includes(d.tier) && d.free_gb != null)
    .reduce((s, d) => s + Math.max(0, d.free_gb - (d.capacity_gb || 0) * MARGIN), 0);
  const needAll = projects.filter((p) => !p.ok && p.sicherung_min >= 2).reduce((s, p) => s + (p.missing_gb || 0), 0);
  // Reserve für neue Drehs: Zuwachs der letzten 6 Monate (nach Projektdatum) × RESERVE_MONTHS, doppelt gesichert
  const since = now - 182 * 86400000;
  const recentGb = projects.filter((p) => p.date && Date.parse(p.date + "T00:00:00Z") >= since && Date.parse(p.date + "T00:00:00Z") <= now)
    .reduce((s, p) => s + p.size_gb, 0);
  const perMonth = recentGb / 6;
  const reserveGb = perMonth * reserveMonths * 2;
  const targetGb = needAll + reserveGb;
  const balance = { need_gb: round(needAll), need_decided_gb: phase3.gb, free_gb: round(freeHdd), freeable_gb: phase4.total_gb,
                    intake_gb_month: round(perMonth), reserve_months: reserveMonths, reserve_gb: round(reserveGb),
                    target_gb: round(targetGb),
                    shortfall_gb: round(Math.max(0, targetGb - freeHdd - phase4.total_gb)) };

  const phase = phase1.fresh < phase1.total ? 1 : phase2.open ? 2 : phase3.tasks ? 3 : phase4.total_gb > 0 ? 4 : 5;
  return { phase, phase1, phase2, phase3, phase4, balance, stale_days: STALE_DAYS, ports: PORTS };
}
const round = (g) => Math.round(g * 100) / 100;
const sum = (ts) => ts.reduce((s, t) => s + t.gb, 0);
