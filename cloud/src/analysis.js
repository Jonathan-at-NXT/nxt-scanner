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
  INCOMPLETE_BACKUP: "warnung", EXCESS_COPIES: "info", UMBENENNEN: "info", DUPLICATE_DISK_NAME: "info" };

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
export function buildDisks(disks, metaRows, projects) {
  const meta = new Map(metaRows.map((m) => [m.disk_uuid, m]));
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
  for (const d of disks) {
    if (d.uuid.startsWith("name:")) continue;
    const cur = newest.get(d.name);
    if (!cur || (d.last_scan || "") > (cur.last_scan || "")) newest.set(d.name, d);
  }
  return disks.map((d) => {
    const m = meta.get(d.uuid) || {};
    const dup = (byName.get(d.name) || 0) > 1;
    const stale = dup && newest.get(d.name)?.uuid !== d.uuid && !m.separate_disk;
    const tier = m.tier || guessTier(d.name);
    const used = d.fs_used_bytes ?? d.used_bytes ?? null;
    const cap = d.capacity_bytes || null;
    return {
      uuid: d.uuid, name: d.name, tier, tier_source: m.tier ? (m.tier_source || "manual") : (tier ? "guess" : null),
      holder: m.holder || d.last_user || null, holder_source: m.holder ? "manual" : "scan",
      location: m.location || null, note: m.note || null,
      capacity_gb: cap ? gb(cap) : null, used_gb: used != null ? gb(used) : null,
      fill: cap && used != null ? Math.min(1, used / cap) : null,
      last_scan: d.last_scan, last_user: d.last_user,
      projects: projCount.get(d.uuid) || 0, project_gb: gb(validBytes.get(d.uuid) || 0),
      duplicate_name: dup, separate_disk: !!m.separate_disk,
      stale,                     // zählt nicht als Kopie, bis als eigene Platte bestätigt
      fallback: d.uuid.startsWith("name:"),
    };
  });
}

/** Projekte je Cluster inkl. Regel, Kopien je Typ, Befunde. */
export function analyze({ projects, disks, diskMeta = [], rules = [], states = [] }) {
  const diskList = buildDisks(disks, diskMeta, projects);
  const disk = new Map(diskList.map((d) => [d.uuid, d]));
  const ruleBy = new Map(rules.map((r) => [r.rule_key, r]));
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

    const byType = new Map();
    for (const m of members) {
      const t = m.type || "?";
      if (!byType.has(t)) byType.set(t, []);
      byType.get(t).push(m);
    }
    const types = [];
    let minCopies = Infinity, sizeGb = 0;
    for (const [t, rows] of [...byType].sort()) {
      const disksOf = [...new Set(rows.map((r) => r.disk_uuid))];
      const copies = rows.map((r) => ({
        disk_uuid: r.disk_uuid, disk: disk.get(r.disk_uuid)?.name || "?", tier: disk.get(r.disk_uuid)?.tier || null,
        holder: disk.get(r.disk_uuid)?.holder || null, last_scan: disk.get(r.disk_uuid)?.last_scan || null,
        rel_path: r.rel_path, name: r.name, soll_name: r.soll_name || null,
        size_gb: gb(r.size_bytes), files: r.file_count,
      })).sort((a, b) => a.disk.localeCompare(b.disk, "de", { numeric: true }));
      const sizes = copies.map((c) => c.size_gb);
      const maxGb = Math.max(...sizes);
      sizeGb += maxGb;
      minCopies = Math.min(minCopies, disksOf.length);
      types.push({ type: t, copies, disks: disksOf.length, size_gb: maxGb,
                   mismatch: disksOf.length >= 2 && new Set(sizes).size > 1 });

      const label = `${members[0].date} ${members[0].project_name}`;
      if (disksOf.length < min) {
        add({ key: `MISSING_BACKUP|${cid}|${t}`, type: "MISSING_BACKUP", cluster: cid, project: label, folder_type: t,
              disks: disksOf, size_gb: maxGb,
              details: disksOf.length ? `Nur auf ${copies.map((c) => c.disk).join(", ")} (${fg(maxGb)}), Soll: ${min}×`
                                      : `Keine Kopie, Soll: ${min}×` });
      }
      if (disksOf.length >= 2 && new Set(sizes).size > 1) {
        const diff = Math.round((Math.max(...sizes) - Math.min(...sizes)) * 100) / 100;
        add({ key: `SIZE_MISMATCH|${cid}|${t}`, type: "SIZE_MISMATCH", cluster: cid, project: label, folder_type: t,
              disks: disksOf, diff_gb: diff, size_gb: maxGb,
              details: copies.map((c) => `${c.disk}: ${fg(c.size_gb)}`).join(" vs ") + ` (Differenz ${fg(diff)})` });
      }
      if (disksOf.length > Math.max(min, 2)) {
        const extra = disksOf.length - Math.max(min, 2);
        add({ key: `EXCESS_COPIES|${cid}|${t}`, type: "EXCESS_COPIES", cluster: cid, project: label, folder_type: t,
              disks: disksOf, size_gb: maxGb, savings_gb: Math.round(maxGb * extra * 100) / 100,
              details: `${disksOf.length} Kopien (Soll ${Math.max(min, 2)}): ` + copies.map((c) => `${c.disk} (${fg(c.size_gb)})`).join(", ") });
      }
    }

    // Unvollständige Kopie: eine Platte hat das Projekt, aber nicht alle Ordnertypen
    const allTypes = new Set(byType.keys());
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
    const maxCopies = Math.max(0, ...types.map((t) => t.disks));
    // Fehlender Teil: nur Ordnertypen unter der Regel zählen (nicht das ganze Projekt)
    const missingGb = Math.round(types.filter((t) => t.disks < min).reduce((s, t) => s + t.size_gb, 0) * 100) / 100;
    // Sicherungsstand: einfach = kein Teil hat eine zweite Kopie, teilweise = gemischt
    const level = min <= 1 ? (copiesOverall >= min ? "ok" : "einfach")
      : copiesOverall >= min ? (copiesOverall >= 3 ? "mehrfach" : "ok")
      : maxCopies >= 2 ? "teilweise" : "einfach";
    out.push({
      id: cid, rule_key: rk || keys[0], date: members[0].date,
      name: members.map((m) => m.project_name).sort((a, b) => b.length - a.length)[0],
      aliases: [...new Set(members.map((m) => m.project_name))],
      types, size_gb: Math.round(sizeGb * 100) / 100,
      disks: [...new Set([...new Set(members.map((m) => m.disk_uuid))].map((u) => disk.get(u)?.name || u))],
      tiers: [...tiersPresent].sort(),
      copies: copiesOverall, copies_max: maxCopies, level, missing_gb: missingGb,
      sicherung, sicherung_min: min, require_tiers: requireTiers,
      rule_note: rule?.note || null, rule_by: rule?.updated_by || null,
      ok: copiesOverall >= min && requireTiers.every((t) => tiersPresent.has(t)),
    });
  }

  // Flotte: gleiche Namen bei verschiedenen Platten
  const seen = new Map();
  for (const d of diskList) if (!d.fallback) { if (!seen.has(d.name)) seen.set(d.name, []); seen.get(d.name).push(d); }
  for (const [name, ds] of seen) {
    if (ds.length > 1 && ds.some((d) => d.stale)) add({ key: `DUPLICATE_DISK_NAME|${name}`, type: "DUPLICATE_DISK_NAME", cluster: null, project: null,
      disks: ds.map((d) => d.uuid),
      details: `${ds.length} Einträge heißen „${name}“. Gezählt wird nur der neueste. Sind es wirklich getrennte Platten, unter Datenträger bestätigen: ` +
        ds.map((d) => `${d.uuid.slice(0, 8)}… (Scan ${String(d.last_scan || "").slice(0, 10)}${d.stale ? ", nicht gezählt" : ""})`).join(", ") });
  }

  out.sort((a, b) => (b.date || "").localeCompare(a.date || "") || a.name.localeCompare(b.name));
  return { projects: out, disks: diskList, findings, overview: overview(out, diskList, findings) };
}

function overview(projects, disks, findings) {
  const realDisks = disks.filter((d) => !d.fallback);
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
    unsecured_partial: projects.filter((p) => p.level === "teilweise").length,
    savings_tb: Math.round(open.filter((f) => f.type === "EXCESS_COPIES").reduce((s, f) => s + (f.savings_gb || 0), 0) / 10) / 100,
    findings: byPrio, tier_tb: Object.fromEntries(Object.entries(tierGb).map(([k, v]) => [k, Math.round(v / 10) / 100])),
    next: unsecured.sort((a, b) => b.missing_gb - a.missing_gb).slice(0, 8)
      .map((p) => ({ id: p.id, name: `${p.date} ${p.name}`, size_gb: p.size_gb, missing_gb: p.missing_gb, level: p.level,
                     copies: p.copies, min: p.sicherung_min, disks: p.disks,
                     missing_types: p.types.filter((t) => t.disks < p.sicherung_min).map((t) => t.type) })),
  };
}
