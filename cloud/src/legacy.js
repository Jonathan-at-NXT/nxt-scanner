// Altprojekte von Nils: Ordner, die mit einer 1–3-stelligen Nummer beginnen ("113-Richemont Event München").
// Erkannt aus folder_tree (bis Ebene 3), Unterordner normaler Projekte und Standard-Unterordner ("06-EXPORT") ausgenommen.

const PAT = /^(\d{1,3})(?!\d)\s*[-_]\s*(.+)$/;
const DATED = /^\d{4}/;
const SUB = new Set(["EXPORT", "EXPORTS", "PHOTOS", "PHOTO", "ASSETS", "AUDIO", "RESOLVE", "LOGO", "LOGOS", "SOFTWARE", "FOOTAGE",
  "RAW", "PREMIERE", "LIGHTROOM", "SOUND", "VIDEO", "VIDEOS", "MUSIK", "MUSIC", "GRAFIK", "GRAFIKEN", "SCRIPT", "PROJEKT", "PROJECT",
  "DOKUMENTE", "DOCS", "FONTS", "PROXY", "PROXIES", "PROXYS", "RENDER", "RENDERS", "CUT", "EDIT", "COLOR", "VFX", "AE", "AFTER EFFECTS",
  "DAVINCI", "FCP", "STILLS", "BILDER", "TEXT", "SFX", "GFX", "INPUT", "OUTPUT", "BACKUP", "OFFLINE", "ROHMATERIAL", "ARCHIV",
  "CLEANCUTS", "INTERMEDIATES", "OVERLAYS", "PROJECTS", "TIMELINES", "TIMELINE"]);
// Schnitt-/Exportstrukturen ("2-TIMELINE-EXPORTS_PRORES") sind keine Altprojekte
const STRUCT = /^(TIMELINE|EXPORT|RENDER|PRORES|PROXY|INTERMEDIATE|OVERLAY|CLEANCUT)/;
export const MIN_GB = 5;
const normName = (s) => s.toUpperCase().normalize("NFKD").replace(/[̀-ͯ]/g, "").replace(/[^A-Z0-9]/g, "").slice(0, 12);

export function legacyProjects(rows, disks) {
  // nur der neueste Datenträger je Name zählt (Doubletten), name:-Fallbacks raus
  const newest = new Map();
  for (const d of disks) {
    if (d.uuid.startsWith("name:")) continue;
    const cur = newest.get(d.name);
    if (!cur || (d.last_scan || "") > (cur.last_scan || "")) newest.set(d.name, d);
  }
  const live = new Map([...newest.values()].map((d) => [d.uuid, d]));
  const hits = [];
  for (const r of rows) {
    if (!live.has(r.disk_uuid)) continue;
    const parts = r.rel_path.split("/");
    const name = parts[parts.length - 1].trim();
    const m = PAT.exec(name);
    if (!m) continue;
    const rest = m[2].trim().toUpperCase();
    if (SUB.has(rest) || STRUCT.test(rest) || /^(TAG|DAY|CAM|KAMERA|CARD|KARTE|[A-D])\s*\d*$/.test(rest) || /^\d+$/.test(rest)) continue;
    if (parts.slice(0, -1).some((p) => DATED.test(p) || PAT.test(p.trim()))) continue;
    hits.push({ ...r, num: Number(m[1]), title: m[2].trim() });
  }
  const groups = new Map();
  for (const h of hits) {
    const key = `${h.num}|${normName(h.title)}`;
    if (!groups.has(key)) groups.set(key, { key, num: h.num, title: h.title, copies: [] });
    const d = live.get(h.disk_uuid);
    groups.get(key).copies.push({ disk_uuid: h.disk_uuid, disk: d.name, path: h.rel_path, size_gb: Math.round((h.size_bytes || 0) / 1e7) / 100,
                                  files: h.file_count, last_scan: d.last_scan });
  }
  const out = [];
  for (const g of groups.values()) {
    const max = Math.max(...g.copies.map((c) => c.size_gb));
    if (max < MIN_GB) continue;
    g.size_gb = max;
    g.total_gb = Math.round(g.copies.reduce((s, c) => s + c.size_gb, 0) * 100) / 100;
    g.disks = [...new Set(g.copies.map((c) => c.disk))].sort((a, b) => a.localeCompare(b, "de", { numeric: true }));
    g.full_copies = new Set(g.copies.filter((c) => c.size_gb >= max * 0.95).map((c) => c.disk_uuid)).size;
    out.push(g);
  }
  return out.sort((a, b) => a.num - b.num);
}

export const LEGACY_DECISIONS = { aussortieren: "Aussortieren", einfach: "Nur 1×", behalten: "Behalten (2×)", ansehen: "Nochmal ansehen",
  clemens: "An Clemens geben", paul: "An Paul geben", kein: "Kein Altprojekt" };
// Übergabe an Personen: eigene Seite /uebergabe/<key> je Person
export const HANDOVER = { clemens: "Clemens", paul: "Paul" };

// Projekte mit Datum bis einschließlich Stichmonat (aus der Projektliste) zählen ebenfalls als Altprojekte.
// Der Stichmonat ist im Dashboard wählbar ("YYYY-MM"), Standard Ende 2023.
export const DEFAULT_CUTOFF = "2023-12";
export const validCutoff = (v) => (/^\d{4}-(0[1-9]|1[0-2])$/.test(String(v || "")) ? String(v) : null);
export function oldProjects(projects, disks, cutoff = DEFAULT_CUTOFF) {
  const live = new Set();
  const newest = new Map();
  for (const d of disks) {
    if (d.uuid.startsWith("name:")) continue;
    const cur = newest.get(d.name);
    if (!cur || (d.last_scan || "") > (cur.last_scan || "")) newest.set(d.name, d);
  }
  for (const d of newest.values()) live.add(d.uuid);
  const name = new Map(disks.map((d) => [d.uuid, d.name]));
  const byCl = new Map();
  for (const p of projects) {
    if (p.status !== "validated" || !p.cluster_id || !p.date || p.date.slice(0, 7) > cutoff || !live.has(p.disk_uuid)) continue;
    if (!byCl.has(p.cluster_id)) byCl.set(p.cluster_id, { rows: [], names: new Set(), date: p.date });
    const g = byCl.get(p.cluster_id);
    g.rows.push(p); g.names.add(p.project_name);
  }
  const out = [];
  for (const [cl, g] of byCl) {
    const perDisk = new Map();
    for (const r of g.rows) perDisk.set(r.disk_uuid, (perDisk.get(r.disk_uuid) || 0) + (r.size_bytes || 0) / 1e9);
    const paths = new Map();
    for (const r of g.rows) paths.set(r.disk_uuid, [...(paths.get(r.disk_uuid) || []), r.rel_path]);
    const max = Math.max(...perDisk.values());
    if (max < MIN_GB) continue;
    const title = [...g.names].sort((a, b) => b.length - a.length)[0];
    out.push({ key: `p|${cl}`, num: null, date: g.date, title, rule_key: cl, size_gb: Math.round(max * 100) / 100,
               total_gb: Math.round([...perDisk.values()].reduce((s, x) => s + x, 0) * 100) / 100,
               copies: [...perDisk].map(([u, gbv]) => ({ disk_uuid: u, disk: name.get(u) || "?", size_gb: Math.round(gbv * 100) / 100,
                                                         path: (paths.get(u) || []).join(" + ") })),
               disks: [...new Set([...perDisk.keys()].map((u) => name.get(u) || "?"))].sort((a, b) => a.localeCompare(b, "de", { numeric: true })),
               full_copies: [...perDisk.values()].filter((x) => x >= max * 0.95).length, source: "projekt" });
  }
  return out;
}
