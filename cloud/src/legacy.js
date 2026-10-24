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

export const LEGACY_DECISIONS = { aussortieren: "Aussortieren", behalten: "Behalten", ansehen: "Nochmal ansehen", kein: "Kein Altprojekt" };
