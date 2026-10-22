/**
 * Notion-Sync aus D1 (ersetzt den Notion-Sync im Scanner).
 *
 * Quelle der Wahrheit ist D1 (disks + folder_tree), befüllt nur von
 * vollständigen Scans. Der Worker spiegelt daraus wie bisher der Scanner:
 * - "Datenträger"-DB (HDD_DB_ID): eine Seite je Platte
 * - "Speicherungen"-DB (SPEICHERUNGEN_DB_ID): eine Seite je Top-Level-Ordner,
 *   bei PROJECT-Ordnern zusätzlich die Unterordner (Relation "Projekt")
 * Status "Manuell" wird respektiert (nur Größe/Dateien/Pfad/Zeiten), Ordner,
 * die nicht mehr auf der Platte liegen, werden archiviert. Gepatcht wird nur,
 * was sich geändert hat ("Letzter Scan" zählt tagesgenau).
 *
 * Fortschritt je Platte: Tabelle notion_sync_state(disk_uuid, synced_scan, ...).
 */

const NOTION = "https://api.notion.com/v1";

// ── Namenskonvention (Port von storage_scanner/rules.py) ──────────────
const TYPE_ALIASES = {
  FOOTAGE: "FOOTAGE", VIDEO: "FOOTAGE", VIDEOS: "FOOTAGE",
  PHOTOS: "PHOTOS", PHOTO: "PHOTOS", FOTOS: "PHOTOS", FOTO: "PHOTOS",
  WORKING: "WORKING", BTS: "BTS",
  PROXIES: "PROXIES", PROXY: "PROXIES", PROXYS: "PROXIES",
};
const TYPE_ALT = Object.keys(TYPE_ALIASES).join("|");
const PATTERN = new RegExp(`^(\\d{4}-\\d{2}-\\d{2})[_-](.+?)[_-]\\s*(${TYPE_ALT})\\s*$`, "i");
const PROJECT_PATTERN = /^(\d{4}-\d{2}-\d{2})[_-](.+)$/;

function validDate(s) {
  const [y, m, d] = s.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}

export function validateFolder(name) {
  let m = PATTERN.exec(name);
  if (m) {
    if (!validDate(m[1])) return null;
    const raw = m[3].toUpperCase();
    return { date: m[1], project_name: m[2].trim(), type: TYPE_ALIASES[raw] || raw };
  }
  m = PROJECT_PATTERN.exec(name);
  if (m) {
    if (!validDate(m[1])) return null;
    const project_name = m[2].trim();
    const last = project_name.split("_").pop().split("-").pop().toUpperCase();
    if (!(last in TYPE_ALIASES)) return { date: m[1], project_name, type: "PROJECT" };
  }
  return null;
}

// Python: round(bytes / 1000**3, 2)
export const bytesToGb = (b) => Math.round(((b || 0) / 1e9) * 100) / 100;

// ── Sollzustand je Platte aus D1 ──────────────────────────────────────
/**
 * disk: Zeile aus disks; nodes: folder_tree-Zeilen dieser Platte.
 * Gibt {hdd, folders:[{name, valid, props, manualProps, children:[...]}]} zurück.
 */
export function buildDiskView(disk, nodes) {
  const byParent = new Map();
  const top = [];
  for (const n of nodes) {
    if (n.depth === 1) top.push(n);
    else if (n.depth === 2) {
      const list = byParent.get(n.parent_rel_path) || [];
      list.push(n);
      byParent.set(n.parent_rel_path, list);
    }
  }
  top.sort((a, b) => a.rel_path.localeCompare(b.rel_path));
  const scanDate = disk.last_scan || "";
  const fallbackPath = (n) => `/Volumes/${disk.name}/${n.rel_path}`;
  const lastName = (rel) => rel.split("/").pop();

  // Felder, die D1 nicht sicher kennt (alte Scanner ohne abs_path), werden nicht
  // angefasst; nur neu angelegte Seiten bekommen einen Ersatzpfad (siehe syncDisk).
  const dynamic = (n) => {
    const p = {
      "Größe (GB)": { number: bytesToGb(n.size_bytes) },
      "Dateien": { number: n.file_count ?? 0 },
    };
    if (n.abs_path) p["Absoluter Pfad"] = { rich_text: [{ text: { content: n.abs_path } }] };
    if (scanDate) p["Letzter Scan"] = { date: { start: scanDate } };
    if (n.mtime) p["Letzte Änderung"] = { date: { start: n.mtime.slice(0, 10) } };
    return p;
  };

  let valid = 0;
  let unassigned = 0;
  const folders = [];
  for (const n of top) {
    const name = lastName(n.rel_path);
    const v = validateFolder(name);
    const props = {
      "Name": { title: [{ text: { content: name } }] },
      ...dynamic(n),
    };
    if (v) {
      valid++;
      props["Projektname"] = { rich_text: [{ text: { content: v.project_name } }] };
      props["Typ"] = { select: { name: v.type } };
      props["Status"] = { select: { name: "Valid" } };
      props["Datum"] = { date: { start: v.date } };
    } else {
      unassigned++;
      props["Projektname"] = { rich_text: [{ text: { content: "" } }] };
      props["Status"] = { select: { name: "Unassigned" } };
    }
    const children = [];
    if (v && v.type === "PROJECT") {
      for (const c of (byParent.get(n.rel_path) || []).sort((a, b) => a.rel_path.localeCompare(b.rel_path))) {
        const cname = lastName(c.rel_path);
        const cv = validateFolder(cname);
        const cprops = { "Name": { title: [{ text: { content: cname } }] }, ...dynamic(c) };
        if (cv) {
          cprops["Typ"] = { select: { name: cv.type } };
          cprops["Status"] = { select: { name: "Valid" } };
          cprops["Projektname"] = { rich_text: [{ text: { content: cv.project_name } }] };
          cprops["Datum"] = { date: { start: cv.date } };
        } else {
          cprops["Status"] = { select: { name: "Unassigned" } };
        }
        children.push({ name: cname, relPath: c.rel_path, props: cprops, manualProps: dynamic(c), fallbackPath: fallbackPath(c) });
      }
    }
    folders.push({ name, relPath: n.rel_path, valid: !!v, props, manualProps: dynamic(n), children, fallbackPath: fallbackPath(n) });
  }

  const hdd = {
    "Name": { title: [{ text: { content: disk.name || "" } }] },
    "Gültige Projekte": { number: valid },
    "Unassigned": { number: unassigned },
    "Volume UUID": { rich_text: [{ text: { content: disk.uuid.startsWith("name:") ? "" : disk.uuid } }] },
    "Zuletzt genutzt von": disk.last_user ? { select: { name: disk.last_user } } : { select: null },
  };
  if (scanDate) hdd["Letzter Scan"] = { date: { start: scanDate } };
  // Kapazität/Belegung nur, wenn ein neuer Scanner sie geliefert hat (sonst stünde dort 0)
  if (disk.capacity_bytes) hdd["Kapazität (GB)"] = { number: bytesToGb(disk.capacity_bytes) };
  if (disk.fs_used_bytes != null) hdd["Belegt (GB)"] = { number: bytesToGb(disk.fs_used_bytes) };
  return { hdd, folders };
}

// ── Vergleich Ist (Notion-Seite) gegen Soll ───────────────────────────
function plain(prop) {
  if (!prop) return null;
  switch (prop.type) {
    case "title": return (prop.title || []).map((t) => t.plain_text).join("");
    case "rich_text": return (prop.rich_text || []).map((t) => t.plain_text).join("");
    case "number": return prop.number;
    case "select": return prop.select ? prop.select.name : null;
    case "date": return prop.date ? (prop.date.start || "").slice(0, 10) : null;
    case "relation": return (prop.relation || []).map((r) => r.id.replace(/-/g, "")).sort().join(",");
    default: return undefined;
  }
}

function wanted(val) {
  if ("title" in val) return val.title.map((t) => t.text.content).join("");
  if ("rich_text" in val) return val.rich_text.map((t) => t.text.content).join("");
  if ("number" in val) return val.number;
  if ("select" in val) return val.select ? val.select.name : null;
  if ("date" in val) return val.date ? (val.date.start || "").slice(0, 10) : null;
  if ("relation" in val) return val.relation.map((r) => r.id.replace(/-/g, "")).sort().join(",");
  return undefined;
}

/** Nur die Properties, die sich wirklich unterscheiden. */
export function changedProps(page, props) {
  const out = {};
  for (const [k, v] of Object.entries(props)) {
    const cur = plain(page?.properties?.[k]);
    if (cur === undefined || cur !== wanted(v)) out[k] = v;
  }
  return out;
}

// ── Notion-Client (gedrosselt, 429/5xx mit Retry) ─────────────────────
export function makeNotion(token, { fetchImpl = fetch, gapMs = 340, sleep } = {}) {
  const wait = sleep || ((ms) => new Promise((r) => setTimeout(r, ms)));
  let last = 0;
  const stats = { calls: 0 };
  async function call(method, path, body) {
    for (let attempt = 0; ; attempt++) {
      const dt = Date.now() - last;
      if (dt < gapMs) await wait(gapMs - dt);
      last = Date.now();
      stats.calls++;
      const resp = await fetchImpl(`${NOTION}/${path}`, {
        method,
        headers: {
          Authorization: `Bearer ${token}`,
          "Notion-Version": "2022-06-28",
          "Content-Type": "application/json",
        },
        body: body ? JSON.stringify(body) : undefined,
      });
      if (resp.ok) return resp.json();
      if ((resp.status === 429 || resp.status >= 500) && attempt < 4) {
        const ra = Number(resp.headers?.get?.("retry-after")) || 2 ** attempt;
        await wait(ra * 1000);
        continue;
      }
      const text = await resp.text();
      throw new Error(`Notion ${method} ${path}: ${resp.status} ${text.slice(0, 300)}`);
    }
  }
  async function queryAll(dbId, filter) {
    const out = [];
    let cursor;
    do {
      const body = { page_size: 100, ...(filter ? { filter } : {}), ...(cursor ? { start_cursor: cursor } : {}) };
      const data = await call("POST", `databases/${dbId}/query`, body);
      out.push(...data.results);
      cursor = data.has_more ? data.next_cursor : undefined;
    } while (cursor);
    return out;
  }
  return { call, queryAll, stats };
}

// Container, die der Scanner auf oberster Ebene "durchsichtig" macht (scan.py)
const CONTAINERS = new Set(["NXT STUDIOS", "OLDER"]);

/** "/Volumes/TOWER 7/NXT STUDIOS/X/VIDEO" -> "X/VIDEO" (wie folder_tree.rel_path) */
export function relFromAbsPath(abs) {
  const m = /^\/Volumes\/[^/]+\/(.+)$/.exec(abs || "");
  if (!m) return "";
  const parts = m[1].split("/");
  if (parts.length > 1 && CONTAINERS.has(parts[0].toUpperCase())) parts.shift();
  return parts.join("/");
}

const titleOf = (page) => (page.properties?.Name?.title || []).map((t) => t.plain_text).join("");

// ── Eine Platte spiegeln ──────────────────────────────────────────────
/**
 * Spiegelt eine Platte nach Notion. dry=true schreibt nichts und liefert nur
 * die geplanten Operationen. Gibt {hdd_page_id, created, updated, archived, unchanged, ops} zurück.
 */
export async function syncDisk(notion, ids, disk, nodes, { dry = false } = {}) {
  const view = buildDiskView(disk, nodes);
  const res = { created: 0, updated: 0, archived: 0, unchanged: 0, ops: [] };
  const op = (kind, name, props) => res.ops.push({ kind, name, fields: props ? Object.keys(props) : [] });

  // Datenträger-Seite: per Titel (wie bisher)
  const hddPages = await notion.queryAll(ids.hdd, { property: "Name", title: { equals: disk.name } });
  let hddId = hddPages[0]?.id;
  // Nie neuere Notion-Daten mit älterem D1-Stand überschreiben (z. B. Scanner ohne D1-Token)
  const notionScan = hddPages[0]?.properties?.["Letzter Scan"]?.date?.start;
  if (hddId && notionScan && disk.last_scan && Date.parse(notionScan) > Date.parse(disk.last_scan) + 60000) {
    return { ...res, skipped: "notion_newer", notion_scan: notionScan, hdd_page_id: hddId };
  }
  if (hddId) {
    const diff = changedProps(hddPages[0], view.hdd);
    if (Object.keys(diff).length) {
      op("update-hdd", disk.name, diff);
      if (!dry) await notion.call("PATCH", `pages/${hddId}`, { properties: diff });
      res.updated++;
    } else res.unchanged++;
  } else {
    op("create-hdd", disk.name, view.hdd);
    if (!dry) hddId = (await notion.call("POST", "pages", { parent: { database_id: ids.hdd }, properties: view.hdd })).id;
    else hddId = "dry-hdd";
    res.created++;
  }
  res.hdd_page_id = hddId;

  // Bestehende Speicherungen dieser Platte, eindeutig über den Ordnerpfad
  // ("Absoluter Pfad" ohne /Volumes/<Platte>/ und ohne Container wie NXT STUDIOS/),
  // weil gleichnamige Unterordner ("VIDEO") in mehreren Projekten vorkommen und die
  // Projekt-Relation in Notion in beide Richtungen zeigt. Seiten ohne Pfad: per Name.
  const byPath = new Map();
  const byTitle = new Map();
  if (!dry || hddId !== "dry-hdd") {
    for (const p of await notion.queryAll(ids.speicherungen, { property: "HDD", relation: { contains: hddId } })) {
      const rel = relFromAbsPath(plain(p.properties?.["Absoluter Pfad"]) || "");
      if (rel) byPath.set(rel, p);
      else if (!byTitle.has(titleOf(p))) byTitle.set(titleOf(p), p);
    }
  }
  const used = new Set();

  async function upsert(name, relPath, props, manualProps, extra, fallback) {
    let page = byPath.get(relPath);
    if (!page || used.has(page.id)) {
      const t = byTitle.get(name);
      page = t && !used.has(t.id) ? t : undefined;
    }
    if (page) used.add(page.id);
    const isManual = page?.properties?.Status?.select?.name === "Manuell";
    const desired = isManual ? manualProps : { ...props, ...extra };
    if (!page && !desired["Absoluter Pfad"] && fallback) {
      desired["Absoluter Pfad"] = { rich_text: [{ text: { content: fallback } }] };
    }
    if (page) {
      const diff = changedProps(page, desired);
      if (Object.keys(diff).length) {
        op(isManual ? "update-manual" : "update", name, diff);
        if (!dry) await notion.call("PATCH", `pages/${page.id}`, { properties: diff });
        res.updated++;
      } else res.unchanged++;
      return page.id;
    }
    op("create", name, desired);
    res.created++;
    if (dry) return `dry-${name}`;
    return (await notion.call("POST", "pages", { parent: { database_id: ids.speicherungen }, properties: desired })).id;
  }

  const hddRel = { "HDD": { relation: [{ id: hddId }] } };
  for (const f of view.folders) {
    const parentId = await upsert(f.name, f.relPath, f.props, f.manualProps, hddRel, f.fallbackPath);
    for (const c of f.children) {
      await upsert(c.name, c.relPath, c.props, c.manualProps,
        { ...hddRel, "Projekt": { relation: [{ id: parentId }] } }, c.fallbackPath);
    }
  }

  for (const page of [...byPath.values(), ...byTitle.values()]) {
    if (!used.has(page.id)) {
      used.add(page.id);
      const name = titleOf(page);
      op("archive", name);
      if (!dry) await notion.call("PATCH", `pages/${page.id}`, { archived: true });
      res.archived++;
    }
  }
  return res;
}

// ── Lauf über alle geänderten Platten ─────────────────────────────────
export async function runNotionSync(env, { dry = false, onlyDisk = null, maxCalls = 900, notion = null } = {}) {
  const client = notion || makeNotion(env.NOTION_TOKEN);
  const ids = { hdd: env.HDD_DB_ID, speicherungen: env.SPEICHERUNGEN_DB_ID };
  if (!ids.hdd || !ids.speicherungen || !env.NOTION_TOKEN) throw new Error("Notion-Konfiguration fehlt");

  const q = onlyDisk
    ? env.DB.prepare(
        `SELECT d.* FROM disks d WHERE d.uuid=? OR d.name=?`).bind(onlyDisk, onlyDisk)
    : env.DB.prepare(
        `SELECT d.* FROM disks d LEFT JOIN notion_sync_state s ON s.disk_uuid=d.uuid
         WHERE d.uuid NOT LIKE 'name:%' AND d.last_scan IS NOT NULL
           AND (s.synced_scan IS NULL OR s.synced_scan <> d.last_scan)
         ORDER BY d.last_scan ASC`);
  const disks = (await q.all()).results || [];
  const report = { disks: [], pending: disks.length, calls: 0, skipped: [] };

  // Gleicher Plattenname, verschiedene UUIDs (Klon, Neuformatierung, Namensdoppel):
  // in Notion gibt es nur EINE Seite je Name -> nur die Platte mit dem neuesten Scan spiegelt.
  const newestByName = new Map(
    ((await env.DB.prepare(
      `SELECT name, uuid, MAX(last_scan) AS last_scan FROM disks
       WHERE uuid NOT LIKE 'name:%' AND last_scan IS NOT NULL GROUP BY name`).all()).results || [])
      .map((r) => [r.name, r.uuid]));

  for (const disk of disks) {
    if (client.stats.calls >= maxCalls) break; // Rest im nächsten Lauf
    const owner = newestByName.get(disk.name);
    if (owner && owner !== disk.uuid) {
      report.skipped.push({ name: disk.name, uuid: disk.uuid, reason: "duplicate_name_older", newer_uuid: owner });
      report.pending--;
      if (!dry) await markSynced(env, disk, null, "übersprungen: gleicher Name, neuere Platte " + owner);
      continue;
    }
    const nodes = (await env.DB.prepare(
      `SELECT rel_path, depth, parent_rel_path, size_bytes, file_count, mtime, abs_path
       FROM folder_tree WHERE disk_uuid=? AND depth IN (1,2)`).bind(disk.uuid).all()).results || [];
    try {
      const r = await syncDisk(client, ids, disk, nodes, { dry });
      if (r.skipped) {
        report.skipped.push({ name: disk.name, uuid: disk.uuid, reason: r.skipped, notion_scan: r.notion_scan });
        if (!dry) await markSynced(env, disk, r.hdd_page_id, "übersprungen: Notion neuer (" + r.notion_scan + ")");
      } else {
        report.disks.push({ name: disk.name, uuid: disk.uuid, ...r, ops: dry ? r.ops : undefined });
        if (!dry) await markSynced(env, disk, r.hdd_page_id, null);
      }
      report.pending--;
    } catch (e) {
      report.disks.push({ name: disk.name, uuid: disk.uuid, error: String(e) });
      if (!dry) {
        await env.DB.prepare(
          `INSERT INTO notion_sync_state(disk_uuid, last_error) VALUES(?,?)
           ON CONFLICT(disk_uuid) DO UPDATE SET last_error=excluded.last_error`
        ).bind(disk.uuid, String(e).slice(0, 500)).run();
      }
    }
  }
  report.calls = client.stats.calls;
  return report;
}

async function markSynced(env, disk, hddPageId, note) {
  await env.DB.prepare(
    `INSERT INTO notion_sync_state(disk_uuid, synced_scan, hdd_page_id, synced_at, last_error)
     VALUES(?,?,?,?,?)
     ON CONFLICT(disk_uuid) DO UPDATE SET synced_scan=excluded.synced_scan,
       hdd_page_id=COALESCE(excluded.hdd_page_id, notion_sync_state.hdd_page_id),
       synced_at=excluded.synced_at, last_error=excluded.last_error`
  ).bind(disk.uuid, disk.last_scan, hddPageId, new Date().toISOString(), note).run();
}
