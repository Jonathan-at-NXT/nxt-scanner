/**
 * Read-only Redundanz-Abfrage für das NXT Data MCP (Bearer INGEST_TOKEN).
 *
 *   POST /query/redundancy  {disk: "<Name oder UUID>", folder: "<rel_path auf dieser Platte>"}
 *
 * Darf der Ordner auf dieser Platte weg? Nur wenn ZUSÄTZLICH zur Quelle mindestens 2 andere
 * physische Geräte eine vollständige Kopie haben. Vollständig = jede Quelldatei liegt dort unter
 * einem Ordner mit gleichem Namen, mit gleichem Unterpfad und gleicher Größe. Im Zweifel: nicht löschen.
 *
 * - Geräte: alle nas:-Kennungen = ein Gerät „NAS“ (RAID ist kein Backup); gleichnamige Volumes = ein
 *   Gerät, außer disk_meta.separate_disk = 1; ausgemusterte Platten (disk_meta.retired_at) zählen nie.
 * - Proxies, 0-Byte-Dateien und Systemmüll (.DS_Store, ._*) müssen nicht kopiert sein.
 * - Vergleich nur über Name + Größe aus dem Datei-Katalog (nur Dateien ab 1 MB), Umlaute NFC/NFD egal.
 *   Das ist kein Inhaltsbeweis: „laut Katalog 2× vorhanden“, nicht „hash-verifiziert“.
 */

const PROXY = /(^|\/)[^/]*prox[^/]*\/|_(\d+_)?proxy\.[^/]+$/i;
const NEEDED = 2;
const nfc = (s) => String(s || "").normalize("NFC");
const junk = (name) => name === ".DS_Store" || name.startsWith("._");

export async function handleRedundancy(request, env, json) {
  let b;
  try { b = await request.json(); } catch { return json({ error: "invalid json" }, 400); }
  const diskRef = nfc(b?.disk).trim();
  const folder = nfc(b?.folder).trim().replace(/^\/+|\/+$/g, "");
  if (!diskRef || !folder) return json({ error: "disk und folder angeben" }, 400);
  const base = folder.split("/").pop();

  // ── Geräte ──────────────────────────────────────────────
  const disks = (await env.DB.prepare(
    `SELECT d.uuid, d.name, m.separate_disk, m.retired_at FROM disks d LEFT JOIN disk_meta m ON m.disk_uuid = d.uuid
     WHERE d.uuid NOT LIKE 'name:%'`).all()).results || [];
  const info = new Map(disks.map((d) => [d.uuid, d]));
  const device = (uuid) => {
    if (uuid.startsWith("nas:")) return "NAS";
    const d = info.get(uuid);
    if (!d) return null;                                   // unbekannte Kennung: zählt nicht
    return d.separate_disk ? `${nfc(d.name)} (${uuid.slice(0, 8)})` : nfc(d.name);
  };
  const retired = (uuid) => !!info.get(uuid)?.retired_at;

  let srcUuids = info.has(diskRef) ? [diskRef]
    : disks.filter((d) => !d.retired_at && nfc(d.name).toLowerCase() === diskRef.toLowerCase()).map((d) => d.uuid);
  if (diskRef.toUpperCase() === "NAS") srcUuids = disks.filter((d) => d.uuid.startsWith("nas:")).map((d) => d.uuid);
  if (!srcUuids.length) return json({ error: `Datenträger „${diskRef}“ nicht gefunden` }, 404);

  // ── Quelldateien (die Kennung mit den meisten Dateien unter dem Ordner) ──
  const variants = [...new Set([folder, folder.normalize("NFD")])];
  let src = null;
  for (const u of srcUuids) {
    const rows = [];
    for (const v of variants) {
      const r = (await env.CATALOG.prepare(
        "SELECT rel_path, name, size FROM files WHERE disk_uuid=? AND rel_path >= ? AND rel_path < ?")
        .bind(u, v + "/", v + "0").all()).results || [];
      rows.push(...r);
    }
    if (rows.length && (!src || rows.length > src.rows.length)) src = { uuid: u, rows };
  }
  const srcDevice = device(srcUuids[0]) || diskRef;
  const sync = async (uuids) => {
    if (!uuids.length) return {};
    const r = (await env.CATALOG.prepare(
      `SELECT disk_uuid, synced_at FROM disk_sync WHERE disk_uuid IN (SELECT value FROM json_each(?))`)
      .bind(JSON.stringify(uuids)).all()).results || [];
    return Object.fromEntries(r.map((x) => [x.disk_uuid, x.synced_at]));
  };
  const hinweis = "Laut Datei-Katalog (Name + Größe, nur Dateien ab 1 MB), nicht hash-verifiziert. Im Zweifel nicht löschen.";
  if (!src) {
    return json({ verdict: "nicht_loeschen", grund: "Ordner auf diesem Datenträger nicht im Katalog gefunden",
      source: { disk: srcDevice, disk_uuids: srcUuids, folder }, complete_copies: [], partial_copies: [], hinweis });
  }

  const need = new Map();                                  // sub -> {name, size}
  const ignored = { proxies: 0, zero_bytes: 0, junk: 0 };
  let bytes = 0;
  const seen = new Set();
  for (const r of src.rows) {
    const rel = nfc(r.rel_path), name = nfc(r.name);
    if (seen.has(rel)) continue;
    seen.add(rel);
    const sub = rel.slice(folder.length + 1);
    if (junk(name)) { ignored.junk++; continue; }
    if (PROXY.test(rel)) { ignored.proxies++; continue; }
    if (!r.size) { ignored.zero_bytes++; continue; }
    need.set(sub, { name, size: r.size });
    bytes += r.size;
  }
  if (!need.size) {
    return json({ verdict: "nicht_loeschen", grund: "keine prüfbaren Dateien (nur Proxies/0 Byte/Systemdateien)",
      source: { disk: srcDevice, disk_uuid: src.uuid, folder, files: 0, bytes: 0 }, ignored, complete_copies: [], partial_copies: [], hinweis });
  }

  // ── Treffer gleicher Name + Größe auf allen anderen Geräten ──
  const pairs = new Map();                                 // "name\0size" -> [sub, …]
  for (const [sub, f] of need) {
    const k = f.name + "\0" + f.size;
    if (!pairs.has(k)) pairs.set(k, []);
    pairs.get(k).push(sub);
  }
  const keys = [];
  for (const k of pairs.keys()) {
    const [name, size] = k.split("\0");
    for (const n of new Set([name, name.normalize("NFD")])) keys.push([n, Number(size)]);
  }
  const hits = [];
  for (let i = 0; i < keys.length; i += 400) {
    const r = (await env.CATALOG.prepare(
      `SELECT f.disk_uuid, f.rel_path, f.name, f.size FROM json_each(?) j
       JOIN files f ON f.name = j.value->>0 AND f.size = j.value->>1`)
      .bind(JSON.stringify(keys.slice(i, i + 400))).all()).results || [];
    hits.push(...r);
  }

  const groups = new Map();                                // device\0basePath -> {device, uuids:Set, path, subs:Set}
  const elsewhere = new Set();                             // subs, die irgendwo anders liegen (egal wo)
  for (const h of hits) {
    if (srcUuids.includes(h.disk_uuid) || retired(h.disk_uuid)) continue;
    const dev = device(h.disk_uuid);
    if (!dev || dev === srcDevice) continue;
    const rel = nfc(h.rel_path);
    for (const sub of pairs.get(nfc(h.name) + "\0" + h.size) || []) {
      elsewhere.add(sub);
      if (rel !== sub && !rel.endsWith("/" + sub)) continue;
      const path = rel.slice(0, rel.length - sub.length).replace(/\/$/, "");
      if (path.split("/").pop() !== base) continue;
      const k = dev + "\0" + path;
      if (!groups.has(k)) groups.set(k, { device: dev, uuids: new Set(), path, subs: new Set() });
      const g = groups.get(k);
      g.uuids.add(h.disk_uuid);
      g.subs.add(sub);
    }
  }

  // je Gerät die beste Ablage
  const best = new Map();
  for (const g of groups.values()) {
    const cur = best.get(g.device);
    if (!cur || g.subs.size > cur.subs.size) best.set(g.device, g);
  }
  const allUuids = [src.uuid, ...new Set([...best.values()].flatMap((g) => [...g.uuids]))];
  const synced = await sync(allUuids);
  const complete = [], partial = [];
  for (const g of best.values()) {
    const missing = [...need.keys()].filter((s) => !g.subs.has(s));
    const entry = { device: g.device, path: g.path, disk_uuids: [...g.uuids],
      catalog_synced_at: [...g.uuids].map((u) => synced[u]).filter(Boolean).sort().pop() || null };
    if (!missing.length) complete.push(entry);
    else partial.push({ ...entry, missing: missing.length,
      missing_bytes: missing.reduce((s, x) => s + need.get(x).size, 0), missing_sample: missing.slice(0, 10) });
  }
  partial.sort((a, b) => a.missing - b.missing);
  const unique = [...need.keys()].filter((s) => !elsewhere.has(s));

  const ok = complete.length >= NEEDED;
  return json({
    verdict: ok ? "loeschbar" : "nicht_loeschen",
    grund: ok ? `laut Katalog ${complete.length}× vollständig auf anderen Geräten vorhanden`
      : `nur ${complete.length} vollständige Kopie(n) auf anderen Geräten, ${NEEDED} nötig`,
    source: { disk: srcDevice, disk_uuid: src.uuid, folder, files: need.size, bytes, catalog_synced_at: synced[src.uuid] || null },
    complete_copies: complete,
    partial_copies: partial,
    unique_here: { files: unique.length, bytes: unique.reduce((s, x) => s + need.get(x).size, 0), sample: unique.slice(0, 20) },
    ignored,
    hinweis,
  });
}
