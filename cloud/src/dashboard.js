/**
 * NXT Speicher-Dashboard (eigener Worker, hinter Cloudflare Access).
 *
 * GET  /                 App (HTML/CSS/JS von hier, keine externen Quellen)
 * GET  /api/state        Kennzahlen, Projekte, Datenträger, Befunde, Unzugeordnete
 * GET  /api/audit        Änderungsprotokoll
 * POST /api/rule         Sicherungsregel eines Projekts
 * POST /api/disk         Stufe/Halter/Standort/Notiz eines Datenträgers
 * POST /api/override     Soll-Name eines Ordners (Zuordnung/Umbenennen)
 * POST /api/finding      Befund als umgesetzt/ignoriert markieren oder wieder öffnen
 *
 * Jede Änderung landet mit der Access-E-Mail im audit_log.
 */

import { verifyAccess, devUser } from "./access.js";
import { analyze, SICHERUNG, TIERS } from "./analysis.js";
import { runDerive } from "./derive.js";
import { validateFolder } from "./notion_sync.js";
import APP_HTML from "../static/app.html";
import APP_CSS from "../static/app.css";
import APP_JS from "../static/app.client.js.txt";
import FONT from "../static/outfit-latin.woff2";

const SECURITY_HEADERS = {
  "content-security-policy":
    "default-src 'none'; script-src 'self'; style-src 'self'; style-src-attr 'unsafe-inline'; font-src 'self'; img-src 'self' data:; " +
    "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
  "strict-transport-security": "max-age=31536000; includeSubDomains",
  "x-content-type-options": "nosniff",
  "referrer-policy": "no-referrer",
  "permissions-policy": "camera=(), microphone=(), geolocation=(), interest-cohort=()",
  "cross-origin-opener-policy": "same-origin",
  "x-frame-options": "DENY",
};

const respond = (body, type, status = 200, extra = {}) =>
  new Response(body, { status, headers: { "content-type": type, "cache-control": "no-store", ...SECURITY_HEADERS, ...extra } });
const json = (obj, status = 200) => respond(JSON.stringify(obj), "application/json; charset=utf-8", status);

const now = () => new Date().toISOString();
const clean = (v, max = 500) => (v == null ? null : String(v).trim().slice(0, max) || null);

async function audit(env, who, action, target, detail) {
  await env.DB.prepare("INSERT INTO audit_log(at, who, action, target, detail) VALUES(?,?,?,?,?)")
    .bind(now(), who, action, target, JSON.stringify(detail)).run();
}

async function loadState(env) {
  const q = (sql) => env.DB.prepare(sql).all().then((r) => r.results || []);
  const [projects, disks, diskMeta, rules, states, overrides] = await Promise.all([
    q(`SELECT disk_uuid, rel_path, name, date, project_name, norm_name, type, status, size_bytes, file_count, mtime,
              cluster_id, soll_name FROM projects`),
    q("SELECT uuid, name, used_bytes, fs_used_bytes, capacity_bytes, last_scan, last_user FROM disks"),
    q("SELECT * FROM disk_meta"), q("SELECT * FROM project_rule"), q("SELECT * FROM finding_state"),
    q("SELECT * FROM folder_override"),
  ]);
  const a = analyze({ projects, disks, diskMeta, rules, states });
  // Ordner ohne gültigen Namen (Kandidaten für Soll-Namen), mit Platte
  const diskName = new Map(disks.map((d) => [d.uuid, d.name]));
  const ov = new Map(overrides.map((o) => [`${o.disk_uuid}|${o.rel_path}`, o]));
  a.unassigned = projects
    .filter((p) => p.status === "unassigned" && !p.disk_uuid.startsWith("name:"))
    .map((p) => ({ disk_uuid: p.disk_uuid, disk: diskName.get(p.disk_uuid) || "?", rel_path: p.rel_path, name: p.name,
                   size_gb: Math.round(((p.size_bytes || 0) / 1e9) * 100) / 100, files: p.file_count, mtime: p.mtime,
                   soll_name: ov.get(`${p.disk_uuid}|${p.rel_path}`)?.soll_name || null }))
    .sort((x, y) => y.size_gb - x.size_gb);
  a.meta = { sicherung: SICHERUNG, tiers: TIERS, generated_at: now(),
             last_scan: disks.reduce((m, d) => (d.last_scan > m ? d.last_scan : m), "") };
  return a;
}

async function readBody(request) {
  const ct = request.headers.get("content-type") || "";
  if (!ct.startsWith("application/json")) throw new Error("JSON erwartet");
  const text = await request.text();
  if (text.length > 20000) throw new Error("Anfrage zu groß");
  return JSON.parse(text);
}

async function handleWrite(path, body, env, user) {
  const who = user.email;
  if (path === "/api/rule") {
    const key = clean(body.rule_key, 300);
    const sicherung = body.sicherung;
    if (!key || !(sicherung in SICHERUNG)) throw new Error("Regel unvollständig");
    const tiers = (Array.isArray(body.require_tiers) ? body.require_tiers : []).filter((t) => TIERS.includes(t));
    await env.DB.prepare(
      `INSERT INTO project_rule(rule_key, sicherung, require_tiers, note, updated_by, updated_at) VALUES(?,?,?,?,?,?)
       ON CONFLICT(rule_key) DO UPDATE SET sicherung=excluded.sicherung, require_tiers=excluded.require_tiers,
         note=excluded.note, updated_by=excluded.updated_by, updated_at=excluded.updated_at`
    ).bind(key, sicherung, tiers.join(","), clean(body.note), who, now()).run();
    await audit(env, who, "regel", key, { sicherung, require_tiers: tiers, note: clean(body.note) });
    return { ok: true };
  }
  if (path === "/api/disk") {
    const uuid = clean(body.disk_uuid, 100);
    if (!uuid) throw new Error("Datenträger fehlt");
    const tier = body.tier == null || body.tier === "" ? null : body.tier;
    if (tier && !TIERS.includes(tier)) throw new Error("Unbekannte Stufe");
    await env.DB.prepare(
      `INSERT INTO disk_meta(disk_uuid, tier, tier_source, holder, location, note, separate_disk, updated_by, updated_at)
       VALUES(?,?,?,?,?,?,?,?,?)
       ON CONFLICT(disk_uuid) DO UPDATE SET tier=excluded.tier, tier_source=excluded.tier_source, holder=excluded.holder,
         location=excluded.location, note=excluded.note, separate_disk=excluded.separate_disk,
         updated_by=excluded.updated_by, updated_at=excluded.updated_at`
    ).bind(uuid, tier, tier ? "manual" : null, clean(body.holder, 100), clean(body.location, 200), clean(body.note),
           body.separate_disk ? 1 : 0, who, now()).run();
    await audit(env, who, "datentraeger", uuid, { tier, holder: clean(body.holder), location: clean(body.location),
                                                 note: clean(body.note), separate_disk: !!body.separate_disk });
    return { ok: true };
  }
  if (path === "/api/override") {
    const uuid = clean(body.disk_uuid, 100), rel = body.rel_path == null ? null : String(body.rel_path).slice(0, 1000);
    if (!uuid || !rel) throw new Error("Ordner fehlt");
    const soll = clean(body.soll_name, 300);
    if (soll && !validateFolder(soll)) throw new Error("Soll-Name entspricht nicht dem Schema JJJJ-MM-TT_Projekt_TYP");
    if (soll) {
      await env.DB.prepare(
        `INSERT INTO folder_override(disk_uuid, rel_path, soll_name, updated_by, updated_at) VALUES(?,?,?,?,?)
         ON CONFLICT(disk_uuid, rel_path) DO UPDATE SET soll_name=excluded.soll_name, updated_by=excluded.updated_by,
           updated_at=excluded.updated_at`).bind(uuid, rel, soll, who, now()).run();
    } else {
      await env.DB.prepare("DELETE FROM folder_override WHERE disk_uuid=? AND rel_path=?").bind(uuid, rel).run();
    }
    await audit(env, who, "soll_name", `${uuid}|${rel}`, { soll_name: soll });
    await runDerive(env); // Zuordnung sofort sichtbar
    return { ok: true };
  }
  if (path === "/api/finding") {
    const key = clean(body.finding_key, 600);
    const status = body.status;
    if (!key || !["umgesetzt", "ignoriert", "offen"].includes(status)) throw new Error("Befund-Status ungültig");
    if (status === "offen") await env.DB.prepare("DELETE FROM finding_state WHERE finding_key=?").bind(key).run();
    else await env.DB.prepare(
      `INSERT INTO finding_state(finding_key, status, note, updated_by, updated_at) VALUES(?,?,?,?,?)
       ON CONFLICT(finding_key) DO UPDATE SET status=excluded.status, note=excluded.note,
         updated_by=excluded.updated_by, updated_at=excluded.updated_at`).bind(key, status, clean(body.note), who, now()).run();
    await audit(env, who, "befund", key, { status, note: clean(body.note) });
    return { ok: true };
  }
  return null;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") return json({ ok: true });

    let user;
    try {
      user = devUser(request, env) || (await verifyAccess(request, env));
    } catch (e) {
      return respond(`Kein Zugriff: ${e.message}`, "text/plain; charset=utf-8", 403);
    }

    if (request.method === "GET") {
      if (url.pathname === "/" || url.pathname === "/index.html") return respond(APP_HTML, "text/html; charset=utf-8");
      if (url.pathname === "/app.css") return respond(APP_CSS, "text/css; charset=utf-8");
      if (url.pathname === "/app.js") return respond(APP_JS, "text/javascript; charset=utf-8");
      if (url.pathname === "/outfit.woff2") return respond(FONT, "font/woff2", 200, { "cache-control": "public, max-age=604800" });
      if (url.pathname === "/api/state") return json({ me: user.email, ...(await loadState(env)) });
      if (url.pathname === "/api/audit") {
        const rows = (await env.DB.prepare("SELECT * FROM audit_log ORDER BY id DESC LIMIT 300").all()).results || [];
        return json({ rows });
      }
      return json({ error: "nicht gefunden" }, 404);
    }

    if (request.method === "POST") {
      // CSRF-Schutz: nur same-origin JSON
      const origin = request.headers.get("origin");
      if (origin && origin !== url.origin) return json({ error: "fremde Herkunft" }, 403);
      try {
        const res = await handleWrite(url.pathname, await readBody(request), env, user);
        return res ? json(res) : json({ error: "nicht gefunden" }, 404);
      } catch (e) {
        return json({ error: e.message }, 400);
      }
    }
    return json({ error: "Methode nicht erlaubt" }, 405);
  },
};
