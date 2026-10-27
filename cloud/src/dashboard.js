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
 * GET  /uebergabe/<person>  Übergabe-Liste (Altprojekte „An Clemens/Paul geben“), eigene Seite
 * POST /api/handover     Entscheidung + Nachricht von Clemens/Paul zu einem ihrer Projekte
 * GET  /api/dropbox      Dropbox-Ausstieg (Ansicht #/dropbox): Ordner, die nur in der Dropbox liegen
 * POST /api/dropbox      Entscheidung + Notiz zu einem Dropbox-Ordner
 * GET  /api/chat?after=  Chat mit dem NXT Data Agent (nur der eigene Verlauf, nach Login-E-Mail) (Antworten schreibt der Agent auf dem Sicherungs-Mac)
 * POST /api/chat         Nachricht an den Agent
 *
 * Jede Änderung landet mit der Access-E-Mail im audit_log.
 */

import { verifyAccess, devUser } from "./access.js";
import { legacyProjects, oldProjects, LEGACY_DECISIONS, DEFAULT_CUTOFF, validCutoff, HANDOVER } from "./legacy.js";
import { handoverPage, HANDOVER_CHOICES } from "./handover.js";
import HANDOVER_JS from "../static/handover.js.txt";
import { DROPBOX_CHOICES } from "./dropbox.js";
import { analyze, SICHERUNG, TIERS, DEFAULT_RESERVE_MONTHS } from "./analysis.js";
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

// Altprojekte: Nils' Nummern-Ordner + Projekte bis zum Stichmonat (YYYY-MM), mit Entscheidungen
async function loadLegacy(env, projects = null, cutoff = DEFAULT_CUTOFF) {
  const [rows, disks, dec, ho, prj] = await Promise.all([
    env.DB.prepare(`SELECT disk_uuid, rel_path, size_bytes, file_count FROM folder_tree
                    WHERE depth<=3 AND (rel_path GLOB '[0-9]*' OR rel_path GLOB '*/[0-9]*')`).all().then((r) => r.results || []),
    env.DB.prepare("SELECT uuid, name, last_scan FROM disks").all().then((r) => r.results || []),
    env.DB.prepare("SELECT * FROM legacy_decision").all().then((r) => r.results || []),
    env.DB.prepare("SELECT * FROM handover_decision").all().then((r) => r.results || []).catch(() => []),
    projects ? Promise.resolve(projects)
      : env.DB.prepare("SELECT disk_uuid, rel_path, date, project_name, status, size_bytes, cluster_id FROM projects").all().then((r) => r.results || []),
  ]);
  const byKey = new Map(dec.map((d) => [d.key, d]));
  const hoKey = new Map(ho.map((d) => [d.key, d]));
  const items = [...legacyProjects(rows, disks).map((g) => ({ ...g, source: "nummer" })), ...oldProjects(prj, disks, cutoff)]
    .map((g) => ({ ...g, decision: byKey.get(g.key)?.decision || null, note: byKey.get(g.key)?.note || null,
                   decided_by: byKey.get(g.key)?.updated_by || null,
                   handover: hoKey.get(g.key) ? { choice: hoKey.get(g.key).choice, message: hoKey.get(g.key).message,
                                                  by: hoKey.get(g.key).updated_by, at: hoKey.get(g.key).updated_at } : null }));
  return items;
}

// Vorschläge aus dem Inhalts-Abgleich als gedachte Zuordnung: Projektzeilen umhängen bzw. ergänzen
function applySuggestions(projects, suggestions) {
  const tpl = new Map();
  for (const p of projects) if (p.cluster_id && !tpl.has(p.cluster_id)) tpl.set(p.cluster_id, p);
  const sk = new Map(suggestions.map((s) => [`${s.disk_uuid}|${s.rel_path}`, s]));
  const out = projects.filter((p) => !sk.has(`${p.disk_uuid}|${p.rel_path}`));
  for (const s of suggestions) {
    const t = tpl.get(s.target_cluster);
    if (!t) continue;
    out.push({ ...t, disk_uuid: s.disk_uuid, rel_path: s.rel_path, name: s.rel_path.split("/").pop(), type: s.target_type,
               status: "validated", size_bytes: s.size_bytes, file_count: null, soll_name: s.soll_name });
  }
  return out;
}

// Leere Dateien, deren Inhalt anderswo intakt liegt (gleicher Ordner + Name, keine Proxys, ohne Dropbox),
// aber nicht auf demselben Datenträger: nur diese machen eine Kopie unvollständig.
const ZERO_MISSING_SQL = `WITH z AS (SELECT disk_uuid, rel_path, name, rtrim(rel_path, replace(rel_path,'/','')) AS p FROM files WHERE size=0 LIMIT 50000),
z2 AS (SELECT disk_uuid, rel_path, name, substr(rel_path, length(rtrim(substr(p,1,length(p)-1), replace(substr(p,1,length(p)-1),'/','')))+1) AS suf FROM z)
SELECT disk_uuid, rel_path FROM z2 WHERE
  EXISTS(SELECT 1 FROM files f WHERE f.name=z2.name AND f.size>0 AND f.disk_uuid<>'dropbox:NXT' AND f.rel_path NOT LIKE '%proxy%' AND substr(f.rel_path, -length(z2.suf))=z2.suf)
  AND NOT EXISTS(SELECT 1 FROM files f WHERE f.name=z2.name AND f.size>0 AND f.disk_uuid=z2.disk_uuid AND f.rel_path NOT LIKE '%proxy%' AND substr(f.rel_path, -length(z2.suf))=z2.suf)`;

let zeroCache = null;

async function loadState(env) {
  const q = (sql) => env.DB.prepare(sql).all().then((r) => r.results || []);
  const [projects, disks, diskMeta, rules, states, overrides, health] = await Promise.all([
    q(`SELECT disk_uuid, rel_path, name, date, project_name, norm_name, type, status, size_bytes, file_count, mtime,
              cluster_id, soll_name FROM projects`),
    q("SELECT uuid, name, used_bytes, fs_used_bytes, capacity_bytes, last_scan, last_user FROM disks"),
    q("SELECT * FROM disk_meta"), q("SELECT * FROM project_rule"), q("SELECT * FROM finding_state"),
    q("SELECT * FROM folder_override"), q("SELECT * FROM disk_health"),
  ]);
  const rm = await env.DB.prepare("SELECT value FROM meta WHERE key='cleanup_reserve_months'").first();
  const reserveMonths = rm ? Number(rm.value) : DEFAULT_RESERVE_MONTHS;
  const legacy = await loadLegacy(env, projects);
  let zeroFiles = [], zeroMissing = null;
  try { zeroFiles = env.CATALOG ? ((await env.CATALOG.prepare("SELECT disk_uuid, rel_path FROM files WHERE size=0 LIMIT 50000").all()).results || []) : []; }
  catch { zeroFiles = []; }
  // teure Abfrage (~2–3 s): je Worker-Instanz 10 min zwischenspeichern
  if (zeroCache && Date.now() - zeroCache.at < 600000) zeroMissing = zeroCache.rows;
  else {
    try { zeroMissing = env.CATALOG ? ((await env.CATALOG.prepare(ZERO_MISSING_SQL).all()).results || null) : null; }
    catch { zeroMissing = null; }
    if (zeroMissing) zeroCache = { at: Date.now(), rows: zeroMissing };
  }
  // Nils' Nummern-Ordner (keine Projekte der Liste): Aussortieren = alle Kopien frei, Nur 1× = alle bis auf eine
  const legacyGb = legacy.filter((x) => x.source === "nummer").reduce((s, x) => s + (x.decision === "aussortieren" ? x.total_gb
    : x.decision === "einfach" ? Math.max(0, x.total_gb - x.size_gb) : 0), 0);
  const a = analyze({ projects, disks, diskMeta, rules, states, health, reserveMonths, legacyGb, zeroFiles, zeroMissing });
  // Vorschläge aus dem Inhalts-Abgleich: offen und noch ohne Soll-Name
  const ovKeys = new Set(overrides.map((o) => `${o.disk_uuid}|${o.rel_path}`));
  const sugg = (await q("SELECT * FROM content_suggestion WHERE status='offen'")).filter((s) => !ovKeys.has(`${s.disk_uuid}|${s.rel_path}`));
  const dn = new Map(disks.map((d) => [d.uuid, d.name]));
  a.suggestions = sugg.map((s) => ({ ...s, disk: dn.get(s.disk_uuid) || "?" })).sort((x, y) => y.shared_gb - x.shared_gb);
  if (sugg.length) {
    const b = analyze({ projects: applySuggestions(projects, sugg), disks, diskMeta, rules, states, health, reserveMonths, legacyGb, zeroFiles, zeroMissing });
    const lv = (r) => Object.fromEntries(["einfach", "teilweise", "ok", "mehrfach"].map((k) => [k, r.projects.filter((p) => p.level === k).length]));
    a.cleanup.with_suggestions = { count: sugg.length, balance: b.cleanup.balance, levels: lv(b), levels_now: lv(a) };
  }
  // Ordner ohne gültigen Namen (Kandidaten für Soll-Namen), mit Platte
  const diskName = new Map(disks.map((d) => [d.uuid, d.name]));
  const ov = new Map(overrides.map((o) => [`${o.disk_uuid}|${o.rel_path}`, o]));
  a.unassigned = projects
    .filter((p) => p.status === "unassigned" && !p.disk_uuid.startsWith("name:"))
    .map((p) => ({ disk_uuid: p.disk_uuid, disk: diskName.get(p.disk_uuid) || "?", rel_path: p.rel_path, name: p.name,
                   size_gb: Math.round(((p.size_bytes || 0) / 1e9) * 100) / 100, files: p.file_count, mtime: p.mtime,
                   soll_name: ov.get(`${p.disk_uuid}|${p.rel_path}`)?.soll_name || null }))
    .sort((x, y) => y.size_gb - x.size_gb);
  // Startwert des Aufräum-Ziels festhalten (einmalig bzw. nach bewusstem Zurücksetzen)
  let base = await env.DB.prepare("SELECT value FROM meta WHERE key='cleanup_baseline'").first();
  if (!base) {
    const b = { at: now(), shortfall_gb: a.cleanup.balance.shortfall_gb, target_gb: a.cleanup.balance.target_gb };
    await env.DB.prepare("INSERT OR IGNORE INTO meta(key,value) VALUES('cleanup_baseline',?)").bind(JSON.stringify(b)).run();
    base = { value: JSON.stringify(b) };
  }
  const baseline = JSON.parse(base.value);
  const closed = Math.max(0, baseline.shortfall_gb - a.cleanup.balance.shortfall_gb);
  a.cleanup.goal = { baseline, closed_gb: Math.round(closed * 100) / 100,
                     progress: baseline.shortfall_gb > 0 ? Math.min(1, closed / baseline.shortfall_gb) : 1 };
  // Kennzahlen „nach Inhalt“ (Datei-Katalog, vom Sicherungs-Mac berechnet)
  try { const cs = await env.DB.prepare("SELECT value FROM meta WHERE key='content_stats'").first(); a.content = cs ? JSON.parse(cs.value) : null; }
  catch { a.content = null; }
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

// Altprojekt aus der Projektliste: Aussortieren = Sicherung "Löschbar", Nur 1× = "Einfach", Behalten = "Doppelt".
// Wird eine dieser Entscheidungen zurückgenommen, geht die Sicherung wieder auf "Doppelt".
async function applyLegacyRule(env, who, key, prev, next, via) {
  if (!key.startsWith("p|") || next === prev) return;
  const rk = key.slice(2);
  const rule = (await env.DB.prepare("SELECT sicherung FROM project_rule WHERE rule_key=?").bind(rk).first()) || {};
  const want = next === "aussortieren" ? "loeschbar" : next === "einfach" ? "einfach" : next === "behalten" ? "doppelt"
    : ["loeschbar", "einfach"].includes(rule.sicherung) && ["aussortieren", "einfach"].includes(prev) ? "doppelt" : null;
  if (want && want !== rule.sicherung) {
    await env.DB.prepare(`INSERT INTO project_rule(rule_key, sicherung, require_tiers, note, updated_by, updated_at) VALUES(?,?,'',NULL,?,?)
      ON CONFLICT(rule_key) DO UPDATE SET sicherung=excluded.sicherung, updated_by=excluded.updated_by, updated_at=excluded.updated_at`)
      .bind(rk, want, who, now()).run();
    await audit(env, who, "regel", rk, { sicherung: [rule.sicherung || null, want], via });
  }
}

async function handleWrite(path, body, env, user) {
  const who = user.email;
  // Teil-Updates: nur Felder, die im Body stehen, werden geändert. Alles andere bleibt,
  // damit ein Formular nie versehentlich bestehende Werte mit "nichts" überschreibt.
  if (path === "/api/rule") {
    const key = clean(body.rule_key, 300);
    if (!key) throw new Error("Projekt fehlt");
    const cur = (await env.DB.prepare("SELECT * FROM project_rule WHERE rule_key=?").bind(key).first()) || {};
    const next = { sicherung: cur.sicherung || "doppelt", require_tiers: cur.require_tiers || "", note: cur.note ?? null,
                   ref_copies: cur.ref_copies || null, type_sicherung: cur.type_sicherung || null };
    const changed = {};
    if ("sicherung" in body) {
      if (!(body.sicherung in SICHERUNG)) throw new Error("Unbekannte Sicherung");
      if (body.sicherung !== next.sicherung) changed.sicherung = [next.sicherung, body.sicherung];
      next.sicherung = body.sicherung;
    }
    if ("require_tiers" in body) {
      const tiers = (Array.isArray(body.require_tiers) ? body.require_tiers : []).filter((t) => TIERS.includes(t)).join(",");
      if (tiers !== next.require_tiers) changed.require_tiers = [next.require_tiers, tiers];
      next.require_tiers = tiers;
    }
    if ("note" in body) {
      const n = clean(body.note);
      if (n !== next.note) changed.note = [next.note, n];
      next.note = n;
    }
    // Sollstand je Ordnertyp: {"FOOTAGE": "<disk_uuid>" | null}; nur genannte Typen ändern sich
    if (body.ref_copies && typeof body.ref_copies === "object") {
      let refs = {};
      try { refs = JSON.parse(next.ref_copies || "{}") || {}; } catch { refs = {}; }
      for (const [t, u] of Object.entries(body.ref_copies)) {
        const tt = clean(t, 30), uu = u == null ? null : clean(u, 100);
        if (!tt) continue;
        if (uu) refs[tt] = uu; else delete refs[tt];
      }
      const val = Object.keys(refs).length ? JSON.stringify(refs) : null;
      if (val !== next.ref_copies) changed.ref_copies = [next.ref_copies, val];
      next.ref_copies = val;
    }
    // Regel je Ordnertyp: {"PHOTOS": "loeschbar" | null}; null = wieder wie das Projekt
    if (body.type_sicherung && typeof body.type_sicherung === "object") {
      let tr = {};
      try { tr = JSON.parse(next.type_sicherung || "{}") || {}; } catch { tr = {}; }
      for (const [t, v] of Object.entries(body.type_sicherung)) {
        const tt = clean(t, 30);
        if (!tt) continue;
        if (v == null || v === "") delete tr[tt];
        else if (v in SICHERUNG) tr[tt] = v;
        else throw new Error("Unbekannte Sicherung");
      }
      const val = Object.keys(tr).length ? JSON.stringify(tr) : null;
      if (val !== next.type_sicherung) changed.type_sicherung = [next.type_sicherung, val];
      next.type_sicherung = val;
    }
    if (!Object.keys(changed).length) return { ok: true, unchanged: true };
    await env.DB.prepare(
      `INSERT INTO project_rule(rule_key, sicherung, require_tiers, note, ref_copies, type_sicherung, updated_by, updated_at) VALUES(?,?,?,?,?,?,?,?)
       ON CONFLICT(rule_key) DO UPDATE SET sicherung=excluded.sicherung, require_tiers=excluded.require_tiers,
         note=excluded.note, ref_copies=excluded.ref_copies, type_sicherung=excluded.type_sicherung,
         updated_by=excluded.updated_by, updated_at=excluded.updated_at`
    ).bind(key, next.sicherung, next.require_tiers, next.note, next.ref_copies, next.type_sicherung, who, now()).run();
    await audit(env, who, "regel", key, changed);
    return { ok: true };
  }
  // Sammel-Entscheidung: nur die Sicherungsstufe vieler Projekte setzen (Stufen/Notiz bleiben)
  // Altprojekte: Entscheidung (aussortieren | behalten | ansehen | null) und Notiz, protokolliert
  if (path === "/api/legacy") {
    const key = clean(body.key, 200);
    if (!key) throw new Error("Projekt fehlt");
    const cur = (await env.DB.prepare("SELECT * FROM legacy_decision WHERE key=?").bind(key).first()) || {};
    const next = { decision: cur.decision || null, note: cur.note || null };
    if ("decision" in body) {
      if (body.decision != null && !(body.decision in LEGACY_DECISIONS)) throw new Error("Unbekannte Entscheidung");
      next.decision = body.decision || null;
    }
    if ("note" in body) next.note = clean(body.note);
    if (next.decision === (cur.decision || null) && next.note === (cur.note || null)) return { ok: true, unchanged: true };
    await env.DB.prepare(`INSERT INTO legacy_decision(key, decision, note, updated_by, updated_at) VALUES(?,?,?,?,?)
      ON CONFLICT(key) DO UPDATE SET decision=excluded.decision, note=excluded.note, updated_by=excluded.updated_by, updated_at=excluded.updated_at`)
      .bind(key, next.decision, next.note, who, now()).run();
    await audit(env, who, "altprojekt", key, { decision: [cur.decision || null, next.decision], note: [cur.note || null, next.note] });
    if ("decision" in body) await applyLegacyRule(env, who, key, cur.decision || null, next.decision, "Altprojekte");
    return { ok: true };
  }
  // Übergabe-Seite: Clemens/Paul entscheiden selbst (Aussortieren/Nur 1×/Behalten/Ansehen) und können eine Nachricht hinterlassen
  if (path === "/api/chat") {
    const text = clean(body.text, 4000);
    if (!text) throw new Error("Nachricht ist leer");
    await env.DB.prepare("INSERT INTO chat_message(at, author, role, text, thread) VALUES(?,?,?,?,?)")
      .bind(now(), who, "user", text, String(who).toLowerCase()).run();
    return { ok: true };
  }
  if (path === "/api/dropbox") {
    const key = String(body.key || "").slice(0, 1000);
    const cur = await env.DB.prepare("SELECT * FROM dropbox_folder WHERE path=?").bind(key).first();
    if (!cur) throw new Error("Ordner unbekannt");
    const next = { choice: cur.choice || null, message: cur.message || null };
    if ("choice" in body) {
      if (body.choice != null && body.choice !== "" && !(body.choice in DROPBOX_CHOICES)) throw new Error("Unbekannte Auswahl");
      next.choice = body.choice || null;
    }
    if ("message" in body) next.message = clean(body.message, 2000);
    await env.DB.prepare("UPDATE dropbox_folder SET choice=?, message=?, updated_by=?, updated_at=? WHERE path=?")
      .bind(next.choice, next.message, who, now(), key).run();
    await audit(env, who, "dropbox", key, { choice: [cur.choice || null, next.choice], message: [cur.message || null, next.message] });
    return { ok: true };
  }
  if (path === "/api/handover") {
    const key = clean(body.key, 200);
    const dec = await env.DB.prepare("SELECT decision FROM legacy_decision WHERE key=?").bind(key).first();
    const person = dec?.decision;
    if (!key || !(person in HANDOVER)) throw new Error("Projekt ist keiner Person zugeordnet");
    if (user.only && user.only !== person) throw new Error("Kein Zugriff auf dieses Projekt");
    const cur = (await env.DB.prepare("SELECT * FROM handover_decision WHERE key=?").bind(key).first()) || {};
    const next = { choice: cur.choice || null, message: cur.message || null };
    if ("choice" in body) {
      if (body.choice != null && !HANDOVER_CHOICES.includes(body.choice)) throw new Error("Unbekannte Auswahl");
      next.choice = body.choice || null;
    }
    if ("message" in body) next.message = clean(body.message, 2000);
    if (next.choice === (cur.choice || null) && next.message === (cur.message || null)) return { ok: true, unchanged: true };
    await env.DB.prepare(`INSERT INTO handover_decision(key, person, choice, message, updated_by, updated_at) VALUES(?,?,?,?,?,?)
      ON CONFLICT(key) DO UPDATE SET person=excluded.person, choice=excluded.choice, message=excluded.message, updated_by=excluded.updated_by, updated_at=excluded.updated_at`)
      .bind(key, person, next.choice, next.message, who, now()).run();
    await audit(env, who, "uebergabe", key, { person, choice: [cur.choice || null, next.choice], message: [cur.message || null, next.message] });
    if ("choice" in body) await applyLegacyRule(env, who, key, cur.choice || null, next.choice, `Übergabe ${HANDOVER[person]}`);
    return { ok: true };
  }
  // Vorschlag aus dem Inhalts-Abgleich: übernehmen (setzt Soll-Name) oder verwerfen
  if (path === "/api/suggestion") {
    const uuid = clean(body.disk_uuid, 100), rel = body.rel_path == null ? null : String(body.rel_path).slice(0, 1000);
    const s = await env.DB.prepare("SELECT * FROM content_suggestion WHERE disk_uuid=? AND rel_path=?").bind(uuid, rel).first();
    if (!s) throw new Error("Vorschlag nicht gefunden");
    if (body.action === "uebernehmen") {
      await env.DB.prepare(`INSERT INTO folder_override(disk_uuid, rel_path, soll_name, updated_by, updated_at) VALUES(?,?,?,?,?)
        ON CONFLICT(disk_uuid, rel_path) DO UPDATE SET soll_name=excluded.soll_name, updated_by=excluded.updated_by, updated_at=excluded.updated_at`)
        .bind(uuid, rel, s.soll_name, who, now()).run();
      await audit(env, who, "soll_name", `${uuid}|${rel}`, { soll_name: s.soll_name, via: "Inhalts-Abgleich" });
    } else if (body.action !== "verwerfen") throw new Error("Unbekannte Aktion");
    await env.DB.prepare("UPDATE content_suggestion SET status=?, decided_by=?, decided_at=? WHERE disk_uuid=? AND rel_path=?")
      .bind(body.action === "uebernehmen" ? "uebernommen" : "verworfen", who, now(), uuid, rel).run();
    if (body.action === "verwerfen") await audit(env, who, "vorschlag", `${uuid}|${rel}`, { verworfen: s.soll_name });
    if (body.action === "uebernehmen" && !body.no_derive) await runDerive(env);
    return { ok: true };
  }
  // Antwort auf eine Rückfrage des Assistenten
  if (path === "/api/answer") {
    const id = Number(body.id);
    const q = await env.DB.prepare("SELECT * FROM assistant_question WHERE id=?").bind(id).first();
    if (!q) throw new Error("Frage nicht gefunden");
    if (q.status !== "offen") throw new Error("Frage ist nicht mehr offen");
    const answer = clean(body.answer, 300), note = clean(body.note, 2000);
    if (!answer && !note) throw new Error("Bitte eine Antwort wählen oder schreiben");
    await env.DB.prepare("UPDATE assistant_question SET status='beantwortet', answer=?, answer_note=?, answered_by=?, answered_at=? WHERE id=?")
      .bind(answer, note, who, now(), id).run();
    await audit(env, who, "antwort", `Frage ${id}`, { frage: q.question, antwort: answer, notiz: note });
    return { ok: true };
  }
  // Upload-Briefkasten: Link anlegen / deaktivieren
  if (path === "/api/briefkasten/link") {
    const name = clean(body.name, 120);
    if (!name) throw new Error("Bitte einen Namen angeben (wird zum Ordnernamen auf dem NAS)");
    const days = Math.min(90, Math.max(1, Number(body.days) || 14));
    const maxGb = body.max_gb ? Math.max(1, Number(body.max_gb)) : null;
    const rnd = crypto.getRandomValues(new Uint8Array(18));
    const token = btoa(String.fromCharCode(...rnd)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
    const exp = new Date(Date.now() + days * 86400000).toISOString();
    await env.DB.prepare("INSERT INTO upload_link(token, name, created_by, created_at, expires_at, max_bytes, note, active) VALUES(?,?,?,?,?,?,?,1)")
      .bind(token, name, who, now(), exp, maxGb ? Math.round(maxGb * 1e9) : null, clean(body.note, 300)).run();
    await audit(env, who, "upload_link", name, { gueltig_tage: days, max_gb: maxGb });
    return { ok: true, token, link: `${env.PUBLIC_INGEST_URL || "https://nxt-scanner-ingest.nxtstudios.workers.dev"}/u/${token}` };
  }
  if (path === "/api/briefkasten/deactivate") {
    const token = clean(body.token, 80);
    const l = await env.DB.prepare("SELECT name FROM upload_link WHERE token=?").bind(token).first();
    if (!l) throw new Error("Link nicht gefunden");
    await env.DB.prepare("UPDATE upload_link SET active=0 WHERE token=?").bind(token).run();
    await audit(env, who, "upload_link", l.name, { deaktiviert: true });
    return { ok: true };
  }
  if (path === "/api/rules/bulk") {
    const keys = [...new Set((Array.isArray(body.rule_keys) ? body.rule_keys : []).map((k) => clean(k, 300)).filter(Boolean))];
    if (!keys.length || keys.length > 1000) throw new Error("1 bis 1000 Projekte auswählen");
    if (!(body.sicherung in SICHERUNG)) throw new Error("Unbekannte Sicherung");
    const at = now();
    const stmt = env.DB.prepare(
      `INSERT INTO project_rule(rule_key, sicherung, require_tiers, note, updated_by, updated_at) VALUES(?,?,'',NULL,?,?)
       ON CONFLICT(rule_key) DO UPDATE SET sicherung=excluded.sicherung, updated_by=excluded.updated_by, updated_at=excluded.updated_at`);
    for (let i = 0; i < keys.length; i += 100) {
      await env.DB.batch(keys.slice(i, i + 100).map((k) => stmt.bind(k, body.sicherung, who, at)));
    }
    await audit(env, who, "regel_sammel", `${keys.length} Projekte`, { sicherung: body.sicherung, rule_keys: keys.slice(0, 50) });
    return { ok: true, count: keys.length };
  }
  if (path === "/api/cleanup/reserve") {    // Reserve-Monate ändern -> Ziel neu setzen
    const m = Number(body.months);
    if (![0, 1, 3, 6].includes(m)) throw new Error("Reserve: 0, 1, 3 oder 6 Monate");
    await env.DB.prepare("INSERT INTO meta(key,value) VALUES('cleanup_reserve_months',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
      .bind(String(m)).run();
    await audit(env, who, "ziel_reserve", "Aufräum-Ziel", { reserve_months: m });
    return handleWrite("/api/cleanup/baseline", {}, env, user);
  }
  if (path === "/api/cleanup/baseline") {   // Ziel neu setzen (z. B. nach Plattenkauf), protokolliert
    const st = await loadState(env);
    const b = { at: now(), shortfall_gb: st.cleanup.balance.shortfall_gb, target_gb: st.cleanup.balance.target_gb };
    await env.DB.prepare("INSERT INTO meta(key,value) VALUES('cleanup_baseline',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
      .bind(JSON.stringify(b)).run();
    await audit(env, who, "ziel_neu", "Aufräum-Ziel", b);
    return { ok: true };
  }
  if (path === "/api/disk") {
    const uuid = clean(body.disk_uuid, 100);
    if (!uuid) throw new Error("Datenträger fehlt");
    const cur = (await env.DB.prepare("SELECT * FROM disk_meta WHERE disk_uuid=?").bind(uuid).first()) || {};
    const next = { tier: cur.tier ?? null, tier_source: cur.tier_source ?? null, holder: cur.holder ?? null,
                   location: cur.location ?? null, note: cur.note ?? null, separate_disk: cur.separate_disk ? 1 : 0,
                   enclosure: cur.enclosure ?? null };
    const changed = {};
    if ("tier" in body) {
      if (!TIERS.includes(body.tier)) throw new Error("Bitte eine Speicherstufe wählen");
      if (body.tier !== next.tier || next.tier_source !== "manual") changed.tier = [next.tier, body.tier];
      next.tier = body.tier; next.tier_source = "manual";
    }
    for (const [f, max] of [["holder", 100], ["location", 200], ["note", 500], ["enclosure", 100]]) {
      if (f in body) {
        const v = clean(body[f], max);
        if (v !== next[f]) changed[f] = [next[f], v];
        next[f] = v;
      }
    }
    if ("separate_disk" in body) {
      const v = body.separate_disk ? 1 : 0;
      if (v !== next.separate_disk) changed.separate_disk = [next.separate_disk, v];
      next.separate_disk = v;
    }
    // Entfernen (umkehrbar) mit Grund / Wiederherstellen
    next.retired_at = cur.retired_at ?? null; next.retired_reason = cur.retired_reason ?? null;
    if ("retire" in body) {
      if (body.retire) {
        const reason = clean(body.retire_reason, 300);
        if (!reason) throw new Error("Bitte einen Grund angeben");
        changed.entfernt = [next.retired_reason, reason];
        next.retired_at = now(); next.retired_reason = reason;
      } else if (next.retired_at) {
        changed.entfernt = [next.retired_reason, null];
        next.retired_at = null; next.retired_reason = null;
      }
    }
    if (!Object.keys(changed).length) return { ok: true, unchanged: true };
    await env.DB.prepare(
      `INSERT INTO disk_meta(disk_uuid, tier, tier_source, holder, location, note, separate_disk, enclosure, retired_at, retired_reason, updated_by, updated_at)
       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
       ON CONFLICT(disk_uuid) DO UPDATE SET tier=excluded.tier, tier_source=excluded.tier_source, holder=excluded.holder,
         location=excluded.location, note=excluded.note, separate_disk=excluded.separate_disk, enclosure=excluded.enclosure,
         retired_at=excluded.retired_at, retired_reason=excluded.retired_reason, updated_by=excluded.updated_by, updated_at=excluded.updated_at`
    ).bind(uuid, next.tier, next.tier_source, next.holder, next.location, next.note, next.separate_disk, next.enclosure,
           next.retired_at, next.retired_reason, who, now()).run();
    await audit(env, who, "datentraeger", uuid, changed);
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

    // Übergabe-Seite je Person; externe Betrachter (HANDOVER_VIEWERS) sehen nur ihre eigene
    const ho = /^\/uebergabe\/([a-z]+)\/?$/.exec(url.pathname);
    const extOk = request.method === "GET" ? (ho && ho[1] === user.only) || ["/app.css", "/outfit.woff2", "/handover.js"].includes(url.pathname)
      : request.method === "POST" && url.pathname === "/api/handover";
    if (user.only && !extOk) {
      return respond("Kein Zugriff", "text/plain; charset=utf-8", 403);
    }
    if (request.method === "GET" && ho) {
      if (!(ho[1] in HANDOVER)) return json({ error: "nicht gefunden" }, 404);
      return respond(handoverPage(await loadLegacy(env, null, "9999-12"), ho[1], { me: user.email }), "text/html; charset=utf-8");
    }

    if (request.method === "GET") {
      if (url.pathname === "/" || url.pathname === "/index.html") return respond(APP_HTML, "text/html; charset=utf-8");
      if (url.pathname === "/app.css") return respond(APP_CSS, "text/css; charset=utf-8");
      if (url.pathname === "/app.js") return respond(APP_JS, "text/javascript; charset=utf-8");
      if (url.pathname === "/handover.js") return respond(HANDOVER_JS, "text/javascript; charset=utf-8");
      if (url.pathname === "/dropbox" || url.pathname === "/dropbox/") return new Response(null, { status: 302, headers: { location: "/#/dropbox" } });
      if (url.pathname === "/api/chat") {
        const after = Number(url.searchParams.get("after")) || 0;
        const rows = (await env.DB.prepare(
          "SELECT * FROM (SELECT * FROM chat_message WHERE thread = ? AND id > ? ORDER BY id DESC LIMIT 200) ORDER BY id")
          .bind(String(user.email).toLowerCase(), after).all()).results || [];
        return json({ rows });
      }
      if (url.pathname === "/api/dropbox") {
        const rows = (await env.DB.prepare("SELECT * FROM dropbox_folder").all()).results || [];
        return json({ rows, choices: DROPBOX_CHOICES });
      }
      if (url.pathname === "/outfit.woff2") return respond(FONT, "font/woff2", 200, { "cache-control": "public, max-age=604800" });
      if (url.pathname === "/api/state") return json({ me: user.email, ...(await loadState(env)) });
      if (url.pathname === "/api/progress") {
        // laufend (Meldung < 3 min alt) oder in der letzten Stunde beendet
        const rows = (await env.DB.prepare(
          `SELECT * FROM job_progress WHERE (state='running' AND julianday(updated_at) >= julianday('now','-3 minutes'))
             OR (state<>'running' AND julianday(updated_at) >= julianday('now','-5 minutes')) ORDER BY state='running' DESC, updated_at DESC`).all()).results || [];
        // hängengebliebene Jobs nur 30 min zeigen (danach gelten sie als abgebrochen).
        // julianday() statt Textvergleich: updated_at ist ISO mit "T"/"Z", datetime('now') nicht.
        const stale = (await env.DB.prepare(
          "SELECT * FROM job_progress WHERE state='running' AND julianday(updated_at) < julianday('now','-3 minutes') AND julianday(updated_at) >= julianday('now','-30 minutes')").all()).results || [];
        return json({ jobs: rows, stalled: stale });
      }
      if (url.pathname === "/api/legacy") {
        const cutoff = validCutoff(url.searchParams.get("bis")) || DEFAULT_CUTOFF;
        return json({ items: await loadLegacy(env, null, cutoff), decisions: LEGACY_DECISIONS, cutoff, default_cutoff: DEFAULT_CUTOFF });
      }
      if (url.pathname === "/api/questions") {
        const rows = (await env.DB.prepare(
          `SELECT * FROM assistant_question WHERE status='offen' OR julianday(answered_at) >= julianday('now','-2 days')
           ORDER BY status='offen' DESC, id DESC LIMIT 30`).all()).results || [];
        return json({ rows });
      }
      if (url.pathname === "/api/briefkasten") {
        const links = (await env.DB.prepare(`SELECT l.*, COUNT(f.id) files, COALESCE(SUM(f.size),0) bytes,
            SUM(f.state='abgeholt') pulled, SUM(f.state='hochgeladen') waiting, SUM(f.state='laeuft') running, MAX(f.uploaded_at) last_upload
          FROM upload_link l LEFT JOIN upload_file f ON f.token=l.token AND f.state!='abgebrochen'
          GROUP BY l.token ORDER BY l.created_at DESC LIMIT 200`).all()).results || [];
        const files = (await env.DB.prepare(`SELECT f.id, f.token, f.rel_path, f.size, f.state, f.created_at, f.uploaded_at, f.pulled_at, f.nas_path, f.error
          FROM upload_file f ORDER BY f.id DESC LIMIT 300`).all()).results || [];
        return json({ links, files, base: `${env.PUBLIC_INGEST_URL || "https://nxt-scanner-ingest.nxtstudios.workers.dev"}/u/` });
      }
      if (url.pathname === "/api/zero") {
        // Leere Dateien eines Datenträgers + heile Versionen (gleicher Name, Größe > 0, gleiche letzte Ordnerebene)
        const disk = url.searchParams.get("disk");
        const zs = (await env.CATALOG.prepare("SELECT rel_path, name FROM files WHERE disk_uuid=? AND size=0 ORDER BY rel_path LIMIT 3000").bind(disk).all()).results || [];
        const names = [...new Set(zs.map((z) => z.name))];
        const cand = new Map();
        for (let i = 0; i < names.length; i += 90) {
          const chunk = names.slice(i, i + 90);
          const rows = (await env.CATALOG.prepare(`SELECT disk_uuid, rel_path, name, size FROM files WHERE size>0 AND name IN (${chunk.map(() => "?").join(",")})`)
            .bind(...chunk).all()).results || [];
          for (const r of rows) { if (!cand.has(r.name)) cand.set(r.name, []); cand.get(r.name).push(r); }
        }
        const dn = new Map(((await env.DB.prepare("SELECT uuid, name FROM disks").all()).results || []).map((d) => [d.uuid, d.name]));
        const parent = (p) => p.split("/").slice(-2, -1)[0] || "";
        const items = zs.map((z) => {
          const ok = (cand.get(z.name) || []).filter((c) => parent(c.rel_path) === parent(z.rel_path))
            .map((c) => ({ disk: dn.get(c.disk_uuid) || c.disk_uuid, rel_path: c.rel_path, size: c.size }));
          return { rel_path: z.rel_path, heil: ok.slice(0, 3) };
        });
        return json({ disk: dn.get(disk) || disk, items, total: zs.length, healable: items.filter((x) => x.heil.length).length });
      }
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
