/**
 * NXT Scanner Ingest-Worker.
 *
 * POST /ingest  — Bearer-Auth (Secret INGEST_TOKEN), Body {disk, folder_tree}.
 *   Schreibt disks + folder_tree idempotent (delete+insert je disk_uuid) nach D1.
 * GET  /health  — Liveness.
 *
 * POST /notion-sync — Bearer-Auth. Notion-Spiegel aus D1 anstoßen
 *   (?dry=1 = nur planen, nichts schreiben; ?disk=<uuid|name> = nur diese Platte).
 * Cron (wrangler.toml) — alle 15 min Notion-Sync für Platten mit neuem Scan.
 *
 * Secrets kommen ausschliesslich aus env (wrangler secret put INGEST_TOKEN) —
 * niemals im Repo.
 */

import { runNotionSync } from "./notion_sync.js";
import { deriveIfChanged } from "./derive.js";

const json = (obj, status = 200) =>
  new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json" },
  });

function authorized(request, env) {
  const auth = request.headers.get("authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  return env.INGEST_TOKEN && token && token === env.INGEST_TOKEN;
}

// ── Rollout-Status: Notion (Referenz) vs. D1 (neue Version) ──────────
function normName(s) {
  return (s || "").toUpperCase().replace(/[_\-\s]+/g, " ").trim();
}

async function fetchNotionDisks(env) {
  const headers = {
    Authorization: `Bearer ${env.NOTION_TOKEN}`,
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
  };
  const disks = [];
  let cursor = undefined;
  do {
    const body = cursor ? { page_size: 100, start_cursor: cursor } : { page_size: 100 };
    const resp = await fetch(`https://api.notion.com/v1/databases/${env.HDD_DB_ID}/query`, {
      method: "POST", headers, body: JSON.stringify(body),
    });
    if (!resp.ok) throw new Error(`Notion ${resp.status}`);
    const data = await resp.json();
    for (const p of data.results) {
      const pr = p.properties || {};
      const title = (pr["Name"]?.title || []).map((t) => t.plain_text).join("");
      const user = pr["Zuletzt genutzt von"]?.select?.name || "";
      const uuid = (pr["Volume UUID"]?.rich_text || []).map((t) => t.plain_text).join("");
      const scan = pr["Letzter Scan"]?.date?.start || "";
      disks.push({ name: title, user, uuid, last_scan: scan });
    }
    cursor = data.has_more ? data.next_cursor : undefined;
  } while (cursor);
  return disks;
}

async function buildRollout(env) {
  const [notion, d1res, fleetres] = await Promise.all([
    fetchNotionDisks(env),
    env.DB.prepare("SELECT uuid, name, last_user, last_scan FROM disks").all(),
    env.DB.prepare("SELECT label, status, alias_of FROM fleet_disk").all(),
  ]);
  const d1 = d1res.results || [];
  const d1ByUuid = new Set(d1.map((d) => (d.uuid || "").toUpperCase()));
  const d1ByName = new Set(d1.map((d) => normName(d.name)));

  // Fleet-Registry: ausgemusterte ausschließen, Aliase auf gescannten Namen abbilden
  const fleet = {};
  for (const f of fleetres.results || []) fleet[normName(f.label)] = f;

  const disks = notion
    .filter((n) => (fleet[normName(n.name)]?.status) !== "retired")
    .map((n) => {
      const reg = fleet[normName(n.name)];
      const aliasDone = reg?.status === "alias" && d1ByName.has(normName(reg.alias_of));
      const done =
        (n.uuid && d1ByUuid.has(n.uuid.toUpperCase())) ||
        d1ByName.has(normName(n.name)) ||
        aliasDone;
      return { ...n, done };
    });
  disks.sort((a, b) => (a.name || "").localeCompare(b.name || "", "de", { numeric: true }));

  const byUser = {};
  for (const d of disks) {
    const u = d.user || "—";
    byUser[u] = byUser[u] || { total: 0, done: 0 };
    byUser[u].total++;
    if (d.done) byUser[u].done++;
  }
  return {
    generated_at: new Date().toISOString(),
    total: disks.length,
    done: disks.filter((d) => d.done).length,
    d1_count: d1.length,
    by_user: byUser,
    disks,
  };
}

// ── Datendashboard: Live-Aggregation über die D1 (bleibend) ──────────
async function buildDashboard(env) {
  const [disksR, projR, fleetR] = await Promise.all([
    env.DB.prepare("SELECT uuid,name,used_bytes,capacity_bytes,last_scan,last_user FROM disks").all(),
    env.DB.prepare("SELECT cluster_id,status,type,size_bytes,name,disk_uuid FROM projects").all(),
    env.DB.prepare("SELECT label,status FROM fleet_disk").all(),
  ]);
  const disks = disksR.results || [], proj = projR.results || [], fleet = fleetR.results || [];
  const TB = 1e12;

  let val = 0, unc = 0, cont = 0;
  for (const p of proj) {
    const b = p.size_bytes || 0;
    if (p.status === "validated") val += b;
    else if (p.status === "container") cont += b;
    else unc += b;
  }
  const usedTotal = disks.reduce((a, d) => a + (d.used_bytes || 0), 0);

  // Kopien je Cluster (nur validierte, Kopien = verschiedene Platten)
  const cl = {}, clName = {};
  for (const p of proj) {
    if (p.status !== "validated" || !p.cluster_id) continue;
    const c = (cl[p.cluster_id] = cl[p.cluster_id] || { disks: new Set(), size: {} });
    c.disks.add(p.disk_uuid);
    c.size[p.disk_uuid] = Math.max(c.size[p.disk_uuid] || 0, p.size_bytes || 0);
    clName[p.cluster_id] = p.name;
  }
  const hist = { "1": 0, "2": 0, "3+": 0 };
  let unsecuredTB = 0, excessTB = 0;
  const singles = [];
  for (const k in cl) {
    const ds = cl[k].disks.size;
    const sizes = Object.values(cl[k].size).sort((a, b) => a - b);
    const med = sizes[Math.floor(sizes.length / 2)] || 0;
    if (ds === 1) { hist["1"]++; unsecuredTB += med; singles.push({ name: clName[k], gb: med / 1e9 }); }
    else if (ds === 2) { hist["2"]++; }
    else { hist["3+"]++; excessTB += (ds - 2) * med; }
  }
  singles.sort((a, b) => b.gb - a.gb);

  const users = {};
  for (const d of disks) {
    const u = d.last_user || "—";
    (users[u] = users[u] || { n: 0, tb: 0 });
    users[u].n++; users[u].tb += (d.used_bytes || 0) / TB;
  }
  const reg = {};
  for (const f of fleet) reg[f.status] = (reg[f.status] || 0) + 1;

  const now = Date.now();
  let fresh7 = 0, lastScan = 0;
  for (const d of disks) {
    const t = Date.parse(d.last_scan || "") || 0;
    if (t > lastScan) lastScan = t;
    if (now - t < 7 * 864e5) fresh7++;
  }

  return {
    generated_at: new Date().toISOString(),
    disks: disks.length,
    used_tb: round1(usedTotal / TB), validated_tb: round1(val / TB),
    unclassified_tb: round1(unc / TB), container_tb: round1(cont / TB),
    projects: Object.keys(cl).length,
    copies: hist, unsecured_tb: round1(unsecuredTB / TB), excess_tb: round1(excessTB / TB),
    top_unsecured: singles.slice(0, 12).map((s) => ({ name: s.name, gb: Math.round(s.gb) })),
    by_user: Object.entries(users).map(([u, v]) => ({ user: u, n: v.n, tb: round1(v.tb) })).sort((a, b) => b.tb - a.tb),
    registry: reg, fresh_7d: fresh7,
    last_scan: lastScan ? new Date(lastScan).toISOString() : null,
  };
}
const round1 = (x) => Math.round(x * 10) / 10;

// ── Live-DIT-Board: aktive Projekte + Ingest-Sessions je Karte ──────
function boardStatus(row) {
  if (row.state === "error" || row.verify_status === "mismatch") return "error";
  if ((row.copies_done || 0) >= 2 && row.verify_status === "verified") return "2x_verified";
  if ((row.copies_done || 0) === 1) return "1x";
  return "unsecured";
}

async function buildBoard(env) {
  const [projR, sessR] = await Promise.all([
    env.DB.prepare(
      "SELECT id,name,date,client,ff_notion_url,created_by FROM project_meta WHERE status='active' ORDER BY date DESC, created_at DESC"
    ).all(),
    env.DB.prepare(
      `SELECT id,project_id,card_label,source_bytes,source_files,target_a_disk,target_b_disk,
              copies_done,verify_status,state,started_at,finished_at,host
       FROM ingest_session ORDER BY started_at DESC`
    ).all(),
  ]);
  const projects = projR.results || [];
  const sessions = sessR.results || [];

  const byProject = {};
  for (const p of projects) byProject[p.id] = { ...p, sessions: [] };
  const orphans = [];
  for (const s of sessions) {
    const row = { ...s, status: boardStatus(s) };
    if (byProject[s.project_id]) byProject[s.project_id].sessions.push(row);
    else orphans.push(row);
  }

  const counts = { unsecured: 0, "1x": 0, "2x_verified": 0, error: 0 };
  for (const s of sessions) counts[boardStatus(s)]++;

  return {
    generated_at: new Date().toISOString(),
    projects: Object.values(byProject),
    orphan_sessions: orphans,
    session_total: sessions.length,
    counts,
  };
}

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil((async () => {
      try {
        const d = await deriveIfChanged(env); // projects/cluster fürs Dashboard aktuell halten
        if (!d.skipped) console.log("derive", JSON.stringify(d));
      } catch (e) { console.error("derive failed", String(e)); }
      try {
        const r = await runNotionSync(env);
        console.log("notion-sync", JSON.stringify({ synced: r.disks.length, pending: r.pending, calls: r.calls }));
      } catch (e) { console.error("notion-sync failed", String(e)); }
    })());
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/notion-sync" && request.method === "POST") {
      if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);
      try {
        const r = await runNotionSync(env, {
          dry: url.searchParams.get("dry") === "1",
          onlyDisk: url.searchParams.get("disk"),
          maxCalls: Number(url.searchParams.get("max_calls")) || 900,
        });
        return json(r);
      } catch (e) {
        return json({ error: "notion-sync", detail: String(e) }, 500);
      }
    }

    if (url.pathname === "/health") {
      return json({ ok: true });
    }

    // ── Datendashboard (bleibend, read-only, mit VIEW_TOKEN) ──
    if (url.pathname === "/dashboard.json") {
      if (url.searchParams.get("key") !== env.VIEW_TOKEN) {
        return json({ error: "unauthorized" }, 401);
      }
      try {
        return json(await buildDashboard(env));
      } catch (e) {
        return json({ error: "dashboard", detail: String(e) }, 500);
      }
    }
    if (url.pathname === "/dashboard") {
      return new Response(DASHBOARD_HTML, {
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }

    // ── Live-DIT-Board (VIEW_TOKEN-gated wie Dashboard) ──
    if (url.pathname === "/board.json") {
      if (url.searchParams.get("key") !== env.VIEW_TOKEN) {
        return json({ error: "unauthorized" }, 401);
      }
      try {
        return json(await buildBoard(env));
      } catch (e) {
        return json({ error: "board", detail: String(e) }, 500);
      }
    }
    if (url.pathname === "/board") {
      return new Response(BOARD_HTML, {
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }

    // ── Projekt/Session-Ingest (Bearer INGEST_TOKEN, upsert) ──
    if (url.pathname === "/ingest/project" && request.method === "POST") {
      if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);
      let p;
      try { p = await request.json(); } catch { return json({ error: "invalid json" }, 400); }
      if (!p || !p.id || !p.name) return json({ error: "missing id/name" }, 400);
      try {
        await env.DB.prepare(
          `INSERT INTO project_meta(id,name,date,client,ff_notion_url,created_by,created_at,status)
           VALUES(?,?,?,?,?,?,?,?)
           ON CONFLICT(id) DO UPDATE SET
             name=excluded.name, date=excluded.date, client=excluded.client,
             ff_notion_url=excluded.ff_notion_url, created_by=excluded.created_by,
             status=excluded.status`
        ).bind(
          p.id, p.name, p.date ?? null, p.client ?? null, p.ff_notion_url ?? null,
          p.created_by ?? null, p.created_at ?? null, p.status ?? "active"
        ).run();
      } catch (e) {
        return json({ error: "db", detail: String(e) }, 500);
      }
      return json({ ok: true, project: p.id });
    }

    if (url.pathname === "/ingest/session" && request.method === "POST") {
      if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);
      let s;
      try { s = await request.json(); } catch { return json({ error: "invalid json" }, 400); }
      if (!s || !s.id || !s.project_id) return json({ error: "missing id/project_id" }, 400);
      try {
        await env.DB.prepare(
          `INSERT INTO ingest_session(id,project_id,card_label,card_uuid,source_bytes,source_files,
             target_a_disk,target_b_disk,copies_done,verify_status,state,started_at,finished_at,host)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(id) DO UPDATE SET
             card_label=excluded.card_label, card_uuid=excluded.card_uuid,
             source_bytes=excluded.source_bytes, source_files=excluded.source_files,
             target_a_disk=excluded.target_a_disk, target_b_disk=excluded.target_b_disk,
             copies_done=excluded.copies_done, verify_status=excluded.verify_status,
             state=excluded.state, finished_at=excluded.finished_at, host=excluded.host`
        ).bind(
          s.id, s.project_id, s.card_label ?? null, s.card_uuid ?? null,
          s.source_bytes ?? null, s.source_files ?? null,
          s.target_a_disk ?? null, s.target_b_disk ?? null,
          s.copies_done ?? 0, s.verify_status ?? "pending", s.state ?? "detected",
          s.started_at ?? null, s.finished_at ?? null, s.host ?? null
        ).run();
      } catch (e) {
        return json({ error: "db", detail: String(e) }, 500);
      }
      return json({ ok: true, session: s.id });
    }

    // ── Rollout-Tracking abgeschlossen (offline genommen 2026-08). ──
    // buildRollout()/ROLLOUT_HTML bleiben im Code erhalten, um es bei Bedarf
    // (neue Datenträger) mit einem Route-Eintrag wieder zu aktivieren.
    // ── Datei-Katalog (Bearer INGEST_TOKEN): Änderungen je Datenträger in Paketen ──
    // Body: {disk_uuid, upserts: [[rel_path, name, size, mtime, kind], ...], deletes: [rel_path, ...], totals?: {files, bytes}, host?}
    if (url.pathname === "/ingest/files" && request.method === "POST") {
      if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);
      let b;
      try { b = await request.json(); } catch { return json({ error: "invalid json" }, 400); }
      const disk = typeof b?.disk_uuid === "string" ? b.disk_uuid.slice(0, 100) : "";
      const ups = Array.isArray(b?.upserts) ? b.upserts : [];
      const dels = Array.isArray(b?.deletes) ? b.deletes : [];
      if (!disk || disk.startsWith("name:") || ups.length + dels.length > 5000) return json({ error: "invalid batch" }, 400);
      const stmts = [];
      // json_each: ein Parameter pro Statement statt 100er-Grenze für gebundene Werte
      for (let i = 0; i < ups.length; i += 1000) {
        stmts.push(env.CATALOG.prepare(
          `INSERT INTO files(disk_uuid, rel_path, name, size, mtime, kind)
           SELECT ?1, j.value->>0, j.value->>1, j.value->>2, j.value->>3, j.value->>4 FROM json_each(?2) j WHERE true
           ON CONFLICT(disk_uuid, rel_path) DO UPDATE SET name=excluded.name, size=excluded.size,
             mtime=excluded.mtime, kind=excluded.kind`).bind(disk, JSON.stringify(ups.slice(i, i + 1000))));
      }
      for (let i = 0; i < dels.length; i += 1000) {
        stmts.push(env.CATALOG.prepare(
          "DELETE FROM files WHERE disk_uuid=?1 AND rel_path IN (SELECT value FROM json_each(?2))")
          .bind(disk, JSON.stringify(dels.slice(i, i + 1000))));
      }
      if (b.totals) {
        stmts.push(env.CATALOG.prepare(
          `INSERT INTO disk_sync(disk_uuid, files, bytes, synced_at, host) VALUES(?,?,?,?,?)
           ON CONFLICT(disk_uuid) DO UPDATE SET files=excluded.files, bytes=excluded.bytes, synced_at=excluded.synced_at, host=excluded.host`)
          .bind(disk, Math.round(b.totals.files || 0), Math.round(b.totals.bytes || 0), new Date().toISOString(), String(b.host || "").slice(0, 100)));
      }
      try {
        if (stmts.length) await env.CATALOG.batch(stmts);
      } catch (e) {
        return json({ error: "db", detail: String(e) }, 500);
      }
      return json({ ok: true, upserts: ups.length, deletes: dels.length });
    }

    // ── Fortschritt laufender Arbeiten (Bearer INGEST_TOKEN), nur Anzeige ──
    if (url.pathname === "/ingest/progress" && request.method === "POST") {
      if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);
      let p;
      try { p = await request.json(); } catch { return json({ error: "invalid json" }, 400); }
      if (!p?.job_id || !["running", "done", "failed"].includes(p.state)) return json({ error: "invalid progress" }, 400);
      const n = (v) => (Number.isFinite(v) ? Math.round(v) : null);
      const t = (v, m = 200) => (v == null ? null : String(v).slice(0, m));
      await env.DB.prepare(
        `INSERT INTO job_progress(job_id, host, kind, disk_uuid, disk_name, started_at, updated_at, done_files, done_bytes,
           total_files, total_bytes, state, message) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
         ON CONFLICT(job_id) DO UPDATE SET started_at=excluded.started_at, updated_at=excluded.updated_at,
           done_files=excluded.done_files, done_bytes=excluded.done_bytes, total_files=excluded.total_files,
           total_bytes=excluded.total_bytes, state=excluded.state, message=excluded.message, disk_name=excluded.disk_name`
      ).bind(t(p.job_id, 300), t(p.host, 100), t(p.kind, 20), t(p.disk_uuid, 100), t(p.disk_name, 100), t(p.started_at, 40),
             new Date().toISOString(), n(p.done_files), n(p.done_bytes), n(p.total_files), n(p.total_bytes), p.state, t(p.message)).run();
      return json({ ok: true });
    }

    // ── Platten-Check (Bearer INGEST_TOKEN): letzter Zustand je Datenträger ──
    if (url.pathname === "/ingest/health" && request.method === "POST") {
      if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);
      let h;
      try { h = await request.json(); } catch { return json({ error: "invalid json" }, 400); }
      if (!h?.disk_uuid || !["ok", "warnung", "defekt", "unbekannt"].includes(h.verdict)) return json({ error: "invalid health" }, 400);
      const sm = h.smart || {}, fs = h.fs || {}, rd = h.read || {};
      const n = (v) => (Number.isFinite(v) ? Math.round(v) : null);
      const t = (v, max = 500) => (v == null ? null : String(v).slice(0, max));
      await env.DB.prepare(
        `INSERT INTO disk_health(disk_uuid, checked_at, host, verdict, smart_status, smart_reason, model, wear_pct, spare_pct,
           media_errors, power_on_hours, temperature, fs_status, fs_reason, read_status, read_errors, read_reason)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
         ON CONFLICT(disk_uuid) DO UPDATE SET checked_at=excluded.checked_at, host=excluded.host, verdict=excluded.verdict,
           smart_status=excluded.smart_status, smart_reason=excluded.smart_reason, model=COALESCE(excluded.model, disk_health.model),
           wear_pct=excluded.wear_pct, spare_pct=excluded.spare_pct, media_errors=excluded.media_errors,
           power_on_hours=excluded.power_on_hours, temperature=excluded.temperature,
           fs_status=COALESCE(excluded.fs_status, disk_health.fs_status), fs_reason=COALESCE(excluded.fs_reason, disk_health.fs_reason),
           read_status=COALESCE(excluded.read_status, disk_health.read_status), read_errors=COALESCE(excluded.read_errors, disk_health.read_errors),
           read_reason=COALESCE(excluded.read_reason, disk_health.read_reason)`
      ).bind(t(h.disk_uuid, 100), t(h.checked_at, 40), t(h.host, 100), h.verdict, t(sm.status, 20), t(sm.reason), t(sm.model, 100),
             n(sm.wear_pct), n(sm.spare_pct), n(sm.media_errors), n(sm.power_on_hours), n(sm.temperature),
             t(fs.status, 20), t(fs.reason), t(rd.status, 20), n(rd.errors), t(rd.reason)).run();
      return json({ ok: true });
    }

    if (url.pathname === "/rollout" || url.pathname === "/rollout.json") {
      return new Response("Rollout-Tracking abgeschlossen – Seite offline.", {
        status: 410,
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    }

    if (url.pathname === "/ingest" && request.method === "POST") {
      if (!authorized(request, env)) {
        return json({ error: "unauthorized" }, 401);
      }

      let payload;
      try {
        payload = await request.json();
      } catch {
        return json({ error: "invalid json" }, 400);
      }

      const disk = payload.disk;
      const tree = Array.isArray(payload.folder_tree) ? payload.folder_tree : [];
      if (!disk || !disk.uuid) {
        return json({ error: "missing disk.uuid" }, 400);
      }
      const uuid = disk.uuid;

      const stmts = [];

      // disks upsert
      stmts.push(
        env.DB.prepare(
          `INSERT INTO disks(uuid,name,fs_type,is_network,used_bytes,capacity_bytes,last_scan,last_user,fs_used_bytes)
           VALUES(?,?,?,?,?,?,?,?,?)
           ON CONFLICT(uuid) DO UPDATE SET
             name=excluded.name, fs_type=excluded.fs_type, is_network=excluded.is_network,
             used_bytes=excluded.used_bytes,
             capacity_bytes=COALESCE(excluded.capacity_bytes, disks.capacity_bytes),
             fs_used_bytes=COALESCE(excluded.fs_used_bytes, disks.fs_used_bytes),
             last_scan=excluded.last_scan, last_user=excluded.last_user`
        ).bind(
          uuid, disk.name ?? null, disk.fs_type ?? null, disk.is_network ?? null,
          disk.used_bytes ?? null, disk.capacity_bytes ?? null,
          disk.last_scan ?? null, disk.last_user ?? null, disk.fs_used_bytes ?? null
        )
      );

      // delete+insert folder_tree je disk
      stmts.push(env.DB.prepare(`DELETE FROM folder_tree WHERE disk_uuid=?`).bind(uuid));
      for (const n of tree) {
        stmts.push(
          env.DB.prepare(
            `INSERT INTO folder_tree(disk_uuid,rel_path,depth,parent_rel_path,size_bytes,file_count,mtime,abs_path)
             VALUES(?,?,?,?,?,?,?,?)`
          ).bind(
            uuid, n.rel_path, n.depth ?? null, n.parent_rel_path ?? null,
            n.size_bytes ?? null, n.file_count ?? null, n.mtime ?? null, n.abs_path ?? null
          )
        );
      }

      try {
        await env.DB.batch(stmts);
      } catch (e) {
        return json({ error: "db", detail: String(e) }, 500);
      }
      // Projekt-Zuordnung sofort neu rechnen, damit das Dashboard nicht auf den Cron wartet
      ctx.waitUntil(deriveIfChanged(env).catch((e) => console.error("derive nach ingest", String(e))));

      return json({ ok: true, disk: uuid, nodes: tree.length });
    }

    return json({ error: "not found" }, 404);
  },
};

// ── Selbst-aktualisierende Rollout-Seite (same-origin, holt /rollout.json) ──
const ROLLOUT_HTML = `<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NXT Scanner — Rollout-Status</title>
<style>
  :root{--bg:#f4f6f9;--surface:#fff;--surface2:#eceff4;--line:#d8dee7;--ink:#1a2230;--soft:#5b677a;--faint:#8b97a8;--accent:#0e8f8a;--done:#2c9d5b;--miss:#d3455b;--miss-bg:#fbe9ec;--done-bg:#e7f5ee;--mono:ui-monospace,"SF Mono",Menlo,Consolas,monospace;--sans:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
  @media (prefers-color-scheme:dark){:root{--bg:#0d1017;--surface:#161b24;--surface2:#1d2431;--line:#29323f;--ink:#e6ebf2;--soft:#97a3b4;--faint:#66707f;--accent:#3ad6c8;--done:#45c37a;--miss:#f26a7a;--miss-bg:#2a1620;--done-bg:#14271c}}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sans);-webkit-font-smoothing:antialiased;line-height:1.5}
  .wrap{max-width:1080px;margin:0 auto;padding:34px 24px 60px}
  .num{font-family:var(--mono);font-variant-numeric:tabular-nums}
  header{display:flex;flex-wrap:wrap;align-items:flex-end;justify-content:space-between;gap:16px;border-bottom:1px solid var(--line);padding-bottom:18px}
  h1{font-size:19px;margin:0;font-weight:650;letter-spacing:-.01em}
  .sub{color:var(--soft);font-size:13px;margin-top:2px}
  .status{font-size:12px;color:var(--faint);text-align:right}
  .dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--done);margin-right:5px;vertical-align:middle}
  .dot.stale{background:var(--faint)}
  .big{display:flex;align-items:baseline;gap:14px;margin:26px 0 6px}
  .big .n{font-size:46px;font-weight:650;letter-spacing:-.02em;line-height:1}
  .big .of{font-size:20px;color:var(--faint)}
  .big .pct{margin-left:auto;font-size:15px;color:var(--soft)}
  .bar{height:14px;border-radius:7px;background:var(--surface2);overflow:hidden;border:1px solid var(--line)}
  .bar>span{display:block;height:100%;background:var(--done);border-radius:7px;transition:width .5s}
  h2{font-size:13px;text-transform:uppercase;letter-spacing:.05em;color:var(--faint);margin:34px 0 12px}
  .users{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px}
  .u{background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:13px 15px}
  .u .name{font-size:13px;font-weight:600;display:flex;justify-content:space-between}
  .u .name .c{font-family:var(--mono);color:var(--soft);font-weight:500}
  .u .t{height:8px;border-radius:5px;background:var(--surface2);overflow:hidden;margin-top:9px}
  .u .t>span{display:block;height:100%;background:var(--accent);border-radius:5px}
  .toolbar{display:flex;align-items:center;gap:14px;margin:32px 0 10px}
  .toolbar h2{margin:0}
  .toggle{margin-left:auto;font-size:13px;color:var(--soft);display:flex;align-items:center;gap:7px;cursor:pointer;user-select:none}
  table{width:100%;border-collapse:collapse;font-size:13.5px}
  thead th{text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--faint);font-weight:600;padding:8px 10px;border-bottom:1px solid var(--line)}
  tbody td{padding:9px 10px;border-bottom:1px solid var(--line)}
  tbody tr:hover{background:var(--surface)}
  td.name{font-weight:550}
  .pill{font-size:11px;font-weight:650;padding:2px 9px;border-radius:999px;white-space:nowrap}
  .pill.done{background:var(--done-bg);color:var(--done)}
  .pill.miss{background:var(--miss-bg);color:var(--miss)}
  td.scan{font-family:var(--mono);color:var(--faint);font-size:12px}
  .err{background:var(--miss-bg);color:var(--miss);padding:16px;border-radius:12px;margin-top:24px;font-size:14px}
  @media(max-width:640px){td.scan,th.scan{display:none}}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div><h1>NXT Scanner — Rollout-Status</h1><div class="sub">Datenträger in der neuen Version (zentrale DB) vs. Notion</div></div>
    <div class="status" id="status">lade…</div>
  </header>
  <div id="content"></div>
</div>
<script>
  var KEY = new URLSearchParams(location.search).get("key") || "";
  var REFRESH = 20000;
  var lastData = null;

  function esc(s){var d=document.createElement("div");d.textContent=s==null?"":String(s);return d.innerHTML;}

  function render(d){
    lastData = d;
    var pct = d.total ? Math.round(d.done/d.total*100) : 0;
    var c = document.getElementById("content");
    var showMiss = document.getElementById("onlymiss") && document.getElementById("onlymiss").checked;

    var users = Object.keys(d.by_user).sort(function(a,b){return d.by_user[b].total-d.by_user[a].total;});
    var userHtml = users.map(function(u){
      var o=d.by_user[u]; var p=o.total?Math.round(o.done/o.total*100):0;
      return '<div class="u"><div class="name"><span>'+esc(u)+'</span><span class="c">'+o.done+'/'+o.total+'</span></div>'+
             '<div class="t"><span style="width:'+p+'%"></span></div></div>';
    }).join("");

    var rows = d.disks.filter(function(x){return !showMiss || !x.done;}).map(function(x){
      var pill = x.done ? '<span class="pill done">✓ in neuer Version</span>' : '<span class="pill miss">fehlt noch</span>';
      return '<tr><td class="name">'+esc(x.name)+'</td><td>'+esc(x.user||"—")+'</td><td>'+pill+
             '</td><td class="scan">'+esc((x.last_scan||"").slice(0,10))+'</td></tr>';
    }).join("");

    c.innerHTML =
      '<div class="big"><span class="n num">'+d.done+'</span><span class="of num">/ '+d.total+' Datenträger</span>'+
      '<span class="pct num">'+pct+'% in der neuen Version</span></div>'+
      '<div class="bar"><span style="width:'+pct+'%"></span></div>'+
      '<h2>Fortschritt pro Person</h2><div class="users">'+userHtml+'</div>'+
      '<div class="toolbar"><h2>Alle Datenträger</h2>'+
      '<label class="toggle"><input type="checkbox" id="onlymiss"'+(showMiss?" checked":"")+'> nur fehlende</label></div>'+
      '<table><thead><tr><th>Datenträger</th><th>Zuletzt genutzt von</th><th>Status</th><th class="scan">Letzter Scan</th></tr></thead><tbody>'+rows+'</tbody></table>';

    document.getElementById("onlymiss").addEventListener("change",function(){render(lastData);});
  }

  function setStatus(txt,stale){
    document.getElementById("status").innerHTML='<span class="dot'+(stale?" stale":"")+'"></span>'+txt;
  }

  function load(){
    fetch("/rollout.json?key="+encodeURIComponent(KEY)).then(function(r){
      if(r.status===401){throw new Error("Kein/falscher Zugriffs-Key in der URL (…/rollout?key=…).");}
      if(!r.ok){throw new Error("Server "+r.status);}
      return r.json();
    }).then(function(d){
      render(d);
      var t=new Date(d.generated_at);
      setStatus("aktualisiert "+t.toLocaleTimeString("de-DE")+" · auto alle 20s",false);
    }).catch(function(e){
      document.getElementById("content").innerHTML='<div class="err">'+esc(e.message)+'</div>';
      setStatus("Fehler",true);
    });
  }
  load();
  setInterval(load,REFRESH);
</script>
</body>
</html>`;

// ── Bleibende Dashboard-Seite (same-origin, holt /dashboard.json) ──
const DASHBOARD_HTML = `<!doctype html>
<html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>NXT Scanner — Datendashboard</title>
<style>
  :root{--bg:#f4f6f9;--surface:#fff;--surface2:#eceff4;--line:#d8dee7;--line2:#e6ebf1;--ink:#1a2230;--soft:#5b677a;--faint:#8b97a8;--accent:#0e8f8a;--good:#2c9d5b;--goodb:#e7f5ee;--miss:#d3455b;--missb:#fbe9ec;--excess:#3672c4;--excessb:#e8f0fb;--warn:#c67c1a;--mono:ui-monospace,"SF Mono",Menlo,Consolas,monospace;--sans:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;--shadow:0 1px 2px rgba(20,30,50,.05),0 4px 14px rgba(20,30,50,.05)}
  @media(prefers-color-scheme:dark){:root{--bg:#0d1017;--surface:#161b24;--surface2:#1d2431;--line:#29323f;--line2:#212a36;--ink:#e6ebf2;--soft:#97a3b4;--faint:#66707f;--accent:#3ad6c8;--good:#45c37a;--goodb:#14271c;--miss:#f26a7a;--missb:#2a1620;--excess:#5b9bf0;--excessb:#14213a;--warn:#e0a63a;--shadow:0 1px 2px rgba(0,0,0,.3),0 6px 20px rgba(0,0,0,.28)}}
  :root[data-theme="dark"]{--bg:#0d1017;--surface:#161b24;--surface2:#1d2431;--line:#29323f;--line2:#212a36;--ink:#e6ebf2;--soft:#97a3b4;--faint:#66707f;--accent:#3ad6c8;--good:#45c37a;--goodb:#14271c;--miss:#f26a7a;--missb:#2a1620;--excess:#5b9bf0;--excessb:#14213a;--warn:#e0a63a}
  :root[data-theme="light"]{--bg:#f4f6f9;--surface:#fff;--surface2:#eceff4;--line:#d8dee7;--line2:#e6ebf1;--ink:#1a2230;--soft:#5b677a;--faint:#8b97a8;--accent:#0e8f8a;--good:#2c9d5b;--goodb:#e7f5ee;--miss:#d3455b;--missb:#fbe9ec;--excess:#3672c4;--excessb:#e8f0fb;--warn:#c67c1a}
  *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sans);-webkit-font-smoothing:antialiased;line-height:1.45}
  .num{font-family:var(--mono);font-variant-numeric:tabular-nums}
  .wrap{max-width:1120px;margin:0 auto;padding:30px 22px 64px}
  header{display:flex;flex-wrap:wrap;align-items:flex-end;justify-content:space-between;gap:16px;border-bottom:1px solid var(--line);padding-bottom:18px}
  h1{font-size:20px;margin:0;font-weight:650;letter-spacing:-.01em}
  .sub{color:var(--soft);font-size:13px;margin-top:3px}
  .status{font-size:12px;color:var(--faint);text-align:right}
  .dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--good);margin-right:5px;vertical-align:middle}
  .dot.stale{background:var(--faint)}
  .kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:22px 0}
  @media(max-width:720px){.kpis{grid-template-columns:repeat(2,1fr)}}
  .kpi{background:var(--surface);border:1px solid var(--line);border-radius:13px;padding:16px 17px;box-shadow:var(--shadow)}
  .kpi .v{font-size:27px;font-weight:650;letter-spacing:-.02em;line-height:1}
  .kpi .v .u{font-size:15px;color:var(--faint);font-weight:500;margin-left:3px}
  .kpi .l{font-size:12px;color:var(--soft);margin-top:5px}
  h2{font-size:13px;text-transform:uppercase;letter-spacing:.05em;color:var(--faint);margin:30px 0 12px}
  .card{background:var(--surface);border:1px solid var(--line);border-radius:13px;padding:18px;box-shadow:var(--shadow)}
  .cols{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  @media(max-width:760px){.cols{grid-template-columns:1fr}}
  .estimate{font-size:11px;font-weight:650;color:var(--warn);border:1px solid var(--warn);border-radius:999px;padding:1px 8px;margin-left:8px;text-transform:none;letter-spacing:0}
  .bar{display:flex;height:34px;border-radius:8px;overflow:hidden;border:1px solid var(--line)}
  .bar>div{display:flex;align-items:center;justify-content:center;color:#fff;font-family:var(--mono);font-size:12px;font-weight:600;min-width:30px}
  .legend{display:flex;flex-wrap:wrap;gap:16px;margin-top:14px;font-size:12.5px;color:var(--soft)}
  .lg{display:flex;align-items:center;gap:7px}.sw{width:11px;height:11px;border-radius:3px}
  .row{display:grid;grid-template-columns:150px 1fr 96px;align-items:center;gap:12px;padding:7px 0;border-bottom:1px solid var(--line2)}
  .row:last-child{border-bottom:0}.row .n{font-size:13px}.row .n small{display:block;color:var(--faint);font-size:11px}
  .track{height:20px;background:var(--surface2);border-radius:6px;overflow:hidden}.track>span{display:block;height:100%;border-radius:6px}
  .row .val{text-align:right;font-family:var(--mono);font-size:13px;font-weight:600}
  .li{display:grid;grid-template-columns:1fr auto;gap:10px;padding:8px 0;border-bottom:1px solid var(--line2);font-size:13px}
  .li:last-child{border-bottom:0}.li .sz{font-family:var(--mono);font-weight:600;color:var(--miss)}
  .li .nm{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .chips{display:flex;flex-wrap:wrap;gap:10px}
  .chip{background:var(--surface2);border-radius:8px;padding:8px 12px;font-size:12.5px}.chip b{font-family:var(--mono)}
  .err{background:var(--missb);color:var(--miss);padding:16px;border-radius:12px;margin-top:24px}
</style></head><body>
<div class="wrap">
  <header>
    <div><h1>NXT Scanner — Datendashboard</h1><div class="sub">Live aus der zentralen Datenbank · aktualisiert automatisch</div></div>
    <div class="status" id="status">lade…</div>
  </header>
  <div id="content"></div>
</div>
<script>
  var KEY=new URLSearchParams(location.search).get("key")||"";
  function tb(x){return x>=1000?(x/1000).toFixed(1)+" PB":x.toFixed(1)+" TB";}
  function esc(s){var d=document.createElement("div");d.textContent=s==null?"":String(s);return d.innerHTML;}
  function render(d){
    var c=document.getElementById("content");
    var cop=d.copies||{}, c1=cop["1"]||0,c2=cop["2"]||0,c3=cop["3+"]||0, ctot=c1+c2+c3||1;
    var st=d.validated_tb+d.unclassified_tb+d.container_tb||1;
    var users=(d.by_user||[]), umax=Math.max.apply(null,users.map(function(u){return u.tb;}).concat([1]));
    var reg=d.registry||{};
    c.innerHTML=
     '<div class="kpis">'+
      '<div class="kpi"><div class="v num">'+d.disks+'</div><div class="l">physische Datenträger</div></div>'+
      '<div class="kpi"><div class="v num">'+tb(d.used_tb)+'</div><div class="l">belegt gesamt</div></div>'+
      '<div class="kpi"><div class="v num">'+d.projects+'</div><div class="l">eindeutige Projekte</div></div>'+
      '<div class="kpi"><div class="v num">'+d.fresh_7d+'</div><div class="l">gescannt (letzte 7 Tage)</div></div>'+
     '</div>'+
     '<h2>Speicher-Aufteilung</h2>'+
     '<div class="card"><div class="bar">'+
       seg(d.validated_tb/st, "var(--accent)", d.validated_tb)+
       seg(d.unclassified_tb/st, "var(--miss)", d.unclassified_tb)+
       seg(d.container_tb/st, "var(--faint)", d.container_tb)+
     '</div><div class="legend">'+
       lg("var(--accent)","validierte Projekte "+tb(d.validated_tb))+
       lg("var(--miss)","unklassifiziert "+tb(d.unclassified_tb))+
       lg("var(--faint)","Container "+tb(d.container_tb))+
     '</div></div>'+
     '<h2>Backup-Gesundheit <span class="estimate">Schätzung (Namens-Cluster)</span></h2>'+
     '<div class="cols"><div class="card">'+
       '<div class="bar">'+
         seg(c1/ctot,"var(--miss)",null,c1+" ungesichert")+
         seg(c2/ctot,"var(--good)",null,c2+" optimal")+
         seg(c3/ctot,"var(--excess)",null,c3+" mehrfach")+
       '</div><div class="legend">'+
         lg("var(--miss)","1 Kopie — "+c1)+lg("var(--good)","2 Kopien — "+c2)+lg("var(--excess)","3+ Kopien — "+c3)+
       '</div>'+
       '<div style="display:flex;gap:24px;margin-top:14px">'+
         '<div><div class="num" style="font-size:20px;font-weight:650;color:var(--miss)">'+tb(d.unsecured_tb)+'</div><div class="l" style="font-size:12px;color:var(--soft)">ungesichert (nur 1 Kopie)</div></div>'+
         '<div><div class="num" style="font-size:20px;font-weight:650;color:var(--excess)">'+tb(d.excess_tb)+'</div><div class="l" style="font-size:12px;color:var(--soft)">überzählig (löschbar)</div></div>'+
       '</div></div>'+
       '<div class="card"><div style="font-size:12px;color:var(--faint);text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px">Größte ungesicherte Projekte</div>'+
        (d.top_unsecured||[]).map(function(x){return '<div class="li"><span class="nm">'+esc(x.name)+'</span><span class="sz">'+x.gb+' GB</span></div>';}).join("")+
       '</div>'+
     '</div>'+
     '<h2>Pro Person</h2><div class="card">'+
       users.map(function(u){return '<div class="row"><div class="n">'+esc(u.user)+'<small>'+u.n+' Datenträger</small></div>'+
         '<div class="track"><span style="width:'+(u.tb/umax*100).toFixed(1)+'%;background:var(--accent)"></span></div>'+
         '<div class="val">'+tb(u.tb)+'</div></div>';}).join("")+
     '</div>'+
     '<h2>Fleet-Registry</h2><div class="card"><div class="chips">'+
       (Object.keys(reg).length? Object.keys(reg).map(function(k){return '<div class="chip">'+esc(k)+': <b>'+reg[k]+'</b></div>';}).join("") : '<span style="color:var(--faint);font-size:12.5px">keine Einträge</span>')+
     '</div></div>';
  }
  function seg(frac,color,tbval,label){var w=Math.max(0,frac*100);if(w<0.5)return "";return '<div style="flex:'+w+';background:'+color+'">'+(label?label:(tbval!=null?tb(tbval):""))+'</div>';}
  function lg(color,text){return '<span class="lg"><span class="sw" style="background:'+color+'"></span>'+text+'</span>';}
  function setStatus(t,stale){document.getElementById("status").innerHTML='<span class="dot'+(stale?" stale":"")+'"></span>'+t;}
  function load(){
    fetch("/dashboard.json?key="+encodeURIComponent(KEY)).then(function(r){
      if(r.status===401)throw new Error("Kein/falscher Zugriffs-Key in der URL (…/dashboard?key=…).");
      if(!r.ok)throw new Error("Server "+r.status);return r.json();
    }).then(function(d){render(d);var t=new Date(d.generated_at);setStatus("aktualisiert "+t.toLocaleTimeString("de-DE")+" · auto alle 60s",false);})
    .catch(function(e){document.getElementById("content").innerHTML='<div class="err">'+esc(e.message)+'</div>';setStatus("Fehler",true);});
  }
  load(); setInterval(load,60000);
</script></body></html>`;

// ── Live-DIT-Board (same-origin, holt /board.json alle 8s) ──
const BOARD_HTML = `<!doctype html>
<html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>NXT Scanner — DIT-Board</title>
<style>
  :root{--bg:#f4f6f9;--surface:#fff;--surface2:#eceff4;--line:#d8dee7;--line2:#e6ebf1;--ink:#1a2230;--soft:#5b677a;--faint:#8b97a8;--accent:#0e8f8a;--good:#2c9d5b;--goodb:#e7f5ee;--miss:#d3455b;--missb:#fbe9ec;--warn:#c67c1a;--warnb:#fdf1e0;--mono:ui-monospace,"SF Mono",Menlo,Consolas,monospace;--sans:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;--shadow:0 1px 2px rgba(20,30,50,.05),0 4px 14px rgba(20,30,50,.05)}
  @media(prefers-color-scheme:dark){:root{--bg:#0d1017;--surface:#161b24;--surface2:#1d2431;--line:#29323f;--line2:#212a36;--ink:#e6ebf2;--soft:#97a3b4;--faint:#66707f;--accent:#3ad6c8;--good:#45c37a;--goodb:#14271c;--miss:#f26a7a;--missb:#2a1620;--warn:#e0a63a;--warnb:#2a2114;--shadow:0 1px 2px rgba(0,0,0,.3),0 6px 20px rgba(0,0,0,.28)}}
  :root[data-theme="dark"]{--bg:#0d1017;--surface:#161b24;--surface2:#1d2431;--line:#29323f;--line2:#212a36;--ink:#e6ebf2;--soft:#97a3b4;--faint:#66707f;--accent:#3ad6c8;--good:#45c37a;--goodb:#14271c;--miss:#f26a7a;--missb:#2a1620;--warn:#e0a63a;--warnb:#2a2114}
  :root[data-theme="light"]{--bg:#f4f6f9;--surface:#fff;--surface2:#eceff4;--line:#d8dee7;--line2:#e6ebf1;--ink:#1a2230;--soft:#5b677a;--faint:#8b97a8;--accent:#0e8f8a;--good:#2c9d5b;--goodb:#e7f5ee;--miss:#d3455b;--missb:#fbe9ec;--warn:#c67c1a;--warnb:#fdf1e0}
  *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sans);-webkit-font-smoothing:antialiased;line-height:1.45}
  .num{font-family:var(--mono);font-variant-numeric:tabular-nums}
  .wrap{max-width:1120px;margin:0 auto;padding:30px 22px 64px}
  header{display:flex;flex-wrap:wrap;align-items:flex-end;justify-content:space-between;gap:16px;border-bottom:1px solid var(--line);padding-bottom:18px}
  h1{font-size:20px;margin:0;font-weight:650;letter-spacing:-.01em}
  .sub{color:var(--soft);font-size:13px;margin-top:3px}
  .status{font-size:12px;color:var(--faint);text-align:right}
  .dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--good);margin-right:5px;vertical-align:middle}
  .dot.stale{background:var(--faint)}
  .kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:22px 0}
  @media(max-width:720px){.kpis{grid-template-columns:repeat(2,1fr)}}
  .kpi{background:var(--surface);border:1px solid var(--line);border-radius:13px;padding:16px 17px;box-shadow:var(--shadow)}
  .kpi .v{font-size:27px;font-weight:650;letter-spacing:-.02em;line-height:1}
  .kpi .l{font-size:12px;color:var(--soft);margin-top:5px}
  .proj{background:var(--surface);border:1px solid var(--line);border-radius:13px;padding:16px 18px;box-shadow:var(--shadow);margin-bottom:14px}
  .proj h3{margin:0;font-size:15px;font-weight:650;display:flex;align-items:center;gap:10px;flex-wrap:wrap}
  .proj .meta{color:var(--soft);font-size:12px;margin-top:2px}
  .proj .meta a{color:var(--accent);text-decoration:none}
  .ff{font-size:11px;font-weight:650;padding:1px 8px;border-radius:999px;background:var(--warnb);color:var(--warn)}
  .cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px;margin-top:12px}
  .sess{border:1px solid var(--line);border-radius:10px;padding:11px 12px;background:var(--surface2)}
  .sess .cl{font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .sess .det{font-family:var(--mono);color:var(--faint);font-size:11px;margin-top:3px}
  .pill{font-size:11px;font-weight:650;padding:2px 9px;border-radius:999px;white-space:nowrap;display:inline-block;margin-top:8px}
  .pill.ok{background:var(--goodb);color:var(--good)}
  .pill.one{background:var(--warnb);color:var(--warn)}
  .pill.uns{background:var(--missb);color:var(--miss)}
  .pill.err{background:var(--missb);color:var(--miss)}
  .empty{color:var(--faint);font-size:13px;padding:20px 0}
  .err{background:var(--missb);color:var(--miss);padding:16px;border-radius:12px;margin-top:24px}
</style></head><body>
<div class="wrap">
  <header>
    <div><h1>NXT Scanner — DIT-Board</h1><div class="sub">Live-Sicherungsstatus je Karte · aktualisiert automatisch</div></div>
    <div class="status" id="status">lade…</div>
  </header>
  <div id="content"></div>
</div>
<script>
  var KEY=new URLSearchParams(location.search).get("key")||"";
  function esc(s){var d=document.createElement("div");d.textContent=s==null?"":String(s);return d.innerHTML;}
  function gb(b){return b?(b/1e9).toFixed(1)+" GB":"—";}
  var LABEL={unsecured:["uns","ungesichert"],"1x":["one","1x — Achtung"],"2x_verified":["ok","2x gesichert · formatieren ok"],error:["err","Fehler"]};
  function pill(st){var m=LABEL[st]||["uns",st];return '<span class="pill '+m[0]+'">'+m[1]+'</span>';}
  function render(d){
    var c=document.getElementById("content");
    var k=d.counts||{};
    var kpis='<div class="kpis">'+
      '<div class="kpi"><div class="v num">'+(d.projects||[]).length+'</div><div class="l">aktive Projekte</div></div>'+
      '<div class="kpi"><div class="v num" style="color:var(--good)">'+(k["2x_verified"]||0)+'</div><div class="l">Karten 2x gesichert</div></div>'+
      '<div class="kpi"><div class="v num" style="color:var(--warn)">'+(k["1x"]||0)+'</div><div class="l">nur 1 Kopie</div></div>'+
      '<div class="kpi"><div class="v num" style="color:var(--miss)">'+((k.unsecured||0)+(k.error||0))+'</div><div class="l">ungesichert / Fehler</div></div>'+
    '</div>';
    var projs=d.projects||[];
    var body=projs.length? projs.map(function(p){
      var ff=p.ff_notion_url?'<a class="ff" href="'+esc(p.ff_notion_url)+'" target="_blank">FF</a>':'';
      var meta=[p.date,p.client].filter(Boolean).map(esc).join(" · ");
      var sess=(p.sessions||[]).length? '<div class="cards">'+p.sessions.map(function(s){
        return '<div class="sess"><div class="cl">'+esc(s.card_label||"Karte")+'</div>'+
          '<div class="det">'+(s.source_files||0)+' Dateien · '+gb(s.source_bytes)+'</div>'+
          pill(s.status)+'</div>';
      }).join("")+'</div>' : '<div class="empty">noch keine Karte eingelesen</div>';
      return '<div class="proj"><h3>'+esc(p.name)+' '+ff+'</h3><div class="meta">'+meta+'</div>'+sess+'</div>';
    }).join("") : '<div class="empty">Keine aktiven Projekte. Lege im NXT Scanner ein Projekt an.</div>';
    c.innerHTML=kpis+body;
  }
  function setStatus(t,stale){document.getElementById("status").innerHTML='<span class="dot'+(stale?" stale":"")+'"></span>'+t;}
  function load(){
    fetch("/board.json?key="+encodeURIComponent(KEY)).then(function(r){
      if(r.status===401)throw new Error("Kein/falscher Zugriffs-Key in der URL (…/board?key=…).");
      if(!r.ok)throw new Error("Server "+r.status);return r.json();
    }).then(function(d){render(d);var t=new Date(d.generated_at);setStatus("aktualisiert "+t.toLocaleTimeString("de-DE")+" · auto alle 8s",false);})
    .catch(function(e){document.getElementById("content").innerHTML='<div class="err">'+esc(e.message)+'</div>';setStatus("Fehler",true);});
  }
  load(); setInterval(load,8000);
</script></body></html>`;
