/**
 * NXT Scanner Ingest-Worker.
 *
 * POST /ingest  — Bearer-Auth (Secret INGEST_TOKEN), Body {disk, folder_tree}.
 *   Schreibt disks + folder_tree idempotent (delete+insert je disk_uuid) nach D1.
 * GET  /health  — Liveness.
 *
 * Secrets kommen ausschliesslich aus env (wrangler secret put INGEST_TOKEN) —
 * niemals im Repo.
 */

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
  const [notion, d1res] = await Promise.all([
    fetchNotionDisks(env),
    env.DB.prepare("SELECT uuid, name, last_user, last_scan FROM disks").all(),
  ]);
  const d1 = d1res.results || [];
  const d1ByUuid = new Set(d1.map((d) => (d.uuid || "").toUpperCase()));
  const d1ByName = new Set(d1.map((d) => normName(d.name)));

  const disks = notion.map((n) => {
    const done =
      (n.uuid && d1ByUuid.has(n.uuid.toUpperCase())) || d1ByName.has(normName(n.name));
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

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return json({ ok: true });
    }

    // ── Rollout-Ansicht (read-only, mit VIEW_TOKEN) ──
    if (url.pathname === "/rollout.json") {
      if (url.searchParams.get("key") !== env.VIEW_TOKEN) {
        return json({ error: "unauthorized" }, 401);
      }
      try {
        return json(await buildRollout(env));
      } catch (e) {
        return json({ error: "rollout", detail: String(e) }, 500);
      }
    }
    if (url.pathname === "/rollout") {
      return new Response(ROLLOUT_HTML, {
        headers: { "content-type": "text/html; charset=utf-8" },
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
          `INSERT INTO disks(uuid,name,fs_type,is_network,used_bytes,capacity_bytes,last_scan,last_user)
           VALUES(?,?,?,?,?,?,?,?)
           ON CONFLICT(uuid) DO UPDATE SET
             name=excluded.name, fs_type=excluded.fs_type, is_network=excluded.is_network,
             used_bytes=excluded.used_bytes, capacity_bytes=excluded.capacity_bytes,
             last_scan=excluded.last_scan, last_user=excluded.last_user`
        ).bind(
          uuid, disk.name ?? null, disk.fs_type ?? null, disk.is_network ?? null,
          disk.used_bytes ?? null, disk.capacity_bytes ?? null,
          disk.last_scan ?? null, disk.last_user ?? null
        )
      );

      // delete+insert folder_tree je disk
      stmts.push(env.DB.prepare(`DELETE FROM folder_tree WHERE disk_uuid=?`).bind(uuid));
      for (const n of tree) {
        stmts.push(
          env.DB.prepare(
            `INSERT INTO folder_tree(disk_uuid,rel_path,depth,parent_rel_path,size_bytes,file_count,mtime)
             VALUES(?,?,?,?,?,?,?)`
          ).bind(
            uuid, n.rel_path, n.depth ?? null, n.parent_rel_path ?? null,
            n.size_bytes ?? null, n.file_count ?? null, n.mtime ?? null
          )
        );
      }

      try {
        await env.DB.batch(stmts);
      } catch (e) {
        return json({ error: "db", detail: String(e) }, 500);
      }

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
