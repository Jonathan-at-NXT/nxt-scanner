/**
 * Upload-Briefkasten: Links zum Hochladen ohne Konto → R2 (EU) → Abholer (NAS) → erst dann gelöscht.
 *
 * Öffentlich (Token im Link):
 *   GET  /u/<token>                 Upload-Seite
 *   GET  /u/<token>/info            Name, Ablauf, Limit, bisher hochgeladen
 *   POST /u/<token>/start           {rel_path,size,last_modified} → Datei anlegen oder fortsetzen (bekannte Teile)
 *   PUT  /u/<token>/part?id=&n=     Teil (PART_SIZE) mit Header x-part-sha256
 *   POST /u/<token>/complete        {id} → R2-Objekt fertigstellen, Größe prüfen
 * Abholer (Bearer INGEST_TOKEN):
 *   GET  /briefkasten/pending       fertige, noch nicht abgeholte Dateien inkl. Teil-Prüfsummen
 *   GET  /briefkasten/file?id=      Datei-Inhalt
 *   POST /briefkasten/done          {id, nas_path, part_sha256:[…]} → vergleicht, löscht R2-Objekt, Status "abgeholt"
 */

import FONT from "../static/outfit-latin.woff2";

export const PART_SIZE = 32 * 1024 * 1024;     // gleich groß für alle Teile außer dem letzten (R2-Vorgabe)
const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } });
const now = () => new Date().toISOString();

function cleanRel(p) {
  const parts = String(p || "").replace(/\\/g, "/").split("/")
    .map((s) => s.replace(/[\u0000-\u001f]/g, "").trim()).filter((s) => s && s !== "." && s !== "..");
  const rel = parts.join("/").slice(0, 900);
  return rel || null;
}

async function linkFor(env, token) {
  if (!/^[A-Za-z0-9_-]{16,64}$/.test(token || "")) return null;
  const l = await env.DB.prepare("SELECT * FROM upload_link WHERE token=?").bind(token).first();
  if (!l || !l.active || (l.expires_at && Date.parse(l.expires_at) < Date.now())) return null;
  return l;
}

export async function handleBriefkasten(request, env, url, authorized) {
  const p = url.pathname;
  // ── Abholer ────────────────────────────────────────────────
  if (p.startsWith("/briefkasten/")) {
    if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);
    if (p === "/briefkasten/pending") {
      const files = (await env.DB.prepare(
        `SELECT f.id, f.rel_path, f.size, f.part_size, f.r2_key, f.uploaded_at, l.name link_name, l.token
         FROM upload_file f JOIN upload_link l ON l.token=f.token WHERE f.state='hochgeladen' ORDER BY f.id LIMIT 200`).all()).results || [];
      for (const f of files) {
        f.parts = ((await env.DB.prepare("SELECT n, sha256, size FROM upload_part WHERE file_id=? ORDER BY n").bind(f.id).all()).results || []);
      }
      return json({ files });
    }
    if (p === "/briefkasten/progress" && request.method === "POST") {   // Balken "Briefkasten → NAS" im Dashboard
      const b = await request.json().catch(() => ({}));
      const n = (v) => (Number.isFinite(Number(v)) ? Math.round(Number(v)) : null);
      const st = ["running", "done", "failed"].includes(b.state) ? b.state : "running";
      const t = new Date().toISOString();
      const name = typeof b.name === "string" && b.name ? b.name.slice(0, 80) : "Briefkasten → NAS";
      const kind = b.name ? "pruefung" : "kopie";
      await env.DB.prepare(`INSERT INTO job_progress(job_id, host, kind, disk_uuid, disk_name, started_at, updated_at, done_files, done_bytes, total_files, total_bytes, state, message)
        VALUES('NAS|' || ? || '|' || ?,'NAS',?,'briefkasten',?,?,?,?,?,NULL,?,?,NULL)
        ON CONFLICT(job_id) DO UPDATE SET updated_at=excluded.updated_at, done_files=excluded.done_files, done_bytes=excluded.done_bytes,
          total_bytes=excluded.total_bytes, state=excluded.state,
          started_at=CASE WHEN job_progress.state!='running' THEN excluded.started_at ELSE job_progress.started_at END`)
        .bind(name, kind, kind, name, t, t, n(b.done_files), n(b.done_bytes), n(b.total_bytes), st).run();
      return json({ ok: true });
    }
    if (p === "/briefkasten/file") {
      const f = await env.DB.prepare("SELECT * FROM upload_file WHERE id=? AND state='hochgeladen'").bind(Number(url.searchParams.get("id"))).first();
      if (!f) return json({ error: "nicht gefunden" }, 404);
      const obj = await env.BRIEFKASTEN.get(f.r2_key);
      if (!obj) return json({ error: "Objekt fehlt" }, 404);
      return new Response(obj.body, { headers: { "content-length": String(obj.size), "content-type": "application/octet-stream" } });
    }
    if (p === "/briefkasten/done" && request.method === "POST") {
      const b = await request.json();
      const f = await env.DB.prepare("SELECT * FROM upload_file WHERE id=? AND state='hochgeladen'").bind(Number(b.id)).first();
      if (!f) return json({ error: "nicht gefunden" }, 404);
      const parts = (await env.DB.prepare("SELECT n, sha256 FROM upload_part WHERE file_id=? ORDER BY n").bind(f.id).all()).results || [];
      const got = Array.isArray(b.part_sha256) ? b.part_sha256 : [];
      const ok = parts.length === got.length && parts.every((x, i) => x.sha256 === got[i]);
      if (!ok) {
        await env.DB.prepare("UPDATE upload_file SET error=? WHERE id=?").bind("Prüfsumme auf dem NAS weicht ab – nicht gelöscht", f.id).run();
        return json({ error: "Prüfsumme weicht ab" }, 409);
      }
      await env.BRIEFKASTEN.delete(f.r2_key);   // erst jetzt: Datei liegt geprüft auf dem NAS
      await env.DB.prepare("UPDATE upload_file SET state='abgeholt', pulled_at=?, nas_path=?, error=NULL WHERE id=?")
        .bind(now(), String(b.nas_path || "").slice(0, 1000), f.id).run();
      return json({ ok: true });
    }
    return json({ error: "nicht gefunden" }, 404);
  }

  // ── Öffentlicher Upload ────────────────────────────────────
  if (p === "/u/outfit.woff2") return new Response(FONT, { headers: { "content-type": "font/woff2", "cache-control": "public, max-age=604800" } });
  const m = p.match(/^\/u\/([^/]+)(\/[a-z]+)?$/);
  if (!m) return null;
  const token = m[1], action = m[2] || "";
  const link = await linkFor(env, token);
  if (!link) return action ? json({ error: "Link invalid or expired" }, 404)
    : new Response("This upload link is invalid or has expired.", { status: 404, headers: { "content-type": "text/plain; charset=utf-8" } });
  if (!action && request.method === "GET") return new Response(PAGE, { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store",
    "content-security-policy": "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; font-src 'self'; img-src data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
    "referrer-policy": "no-referrer", "x-content-type-options": "nosniff" } });
  const used = async () => (await env.DB.prepare("SELECT COALESCE(SUM(size),0) b, COUNT(*) n FROM upload_file WHERE token=? AND state!='abgebrochen'").bind(token).first());
  if (action === "/info") {
    const u = await used();
    return json({ name: link.name, expires_at: link.expires_at, max_bytes: link.max_bytes, used_bytes: u.b, files: u.n, part_size: PART_SIZE, note: link.note });
  }
  if (action === "/start" && request.method === "POST") {
    const b = await request.json();
    const rel = cleanRel(b.rel_path), size = Math.floor(Number(b.size));
    if (!rel || !(size >= 0)) return json({ error: "Invalid file" }, 400);
    const ex = await env.DB.prepare("SELECT * FROM upload_file WHERE token=? AND rel_path=? AND size=? AND state IN ('laeuft','hochgeladen','abgeholt') ORDER BY id DESC LIMIT 1")
      .bind(token, rel, size).first();
    if (ex && ex.state !== "laeuft") return json({ id: ex.id, done: true });
    if (ex) {
      const parts = (await env.DB.prepare("SELECT n FROM upload_part WHERE file_id=?").bind(ex.id).all()).results || [];
      return json({ id: ex.id, part_size: ex.part_size, have: parts.map((x) => x.n) });
    }
    const u = await used();
    if (link.max_bytes && u.b + size > link.max_bytes) return json({ error: "Link limit reached" }, 413);
    const key = `${token}/${crypto.randomUUID()}/${rel}`;
    const mpu = await env.BRIEFKASTEN.createMultipartUpload(key);
    const r = await env.DB.prepare(`INSERT INTO upload_file(token, rel_path, size, last_modified, r2_key, upload_id, part_size, created_at)
      VALUES(?,?,?,?,?,?,?,?)`).bind(token, rel, size, Number(b.last_modified) || null, key, mpu.uploadId, PART_SIZE, now()).run();
    return json({ id: r.meta.last_row_id, part_size: PART_SIZE, have: [] });
  }
  if (action === "/part" && request.method === "PUT") {
    const id = Number(url.searchParams.get("id")), n = Number(url.searchParams.get("n"));
    const sha = (request.headers.get("x-part-sha256") || "").toLowerCase();
    const f = await env.DB.prepare("SELECT * FROM upload_file WHERE id=? AND token=? AND state='laeuft'").bind(id, token).first();
    if (!f || !(n >= 1) || !/^[0-9a-f]{64}$/.test(sha)) return json({ error: "Invalid part" }, 400);
    const buf = await request.arrayBuffer();
    const expected = Math.min(f.part_size, f.size - (n - 1) * f.part_size);
    if (buf.byteLength !== Math.max(0, expected)) return json({ error: "Wrong part size" }, 400);
    const digest = [...new Uint8Array(await crypto.subtle.digest("SHA-256", buf))].map((x) => x.toString(16).padStart(2, "0")).join("");
    if (digest !== sha) return json({ error: "Part arrived damaged, please retry" }, 422);
    const part = await env.BRIEFKASTEN.resumeMultipartUpload(f.r2_key, f.upload_id).uploadPart(n, buf);
    await env.DB.prepare("INSERT INTO upload_part(file_id, n, etag, sha256, size) VALUES(?,?,?,?,?) ON CONFLICT(file_id, n) DO UPDATE SET etag=excluded.etag, sha256=excluded.sha256, size=excluded.size")
      .bind(id, n, part.etag, sha, buf.byteLength).run();
    return json({ ok: true });
  }
  if (action === "/complete" && request.method === "POST") {
    const b = await request.json();
    const f = await env.DB.prepare("SELECT * FROM upload_file WHERE id=? AND token=? AND state='laeuft'").bind(Number(b.id), token).first();
    if (!f) return json({ error: "not found" }, 404);
    const parts = (await env.DB.prepare("SELECT n, etag, size FROM upload_part WHERE file_id=? ORDER BY n").bind(f.id).all()).results || [];
    const need = Math.max(1, Math.ceil(f.size / f.part_size));
    if (parts.length !== need || parts.reduce((s, x) => s + x.size, 0) !== f.size) return json({ error: "Some parts are still missing" }, 409);
    const obj = await env.BRIEFKASTEN.resumeMultipartUpload(f.r2_key, f.upload_id).complete(parts.map((x) => ({ partNumber: x.n, etag: x.etag })));
    if (obj.size !== f.size) return json({ error: "Size mismatch" }, 500);
    await env.DB.prepare("UPDATE upload_file SET state='hochgeladen', uploaded_at=? WHERE id=?").bind(now(), f.id).run();
    return json({ ok: true });
  }
  return json({ error: "not found" }, 404);
}

const PAGE = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow"><title>NXT Upload</title>
<link rel="icon" type="image/png" sizes="64x64" href="data:image/png;base64,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">
<link rel="apple-touch-icon" href="data:image/png;base64,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">
<style>
:root{--bg:#0e0e10;--panel:#17171b;--line:#2a2a31;--text:#f2f2f4;--muted:#9a9aa3;--accent:#ff5a1f;--ok:#3ecf8e;--crit:#ff4d4f}
@media (prefers-color-scheme: light){:root{--bg:#f6f6f7;--panel:#fff;--line:#e3e3e7;--text:#141417;--muted:#6b6b74}}
@font-face{font-family:"Outfit";src:url("/u/outfit.woff2") format("woff2");font-weight:300 700;font-display:swap}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{max-width:760px;margin:0 auto;padding:32px 16px}h1{font-family:"Outfit",sans-serif;font-weight:600;letter-spacing:-.02em;font-size:24px;margin:0 0 4px}
.brand{display:flex;align-items:center;gap:10px;margin-bottom:28px;font-family:"Outfit",sans-serif}.brand b{font-weight:700;font-size:30px;letter-spacing:-.01em}.brand img{height:30px;width:30px;filter:invert(1)}@media (prefers-color-scheme: light){.brand img{filter:none}}.brand span{font-size:13px;text-transform:uppercase;letter-spacing:.14em;color:var(--muted)}.muted{color:var(--muted)}
.drop{margin:22px 0;border:2px dashed var(--line);border-radius:14px;padding:36px 16px;text-align:center;background:var(--panel)}
.drop.on{border-color:var(--accent)}button,label.btn{display:inline-block;background:var(--accent);color:#fff;border:0;border-radius:8px;padding:9px 16px;font:inherit;cursor:pointer;margin:4px}
label.btn.ghost{background:transparent;border:1px solid var(--line);color:var(--text)}
.bar{height:8px;background:var(--line);border-radius:6px;overflow:hidden}.bar span{display:block;height:100%;background:var(--accent);width:0}
.file{display:grid;grid-template-columns:1fr auto;gap:4px 12px;padding:10px 0;border-bottom:1px solid var(--line);font-size:14px}
.file .bar{grid-column:1/-1}.ok{color:var(--ok)}.err{color:var(--crit)}.name{word-break:break-all}
</style></head><body><main>
<div class="brand"><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAQAAAAEACAYAAABccqhmAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAlZSURBVHgB7d3RkePGEcbx71T37svAk4FPGawzUAakI5AdAcYRSI6AcASnDHYzsBwB4Qh0GdCcJbe0ko57BAnMdE//f1VfSQ/3gJ1BD0GgOZAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALDinWDJh2O2x/xFbfzvmPGYSXU9nPNnYW2t5hhf8fGYX445GMgPqiMd8yjpQKpnEMywVPwvyVrfTtKBNMtWaG4je8Vfste6kqQDaZrH90JLG52+j1mUtK6PQmsfvxFasVz8NXwQWvvAAtDG94pd/DCCBaC+4ZgfBRjAAlBXKf4swAgWgHoofpjDAlAHxQ+TWADWR/HDLPoA1rUT3VYwjCuA9VD8MI8FYB0UP1zgK8CySnfbJ51+2gqYxwKwnFL8j6LHHY7wFWAZFD9cYgG4XzrmP6L44RALwH2STp/8SYBDLAC3S6L44RwLwG2SKH50gAVgviSKH51gAZgnieJHR1gArlfu8pe7/UlAJ1gArlOKv3zys48dusIC8HUUP7rFAvC2snNvueyn+NElFoDLom/bjQBYAL6M4kcILAB/VLbwGgUEwALwW+zfh1BYAH5F8SMcFoATir++z0Jrn1kAKP5WnsQi0NpPCq4U/4FczNqypANpkl8UvK19J18T1iI15GP2kg6kWvY672D1TjHtxLbd16h5fiTZ9qDTeePddMxfz/8Np7T0lkm0vDpbCk428jVvl7JX4Mv+Uvylr9/LZFkI+rlPFPo3LRT/bYlukK/5upRRFL+rCbOSyAb5mqtL+VGBJXGH+Z5EtZOvebqUrMCSKP57E83LG568zM9byQosieJfIpH09FVxq8CSKP6lEkVSH+dM6e77ToElUfxLJoKkfoo/9Pspyx+/l69Js57e9XLO7BW8r79MZFkBvUyYl/Ssl3NmL4qf4l8pvdqoj3Mm/ItqHkTxr5kefS9fc3Apjwq+Xf1GvibMY3ozyNf4X8qo4DbyNWFe05NBvsb+UrKC6+USzkN68YN8jfulZAU3yNeEeY93Pe3/8HcFN8jXhPUQz3pp7S03ubcKbpCvSeslXiX1U/yhu/uKQb4mrad4lNRPdx/FL1+T1lu8SaK1txu93Lm9JlYvVz3ppSM0fHdfsZOvSbsnW9l9QYYXD+qn+EN39xU7+Zq0e7I9/83Z6PF5sJHNsZubUcGLv/zxn+Rr0m7N7x/tZKPHad0gm+M2N6E37iwi7dz7pUc72eixWjbI5pjNTVZw0Yu/yMaO8yVWDbI5XnOTFVwSxV9kQ8f5OhbtZHOs5mar4JLibOG119uPdrKBY/xSLOnlHlH5IHhQcEkU/2u58TFeihW9fE3cy1h333vVl3TazSSpf5MCv4p5IUl9nC+TDJ4L36iuJIof10ui+FdVcwFIovhxvXKp3MP58vMx3yr4uRBpz/695p+0ueLxzUkrvfT1l5uW4Vt7I23bfesPOXKFY7slLWzUx/kyCuGK/9bVPq98bLemtl72e8zC80sKKf7rZK17fLempkE2x2BushBq2+5H3f89L2vdY7w1tQySDh0k/MadxUa+Ju2ejFpGllY/1ltSQw8bv7Bx51mkPftHLSdLVY55bta2k3RwHjbuPBvka+LuyahlZanKcc/NWnpq7U1CqOLPWl6WdDCYNSRR/F0Z5Gvi7knWOrKkg8EsLamPhjA27jwb5Gvi7knWerKkg8EsKamP4n8U3X3PBvmauHuSta4s6WAwS+mlFXwUnkXasz9rfVnSwWCW8KA+GsLCb9z5YidfE3dPsurIsvn332ujdY+vt/PAvJ18Tdw9qdnVlWVzDO7RS08I3X2KtWd/yVZ1Zdkch1sNFY6tx/PApEjbdrea9CybY3GLodKxrRk27jyj+OsYZXM85tpVPLa1shetvc+S2LO/llE2x+VavXxF3IsGn2dJcbbwsvBjjlE2x+Ya9PV3Jonir22UzfH5mqR+Wnvp7lO8F3ZY+a43yuYYvSWpn+4+il+8raelUTbH6ZKkPs4VuvvOkij+lkbZHKsv6WWj1yw8S6L4Wxtlc7x+byOKvzs7+Zq8W7OX3bu8o2yO2Wsbo8c4N1vhNyJs3b2X7Uc8o2yO24vB6PHNSTnPvxP+wNMk3pK97D/fHWVz7IrB6LHNCRt3vsHTRM6Nl+e7o2yOXw/7PuxFg8+bvEzk3Hhq7hjla2y9ZC+K/001Xw9eU3klc3k992chqpdzYBIu6nEB+Lco/uieRPFf5b36Uop/K0TGOTBDT1cATDz+Kc6BWXpZAP4lJj66UvxZmM3LHd1LyfJvlK8xtxY27ryDp4nusfiLUb7G3VK2ws083wTkki+28pSn3On/WbiZ13sAFH9skyj+RXi8AqD4Y5vEM/7FeLsCoPhjm0TxL8rTFcDfxNtZI6O9ewVergAo/tho714Rj3naG2V/HlqFjTtXZnXiI72fbZTdeWiZLKzK6j0AnvGCG74VWFwAKH5wz6cSazcBJ1H8kb0s/qNQhaUrgEk8442MK78GrFwBTKL4I5uO+VYUf3UWFoBJFH9kk5j/ZlovAJOY/MjYuLOxlgvAJCY/sicx/821WgAmMfmR0dprRIsFoFz2lRs+kxARG3caU7O109PbemoaZaf9ltbeQGr2AfBzztj+IX7YY1KNlf+T+OR/yyhbn9RLpvyoayuYVOMKgBd2xEV3n3Fr3wSk+OOaRHefeWsuABR/XJN4zOvCWgsAj3riorvPmaVv+mRhrlE2b+DNDY95HaL42xtls6DnZBRcovjbG2WzqK8Nz/cdo/jbG2W3uJn7znECtDfKboEz95275wTgvezLGGW3yC9lK3SBE6C9LNvF/jqR3tfQvVv7ANi2OaZJp2f8T0IX5i4Apbeb4o9pEn393ZnzYyB+2BHXJLr7unTtFQDFHxc7OHWsLADTV/4Nxb8+q5uk/CQ2celeeZR36Y7v/pgkrK30z5e765bu9o9CGKWVk+Jv66NOY26h+LMQwrtX/5/067P9SafLPy796tvqtBj8SfX9V6evek8CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACo6v9OcVqaNVL+/QAAAABJRU5ErkJggg==" alt="NXT"><b>NXT</b><span>Studios · Upload</span></div><h1 id="title">…</h1><div class="muted" id="sub"></div>
<div class="drop" id="drop"><div style="font-size:17px;margin-bottom:10px">Drag files or folders here</div>
<label class="btn">Choose files<input type="file" id="pick" multiple hidden></label>
<label class="btn ghost">Choose folder<input type="file" id="pickdir" webkitdirectory multiple hidden></label>
<div class="muted" style="margin-top:10px;font-size:13px">Large files are no problem. If the connection drops, just choose the same files again and the upload resumes where it stopped.</div></div>
<div id="total" hidden><div class="muted" id="totaltext"></div><div class="bar" style="margin:6px 0 14px"><span id="totalbar"></span></div></div>
<div id="list"></div>
</main><script>
const token = location.pathname.split("/")[2];
const $ = (s) => document.querySelector(s);
const fmt = (b) => b >= 1e12 ? (b/1e12).toFixed(2)+" TB" : b >= 1e9 ? (b/1e9).toFixed(1)+" GB" : b >= 1e6 ? (b/1e6).toFixed(0)+" MB" : Math.max(1,Math.round(b/1e3))+" KB";
let PART = 33554432, queue = [], totalBytes = 0, doneBytes = 0, running = 0, sent = [];
const dur = (s) => s >= 3600 ? Math.floor(s/3600)+" h "+Math.round(s%3600/60)+" min" : s >= 60 ? Math.round(s/60)+" min" : Math.max(1,Math.round(s))+" s";
function speed(){ const now=Date.now(); sent=sent.filter(x=>now-x[0]<20000); if(sent.length<2) return 0; const t=(now-sent[0][0])/1000; return t>=2 ? sent.slice(1).reduce((a,x)=>a+x[1],0)/t : 0; }
fetch("/u/"+token+"/info").then(r=>r.json()).then(i=>{ if(i.error){$("#title").textContent=i.error;$("#drop").hidden=true;return;}
  PART=i.part_size; $("#title").textContent=i.name;
  $("#sub").textContent=(i.note?i.note+" · ":"")+(i.expires_at?"valid until "+new Date(i.expires_at).toLocaleDateString("en-GB"):"")+(i.max_bytes?" · max "+fmt(i.max_bytes):"")+(i.files?" · so far "+i.files+" files, "+fmt(i.used_bytes):""); });
async function sha256(buf){ const d=await crypto.subtle.digest("SHA-256",buf); return [...new Uint8Array(d)].map(x=>x.toString(16).padStart(2,"0")).join(""); }
function row(f){ const el=document.createElement("div"); el.className="file";
  el.innerHTML='<div class="name"></div><div class="muted st">waiting</div><div class="bar"><span></span></div>';
  el.querySelector(".name").textContent=f.rel+" · "+fmt(f.file.size); $("#list").appendChild(el); return el; }
function upd(){ $("#total").hidden=!totalBytes; const v=speed(), left=totalBytes-doneBytes; $("#totaltext").textContent=fmt(doneBytes)+" of "+fmt(totalBytes)+" uploaded"+(v&&left>0?" · "+(v*8/1e6>=100?(v*8/1e9).toFixed(2)+" Gbit/s":(v*8/1e6).toFixed(0)+" Mbit/s")+" ("+fmt(v)+"/s) · about "+dur(left/v)+" left":""); $("#totalbar").style.width=(totalBytes?doneBytes/totalBytes*100:0)+"%"; }
async function api(path, opt){ for(let a=0;a<6;a++){ try{ const r=await fetch("/u/"+token+path,opt); const j=await r.json().catch(()=>({})); if(r.ok) return j;
  if(r.status<500 && r.status!==422) throw new Error(j.error||("Error "+r.status)); }catch(e){ if(a===5||String(e.message).startsWith("Link")) throw e; }
  await new Promise(r=>setTimeout(r,2000*(a+1))); } }
async function upload(f){ const el=f.el, st=el.querySelector(".st"), bar=el.querySelector(".bar span");
  try{ st.textContent="starting…";
    const s=await api("/start",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({rel_path:f.rel,size:f.file.size,last_modified:f.file.lastModified})});
    if(s.done){ st.textContent="already there ✓"; st.className="ok st"; bar.style.width="100%"; doneBytes+=f.file.size; upd(); return; }
    const ps=s.part_size, n=Math.max(1,Math.ceil(f.file.size/ps)), have=new Set(s.have||[]); let mine=0;
    for(const k of have){ const b=Math.min(ps,f.file.size-(k-1)*ps); mine+=b; doneBytes+=b; }
    const todo=[]; for(let k=1;k<=n;k++) if(!have.has(k)) todo.push(k);
    const worker=async()=>{ while(todo.length){ const k=todo.shift(); const blob=f.file.slice((k-1)*ps,Math.min(f.file.size,k*ps)); const buf=await blob.arrayBuffer();
        const h=await sha256(buf); await api("/part?id="+s.id+"&n="+k,{method:"PUT",headers:{"x-part-sha256":h},body:buf});
        mine+=buf.byteLength; doneBytes+=buf.byteLength; if(!sent.length) sent.push([Date.now()-1,0]); sent.push([Date.now(),buf.byteLength]); bar.style.width=(mine/Math.max(1,f.file.size)*100)+"%"; st.textContent=Math.round(mine/Math.max(1,f.file.size)*100)+" %"; upd(); } };
    await Promise.all([worker(),worker(),worker()]);
    await api("/complete",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({id:s.id})});
    st.textContent="done ✓"; st.className="ok st"; bar.style.width="100%";
  }catch(e){ st.textContent="Error: "+e.message+" – choose the file again to resume"; st.className="err st"; }
}
async function pump(){ while(queue.length && running<2){ const f=queue.shift(); running++; upload(f).finally(()=>{running--; pump();}); } }
function add(files){ for(const {file,rel} of files){ if(file.name===".DS_Store"||file.name.startsWith("._")) continue; const f={file,rel}; f.el=row(f); totalBytes+=file.size; queue.push(f); } upd(); pump(); }
$("#pick").onchange=(e)=>add([...e.target.files].map(f=>({file:f,rel:f.name})));
$("#pickdir").onchange=(e)=>add([...e.target.files].map(f=>({file:f,rel:f.webkitRelativePath||f.name})));
const drop=$("#drop"); drop.ondragover=(e)=>{e.preventDefault();drop.classList.add("on")}; drop.ondragleave=()=>drop.classList.remove("on");
async function walk(entry,path,out){ if(entry.isFile){ await new Promise(r=>entry.file(f=>{out.push({file:f,rel:path+f.name});r();},r)); }
  else if(entry.isDirectory){ const rd=entry.createReader(); let batch; do{ batch=await new Promise(r=>rd.readEntries(r,()=>r([]))); for(const e of batch) await walk(e,path+entry.name+"/",out); }while(batch.length); } }
drop.ondrop=async(e)=>{ e.preventDefault(); drop.classList.remove("on"); const out=[]; const items=[...e.dataTransfer.items].map(i=>i.webkitGetAsEntry&&i.webkitGetAsEntry()).filter(Boolean);
  if(items.length){ for(const it of items) await walk(it,"",out); } else { for(const f of e.dataTransfer.files) out.push({file:f,rel:f.name}); } add(out); };
setInterval(upd,2000);
window.onbeforeunload=()=> (running||queue.length) ? "Upload still running" : undefined;
</script></body></html>`;
