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
  const m = p.match(/^\/u\/([^/]+)(\/[a-z]+)?$/);
  if (!m) return null;
  const token = m[1], action = m[2] || "";
  const link = await linkFor(env, token);
  if (!link) return action ? json({ error: "Link ungültig oder abgelaufen" }, 404)
    : new Response("Dieser Upload-Link ist ungültig oder abgelaufen.", { status: 404, headers: { "content-type": "text/plain; charset=utf-8" } });
  if (!action && request.method === "GET") return new Response(PAGE, { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store",
    "content-security-policy": "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
    "referrer-policy": "no-referrer", "x-content-type-options": "nosniff" } });
  const used = async () => (await env.DB.prepare("SELECT COALESCE(SUM(size),0) b, COUNT(*) n FROM upload_file WHERE token=? AND state!='abgebrochen'").bind(token).first());
  if (action === "/info") {
    const u = await used();
    return json({ name: link.name, expires_at: link.expires_at, max_bytes: link.max_bytes, used_bytes: u.b, files: u.n, part_size: PART_SIZE, note: link.note });
  }
  if (action === "/start" && request.method === "POST") {
    const b = await request.json();
    const rel = cleanRel(b.rel_path), size = Math.floor(Number(b.size));
    if (!rel || !(size >= 0)) return json({ error: "Ungültige Datei" }, 400);
    const ex = await env.DB.prepare("SELECT * FROM upload_file WHERE token=? AND rel_path=? AND size=? AND state IN ('laeuft','hochgeladen','abgeholt') ORDER BY id DESC LIMIT 1")
      .bind(token, rel, size).first();
    if (ex && ex.state !== "laeuft") return json({ id: ex.id, done: true });
    if (ex) {
      const parts = (await env.DB.prepare("SELECT n FROM upload_part WHERE file_id=?").bind(ex.id).all()).results || [];
      return json({ id: ex.id, part_size: ex.part_size, have: parts.map((x) => x.n) });
    }
    const u = await used();
    if (link.max_bytes && u.b + size > link.max_bytes) return json({ error: "Limit dieses Links erreicht" }, 413);
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
    if (!f || !(n >= 1) || !/^[0-9a-f]{64}$/.test(sha)) return json({ error: "Ungültiger Teil" }, 400);
    const buf = await request.arrayBuffer();
    const expected = Math.min(f.part_size, f.size - (n - 1) * f.part_size);
    if (buf.byteLength !== Math.max(0, expected)) return json({ error: "Teilgröße falsch" }, 400);
    const digest = [...new Uint8Array(await crypto.subtle.digest("SHA-256", buf))].map((x) => x.toString(16).padStart(2, "0")).join("");
    if (digest !== sha) return json({ error: "Teil beschädigt angekommen, bitte erneut" }, 422);
    const part = await env.BRIEFKASTEN.resumeMultipartUpload(f.r2_key, f.upload_id).uploadPart(n, buf);
    await env.DB.prepare("INSERT INTO upload_part(file_id, n, etag, sha256, size) VALUES(?,?,?,?,?) ON CONFLICT(file_id, n) DO UPDATE SET etag=excluded.etag, sha256=excluded.sha256, size=excluded.size")
      .bind(id, n, part.etag, sha, buf.byteLength).run();
    return json({ ok: true });
  }
  if (action === "/complete" && request.method === "POST") {
    const b = await request.json();
    const f = await env.DB.prepare("SELECT * FROM upload_file WHERE id=? AND token=? AND state='laeuft'").bind(Number(b.id), token).first();
    if (!f) return json({ error: "nicht gefunden" }, 404);
    const parts = (await env.DB.prepare("SELECT n, etag, size FROM upload_part WHERE file_id=? ORDER BY n").bind(f.id).all()).results || [];
    const need = Math.max(1, Math.ceil(f.size / f.part_size));
    if (parts.length !== need || parts.reduce((s, x) => s + x.size, 0) !== f.size) return json({ error: "Es fehlen noch Teile" }, 409);
    const obj = await env.BRIEFKASTEN.resumeMultipartUpload(f.r2_key, f.upload_id).complete(parts.map((x) => ({ partNumber: x.n, etag: x.etag })));
    if (obj.size !== f.size) return json({ error: "Größe weicht ab" }, 500);
    await env.DB.prepare("UPDATE upload_file SET state='hochgeladen', uploaded_at=? WHERE id=?").bind(now(), f.id).run();
    return json({ ok: true });
  }
  return json({ error: "nicht gefunden" }, 404);
}

const PAGE = `<!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow"><title>NXT Upload</title>
<style>
:root{--bg:#0e0e10;--panel:#17171b;--line:#2a2a31;--text:#f2f2f4;--muted:#9a9aa3;--accent:#ff5a1f;--ok:#3ecf8e;--crit:#ff4d4f}
@media (prefers-color-scheme: light){:root{--bg:#f6f6f7;--panel:#fff;--line:#e3e3e7;--text:#141417;--muted:#6b6b74}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{max-width:760px;margin:0 auto;padding:32px 16px}h1{font-size:22px;margin:0 0 4px}.muted{color:var(--muted)}
.drop{margin:22px 0;border:2px dashed var(--line);border-radius:14px;padding:36px 16px;text-align:center;background:var(--panel)}
.drop.on{border-color:var(--accent)}button,label.btn{display:inline-block;background:var(--accent);color:#fff;border:0;border-radius:8px;padding:9px 16px;font:inherit;cursor:pointer;margin:4px}
label.btn.ghost{background:transparent;border:1px solid var(--line);color:var(--text)}
.bar{height:8px;background:var(--line);border-radius:6px;overflow:hidden}.bar span{display:block;height:100%;background:var(--accent);width:0}
.file{display:grid;grid-template-columns:1fr auto;gap:4px 12px;padding:10px 0;border-bottom:1px solid var(--line);font-size:14px}
.file .bar{grid-column:1/-1}.ok{color:var(--ok)}.err{color:var(--crit)}.name{word-break:break-all}
</style></head><body><main>
<div class="muted">NXT Studios · Upload</div><h1 id="title">…</h1><div class="muted" id="sub"></div>
<div class="drop" id="drop"><div style="font-size:17px;margin-bottom:10px">Dateien oder Ordner hierher ziehen</div>
<label class="btn">Dateien wählen<input type="file" id="pick" multiple hidden></label>
<label class="btn ghost">Ordner wählen<input type="file" id="pickdir" webkitdirectory multiple hidden></label>
<div class="muted" style="margin-top:10px;font-size:13px">Große Dateien sind kein Problem. Bricht die Verbindung ab, einfach dieselben Dateien erneut wählen – es geht an der Stelle weiter.</div></div>
<div id="total" hidden><div class="muted" id="totaltext"></div><div class="bar" style="margin:6px 0 14px"><span id="totalbar"></span></div></div>
<div id="list"></div>
</main><script>
const token = location.pathname.split("/")[2];
const $ = (s) => document.querySelector(s);
const fmt = (b) => b >= 1e12 ? (b/1e12).toFixed(2)+" TB" : b >= 1e9 ? (b/1e9).toFixed(1)+" GB" : b >= 1e6 ? (b/1e6).toFixed(0)+" MB" : Math.max(1,Math.round(b/1e3))+" KB";
let PART = 33554432, queue = [], totalBytes = 0, doneBytes = 0, running = 0;
fetch("/u/"+token+"/info").then(r=>r.json()).then(i=>{ if(i.error){$("#title").textContent=i.error;$("#drop").hidden=true;return;}
  PART=i.part_size; $("#title").textContent=i.name;
  $("#sub").textContent=(i.note?i.note+" · ":"")+(i.expires_at?"gültig bis "+new Date(i.expires_at).toLocaleDateString("de-DE"):"")+(i.max_bytes?" · max. "+fmt(i.max_bytes):"")+(i.files?" · bisher "+i.files+" Dateien, "+fmt(i.used_bytes):""); });
async function sha256(buf){ const d=await crypto.subtle.digest("SHA-256",buf); return [...new Uint8Array(d)].map(x=>x.toString(16).padStart(2,"0")).join(""); }
function row(f){ const el=document.createElement("div"); el.className="file";
  el.innerHTML='<div class="name"></div><div class="muted st">wartet</div><div class="bar"><span></span></div>';
  el.querySelector(".name").textContent=f.rel+" · "+fmt(f.file.size); $("#list").appendChild(el); return el; }
function upd(){ $("#total").hidden=!totalBytes; $("#totaltext").textContent=fmt(doneBytes)+" von "+fmt(totalBytes)+" hochgeladen"; $("#totalbar").style.width=(totalBytes?doneBytes/totalBytes*100:0)+"%"; }
async function api(path, opt){ for(let a=0;a<6;a++){ try{ const r=await fetch("/u/"+token+path,opt); const j=await r.json().catch(()=>({})); if(r.ok) return j;
  if(r.status<500 && r.status!==422) throw new Error(j.error||("Fehler "+r.status)); }catch(e){ if(a===5||String(e.message).startsWith("Link")) throw e; }
  await new Promise(r=>setTimeout(r,2000*(a+1))); } }
async function upload(f){ const el=f.el, st=el.querySelector(".st"), bar=el.querySelector(".bar span");
  try{ st.textContent="startet…";
    const s=await api("/start",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({rel_path:f.rel,size:f.file.size,last_modified:f.file.lastModified})});
    if(s.done){ st.textContent="schon da ✓"; st.className="ok st"; bar.style.width="100%"; doneBytes+=f.file.size; upd(); return; }
    const ps=s.part_size, n=Math.max(1,Math.ceil(f.file.size/ps)), have=new Set(s.have||[]); let mine=0;
    for(const k of have){ const b=Math.min(ps,f.file.size-(k-1)*ps); mine+=b; doneBytes+=b; }
    const todo=[]; for(let k=1;k<=n;k++) if(!have.has(k)) todo.push(k);
    const worker=async()=>{ while(todo.length){ const k=todo.shift(); const blob=f.file.slice((k-1)*ps,Math.min(f.file.size,k*ps)); const buf=await blob.arrayBuffer();
        const h=await sha256(buf); await api("/part?id="+s.id+"&n="+k,{method:"PUT",headers:{"x-part-sha256":h},body:buf});
        mine+=buf.byteLength; doneBytes+=buf.byteLength; bar.style.width=(mine/Math.max(1,f.file.size)*100)+"%"; st.textContent=Math.round(mine/Math.max(1,f.file.size)*100)+" %"; upd(); } };
    await Promise.all([worker(),worker(),worker()]);
    await api("/complete",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({id:s.id})});
    st.textContent="fertig ✓"; st.className="ok st"; bar.style.width="100%";
  }catch(e){ st.textContent="Fehler: "+e.message+" – Datei erneut wählen, um fortzusetzen"; st.className="err st"; }
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
window.onbeforeunload=()=> (running||queue.length) ? "Upload läuft noch" : undefined;
</script></body></html>`;
