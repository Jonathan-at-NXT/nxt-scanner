/**
 * Zentrale Ableitung im Worker (Port von cloud/derive.py):
 * folder_tree (depth 1) -> projects (Klassifikation + Projekt-Cluster über alle Platten).
 *
 * Abweichung zu derive.py: die cluster_id ist der kleinste "DATUM|NAME"-Schlüssel
 * der Mitglieder statt des Union-Find-Wurzelknotens. Damit hängt sie nicht von der
 * Reihenfolge der Zeilen ab, und Sicherungsregeln (project_rule) bleiben stabil.
 */

import { validateFolder } from "./notion_sync.js";

export const CONTAINER = new Set(["OLDER", "OTHER", "NXT STUDIOS", "NXTSTUDIOS", "DIVERSE", "MISC"]);

// Python: re.sub(r"[_\-]", " ", s.upper()); re.sub(r"[^A-Z0-9 ]", "", s); re.sub(r"\s+", " ", s).strip()
export function norm(name) {
  let s = (name || "").toUpperCase().replace(/[_-]/g, " ");
  s = s.replace(/[^A-Z0-9 ]/g, "");
  return s.replace(/\s+/g, " ").trim();
}

// ── difflib.SequenceMatcher(None, a, b).ratio() — originalgetreuer Port ──
export function seqRatio(a, b) {
  const la = a.length, lb = b.length;
  if (la + lb === 0) return 1.0;
  // __chain_b mit autojunk (ab 200 Zeichen)
  const b2j = new Map();
  for (let i = 0; i < lb; i++) {
    const c = b[i];
    if (!b2j.has(c)) b2j.set(c, []);
    b2j.get(c).push(i);
  }
  if (lb >= 200) {
    const ntest = Math.floor(lb / 100) + 1;
    for (const [c, idxs] of [...b2j]) if (idxs.length > ntest) b2j.delete(c);
  }
  const findLongest = (alo, ahi, blo, bhi) => {
    let besti = alo, bestj = blo, bestsize = 0;
    let j2len = new Map();
    for (let i = alo; i < ahi; i++) {
      const newj2len = new Map();
      for (const j of b2j.get(a[i]) || []) {
        if (j < blo) continue;
        if (j >= bhi) break;
        const k = (j2len.get(j - 1) || 0) + 1;
        newj2len.set(j, k);
        if (k > bestsize) { besti = i - k + 1; bestj = j - k + 1; bestsize = k; }
      }
      j2len = newj2len;
    }
    // ohne isjunk: Verlängerung über "populäre" Elemente wie in difflib
    while (besti > alo && bestj > blo && a[besti - 1] === b[bestj - 1]) { besti--; bestj--; bestsize++; }
    while (besti + bestsize < ahi && bestj + bestsize < bhi && a[besti + bestsize] === b[bestj + bestsize]) bestsize++;
    return [besti, bestj, bestsize];
  };
  let matches = 0;
  const queue = [[0, la, 0, lb]];
  while (queue.length) {
    const [alo, ahi, blo, bhi] = queue.pop();
    const [i, j, k] = findLongest(alo, ahi, blo, bhi);
    if (k) {
      matches += k;
      if (alo < i && blo < j) queue.push([alo, i, blo, j]);
      if (i + k < ahi && j + k < bhi) queue.push([i + k, ahi, j + k, bhi]);
    }
  }
  return (2.0 * matches) / (la + lb);
}

/** sollName (optional): manueller Soll-Name aus folder_override; gilt vor dem echten Namen. */
export function classify(node, sollName = null) {
  const name = (node.rel_path || "").split("/").pop();
  const out = {
    disk_uuid: node.disk_uuid, rel_path: node.rel_path, name,
    size_bytes: node.size_bytes ?? null, file_count: node.file_count ?? null, mtime: node.mtime ?? null,
    date: null, project_name: null, type: null, norm_name: norm(name), status: "unassigned", cluster_id: null,
    soll_name: sollName || null,
  };
  const sv = sollName ? validateFolder(sollName) : null;
  if (sv) {
    Object.assign(out, { date: sv.date, project_name: sv.project_name, type: sv.type, norm_name: norm(sv.project_name), status: "validated" });
    return out;
  }
  if (CONTAINER.has(norm(name))) { out.status = "container"; return out; }
  const v = validateFolder(name);
  if (v) Object.assign(out, { date: v.date, project_name: v.project_name, type: v.type, norm_name: norm(v.project_name), status: "validated" });
  return out;
}

function dateClose(d1, d2, days = 2) {
  if (d1 === d2) return true;
  if (!d1 || !d2) return false;
  const a = Date.parse(d1 + "T00:00:00Z"), b = Date.parse(d2 + "T00:00:00Z");
  if (Number.isNaN(a) || Number.isNaN(b)) return false;
  return Math.abs(a - b) / 86400000 <= days;
}

/** Setzt cluster_id (nur validierte). Gleiche Regeln wie derive.assign_clusters. */
export function assignClusters(projects) {
  const idx = projects.map((p, i) => (p.status === "validated" ? i : -1)).filter((i) => i >= 0);
  const parent = new Map(idx.map((i) => [i, i]));
  const find = (i) => { while (parent.get(i) !== i) { parent.set(i, parent.get(parent.get(i))); i = parent.get(i); } return i; };
  const union = (a, b) => { const ra = find(a), rb = find(b); if (ra !== rb) parent.set(rb, ra); };

  const buckets = new Map();
  for (const i of idx) {
    const k = projects[i].norm_name.slice(0, 3);
    if (!buckets.has(k)) buckets.set(k, []);
    buckets.get(k).push(i);
  }
  for (const i of idx) {
    const pi = projects[i];
    for (const j of buckets.get(pi.norm_name.slice(0, 3)) || []) {
      if (j <= i) continue;
      const pj = projects[j];
      const a = pi.norm_name, b = pj.norm_name;
      if (!a || !b) continue;
      const sameDate = pi.date === pj.date;
      let same = (sameDate && a === b) || (a === b && dateClose(pi.date, pj.date));
      if (!same && (sameDate || dateClose(pi.date, pj.date))) {
        const sim = seqRatio(a, b);
        same = (sameDate && sim >= 0.86) || (sim >= 0.93 && dateClose(pi.date, pj.date));
      }
      if (same) union(i, j);
    }
  }
  const keyOf = (p) => `${p.date}|${p.norm_name}`;
  const best = new Map();
  for (const i of idx) {
    const r = find(i), k = keyOf(projects[i]);
    if (!best.has(r) || k < best.get(r)) best.set(r, k);
  }
  for (const i of idx) projects[i].cluster_id = best.get(find(i));
  return projects;
}

/** Liest folder_tree aus D1, schreibt projects + scans_log neu (wie derive.py). */
export async function runDerive(env) {
  const nodes = (await env.DB.prepare(
    "SELECT disk_uuid, rel_path, size_bytes, file_count, mtime FROM folder_tree WHERE depth=1 ORDER BY disk_uuid, rel_path").all()).results || [];
  const overrides = new Map(((await env.DB.prepare("SELECT disk_uuid, rel_path, soll_name FROM folder_override").all()).results || [])
    .map((o) => [`${o.disk_uuid}|${o.rel_path}`, o.soll_name]));
  const projects = assignClusters(nodes.map((n) => classify(n, overrides.get(`${n.disk_uuid}|${n.rel_path}`))));
  const disks = (await env.DB.prepare("SELECT uuid, used_bytes, last_scan FROM disks").all()).results || [];
  const counts = (await env.DB.prepare("SELECT disk_uuid, COUNT(*) n FROM folder_tree GROUP BY disk_uuid").all()).results || [];
  const nodeCount = new Map(counts.map((r) => [r.disk_uuid, r.n]));
  const valid = new Map(), unassigned = new Map();
  for (const p of projects) {
    if (p.status === "validated") valid.set(p.disk_uuid, (valid.get(p.disk_uuid) || 0) + 1);
    else if (p.status === "unassigned") unassigned.set(p.disk_uuid, (unassigned.get(p.disk_uuid) || 0) + 1);
  }
  const cols = ["disk_uuid", "rel_path", "name", "date", "project_name", "norm_name", "type", "status", "size_bytes", "file_count", "mtime", "cluster_id", "soll_name"];
  const ins = env.DB.prepare(`INSERT INTO projects(${cols.join(",")}) VALUES(${cols.map(() => "?").join(",")})`);
  const insLog = env.DB.prepare(
    "INSERT INTO scans_log(disk_uuid, scan_date, used_bytes, valid_count, unassigned_count, node_count) VALUES(?,?,?,?,?,?)");
  const stmts = [env.DB.prepare("DELETE FROM projects"), env.DB.prepare("DELETE FROM scans_log")];
  for (const p of projects) stmts.push(ins.bind(...cols.map((c) => p[c] ?? null)));
  for (const d of disks) stmts.push(insLog.bind(d.uuid, d.last_scan ?? null, d.used_bytes ?? null,
    valid.get(d.uuid) || 0, unassigned.get(d.uuid) || 0, nodeCount.get(d.uuid) || 0));
  await env.DB.batch(stmts); // eine Transaktion: Dashboard sieht nie einen halben Stand
  return { nodes: nodes.length, projects: projects.length,
           validated: projects.filter((p) => p.status === "validated").length,
           clusters: new Set(projects.filter((p) => p.cluster_id).map((p) => p.cluster_id)).size };
}

/** Nur neu ableiten, wenn sich Scans oder Soll-Namen geändert haben (spart D1-Schreibzugriffe). */
export async function deriveIfChanged(env) {
  const r = await env.DB.prepare(
    `SELECT (SELECT COALESCE(MAX(last_scan),'') || ':' || COUNT(*) FROM disks) || '/' ||
            (SELECT COALESCE(MAX(updated_at),'') || ':' || COUNT(*) FROM folder_override) AS stamp`).first();
  const stamp = r?.stamp || "";
  const prev = await env.DB.prepare("SELECT value FROM meta WHERE key='derive_stamp'").first();
  if (prev?.value === stamp) return { skipped: true };
  const res = await runDerive(env);
  await env.DB.prepare("INSERT INTO meta(key,value) VALUES('derive_stamp',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
    .bind(stamp).run();
  return res;
}
