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

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return json({ ok: true });
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
