import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { validateFolder, buildDiskView, changedProps, syncDisk, runNotionSync, makeNotion } from "../src/notion_sync.js";

const here = new URL(".", import.meta.url);

test("validateFolder entspricht exakt storage_scanner/rules.py", () => {
  const cases = JSON.parse(readFileSync(new URL("rules_expected.json", here), "utf-8"));
  for (const [name, expected] of cases) assert.deepEqual(validateFolder(name), expected, name);
});

const disk = { uuid: "U1", name: "TOWER 7", last_scan: "2026-10-22T11:16:43+02:00", last_user: "Jonathan Oehler",
               capacity_bytes: 16e12, fs_used_bytes: 15.8e12, used_bytes: 1 };
const nodes = [
  { rel_path: "2025-04-01_LOCO CHIPS_FOOTAGE", depth: 1, parent_rel_path: null, size_bytes: 124.8e9, file_count: 230,
    mtime: "2025-04-02T10:00:00", abs_path: "/Volumes/TOWER 7/2025-04-01_LOCO CHIPS_FOOTAGE" },
  { rel_path: "2024-01-01_PROJ", depth: 1, parent_rel_path: null, size_bytes: 5e9, file_count: 10, mtime: "2024-01-02T00:00:00" },
  { rel_path: "2024-01-01_PROJ/2024-01-01_PROJ_FOOTAGE", depth: 2, parent_rel_path: "2024-01-01_PROJ", size_bytes: 4e9, file_count: 8, mtime: null },
  { rel_path: "2024-01-01_PROJ/EXPORTS", depth: 2, parent_rel_path: "2024-01-01_PROJ", size_bytes: 1e9, file_count: 2, mtime: null },
  { rel_path: "LOSE", depth: 1, parent_rel_path: null, size_bytes: 1e6, file_count: 1, mtime: null },
  { rel_path: "2025-04-01_LOCO CHIPS_FOOTAGE/RED", depth: 2, parent_rel_path: "2025-04-01_LOCO CHIPS_FOOTAGE", size_bytes: 1, file_count: 1 },
];

test("buildDiskView: Datenträger-Felder, Zählung, Kinder nur bei PROJECT", () => {
  const v = buildDiskView(disk, nodes);
  assert.equal(v.hdd["Gültige Projekte"].number, 2);
  assert.equal(v.hdd["Unassigned"].number, 1);
  assert.equal(v.hdd["Kapazität (GB)"].number, 16000);
  assert.equal(v.hdd["Belegt (GB)"].number, 15800);
  const byName = Object.fromEntries(v.folders.map((f) => [f.name, f]));
  assert.equal(byName["2025-04-01_LOCO CHIPS_FOOTAGE"].children.length, 0);
  assert.deepEqual(byName["2024-01-01_PROJ"].children.map((c) => c.name), ["2024-01-01_PROJ_FOOTAGE", "EXPORTS"]);
  assert.equal(byName["LOSE"].props.Status.select.name, "Unassigned");
  assert.equal(byName["2025-04-01_LOCO CHIPS_FOOTAGE"].props["Größe (GB)"].number, 124.8);
});

test("buildDiskView: unbekannte Kapazität/Pfade werden nicht angefasst", () => {
  const v = buildDiskView({ ...disk, capacity_bytes: null, fs_used_bytes: null }, nodes);
  assert.ok(!("Kapazität (GB)" in v.hdd) && !("Belegt (GB)" in v.hdd));
  const proj = v.folders.find((f) => f.name === "2024-01-01_PROJ");
  assert.ok(!("Absoluter Pfad" in proj.props));
  assert.equal(proj.fallbackPath, "/Volumes/TOWER 7/2024-01-01_PROJ");
});

// ── Fake-Notion ─────────────────────────────────────────────────────
function toPage(id, props) {
  const p = {};
  for (const [k, v] of Object.entries(props)) {
    if ("title" in v) p[k] = { type: "title", title: v.title.map((t) => ({ plain_text: t.text.content })) };
    else if ("rich_text" in v) p[k] = { type: "rich_text", rich_text: v.rich_text.map((t) => ({ plain_text: t.text.content })) };
    else if ("number" in v) p[k] = { type: "number", number: v.number };
    else if ("select" in v) p[k] = { type: "select", select: v.select };
    else if ("date" in v) p[k] = { type: "date", date: v.date };
    else if ("relation" in v) p[k] = { type: "relation", relation: v.relation };
  }
  return { id, properties: p, archived: false };
}

function fakeNotion() {
  const dbs = { HDD: [], SP: [] };
  let n = 0;
  const log = [];
  const stats = { calls: 0 };
  const find = (id) => [...dbs.HDD, ...dbs.SP].find((p) => p.id === id);
  return {
    dbs, log, stats,
    async queryAll(db, filter) {
      stats.calls++;
      const key = db === "HDD" ? "HDD" : "SP";
      return dbs[key].filter((p) => !p.archived && (
        filter.title ? p.properties.Name.title.map((t) => t.plain_text).join("") === filter.title.equals
                     : (p.properties.HDD?.relation || []).some((r) => r.id === filter.relation.contains)));
    },
    async call(method, path, body) {
      stats.calls++;
      log.push([method, path]);
      if (method === "POST" && path === "pages") {
        const page = toPage(`p${++n}`, body.properties);
        dbs[body.parent.database_id === "HDD" ? "HDD" : "SP"].push(page);
        return page;
      }
      const page = find(path.split("/")[1]);
      if (body.archived) page.archived = true;
      Object.assign(page.properties, toPage("x", body.properties || {}).properties);
      return page;
    },
  };
}
const ids = { hdd: "HDD", speicherungen: "SP" };

test("syncDisk: erster Lauf legt an, zweiter Lauf ändert nichts", async () => {
  const notion = fakeNotion();
  const r1 = await syncDisk(notion, ids, disk, nodes);
  assert.equal(r1.created, 1 + 3 + 2); // HDD + 3 Top-Level + 2 Kinder
  const r2 = await syncDisk(notion, ids, disk, nodes);
  assert.equal(r2.created + r2.updated + r2.archived, 0);
  const child = notion.dbs.SP.find((p) => p.properties.Name.title[0].plain_text === "EXPORTS");
  const parent = notion.dbs.SP.find((p) => p.properties.Name.title[0].plain_text === "2024-01-01_PROJ");
  assert.equal(child.properties.Projekt.relation[0].id, parent.id);
  assert.equal(parent.properties["Absoluter Pfad"].rich_text[0].plain_text, "/Volumes/TOWER 7/2024-01-01_PROJ");
});

test("syncDisk: Manuell nur dynamische Felder, Soll-Name bleibt, Verschwundenes wird archiviert", async () => {
  const notion = fakeNotion();
  await syncDisk(notion, ids, disk, nodes);
  const lose = notion.dbs.SP.find((p) => p.properties.Name.title[0].plain_text === "LOSE");
  lose.properties.Status = { type: "select", select: { name: "Manuell" } };
  lose.properties.Typ = { type: "select", select: { name: "FOOTAGE" } };
  lose.properties["Soll-Name"] = { type: "rich_text", rich_text: [{ plain_text: "2025-01-01_X_FOOTAGE" }] };

  const changed = nodes.filter((n) => n.rel_path !== "2025-04-01_LOCO CHIPS_FOOTAGE")
    .map((n) => (n.rel_path === "LOSE" ? { ...n, size_bytes: 9e9, file_count: 50 } : n));
  const r = await syncDisk(notion, ids, { ...disk, last_scan: "2026-10-23T09:00:00+02:00" }, changed);
  assert.equal(r.archived, 1);
  assert.equal(lose.properties.Status.select.name, "Manuell");
  assert.equal(lose.properties.Typ.select.name, "FOOTAGE");
  assert.equal(lose.properties["Soll-Name"].rich_text[0].plain_text, "2025-01-01_X_FOOTAGE");
  assert.equal(lose.properties.Dateien.number, 50);
});

test("syncDisk dry=true schreibt nichts", async () => {
  const notion = fakeNotion();
  const r = await syncDisk(notion, ids, disk, nodes, { dry: true });
  assert.ok(r.ops.length > 0);
  assert.equal(notion.log.length, 0);
});

test("changedProps erkennt gleiche Werte trotz Zeitstempel-Format", () => {
  const page = toPage("a", { "Letzter Scan": { date: { start: "2026-10-22T09:16:43.000+00:00" } }, "Dateien": { number: 5 } });
  assert.deepEqual(changedProps(page, { "Letzter Scan": { date: { start: "2026-10-22T11:16:43+02:00" } }, "Dateien": { number: 5 } }), {});
});

test("makeNotion wiederholt bei 429 mit Retry-After", async () => {
  let n = 0;
  const fetchImpl = async () => (++n === 1
    ? { ok: false, status: 429, headers: { get: () => "1" }, text: async () => "" }
    : { ok: true, json: async () => ({ ok: 1 }) });
  const waits = [];
  const c = makeNotion("t", { fetchImpl, gapMs: 0, sleep: async (ms) => waits.push(ms) });
  assert.deepEqual(await c.call("GET", "x"), { ok: 1 });
  assert.ok(waits.includes(1000));
});

test("runNotionSync: nur Platten mit neuem Scan, Stand wird gemerkt", async () => {
  const state = new Map();
  const db = {
    prepare(sql) {
      const st = { args: [], bind(...a) { st.args = a; return st; },
        async all() {
          if (sql.includes("FROM disks")) return { results: state.get("U1") === disk.last_scan ? [] : [disk] };
          return { results: nodes };
        },
        async run() { if (sql.includes("synced_scan")) state.set(st.args[0], st.args[1]); return {}; } };
      return st;
    },
  };
  const env = { DB: db, NOTION_TOKEN: "t", HDD_DB_ID: "HDD", SPEICHERUNGEN_DB_ID: "SP" };
  const notion = fakeNotion();
  const r1 = await runNotionSync(env, { notion });
  assert.equal(r1.disks.length, 1);
  assert.equal(state.get("U1"), disk.last_scan);
  const r2 = await runNotionSync(env, { notion });
  assert.equal(r2.disks.length, 0);
});
