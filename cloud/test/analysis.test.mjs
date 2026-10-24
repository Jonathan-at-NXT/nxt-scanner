import { test } from "node:test";
import assert from "node:assert/strict";
import { analyze } from "../src/analysis.js";

const GB = 1e9;
const disk = (uuid, name) => ({ uuid, name, used_bytes: 1, capacity_bytes: 10 * GB, last_scan: "2026-10-24T10:00:00Z" });
const proj = (disk_uuid, rel_path, gb) => ({ disk_uuid, rel_path, name: rel_path, date: "2026-04-15", project_name: "DÖRR ROADSHOW",
  norm_name: "DORR ROADSHOW", type: "FOOTAGE", status: "validated", size_bytes: gb * GB, cluster_id: "2026-04-15|DORR ROADSHOW" });

test("Teilkopien (nur ein Drehtag) zählen nicht als volle Kopie", () => {
  const a = analyze({
    disks: [disk("A", "VB02"), disk("B", "TOWER 10"), disk("C", "NXT 013"), disk("D", "NXT 024")],
    projects: [proj("A", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 260), proj("B", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 259),
               proj("C", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 135), proj("D", "2026-04-16_DÖRR ROADSHOW_FOOTAGE", 116)],
  });
  const t = a.projects[0].types[0];
  assert.equal(t.disks, 2);
  assert.equal(t.partial, 2);
  assert.equal(a.projects[0].level, "ok");
  assert.ok(!a.findings.some((f) => f.type === "EXCESS_COPIES"));
});

test("Tagesordner auf derselben Platte werden addiert", () => {
  const a = analyze({
    disks: [disk("A", "VB02"), disk("B", "TOWER 10")],
    projects: [proj("A", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 260),
               proj("B", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 135), proj("B", "2026-04-16_DÖRR ROADSHOW_FOOTAGE", 124)],
  });
  assert.equal(a.projects[0].types[0].disks, 2);
});
