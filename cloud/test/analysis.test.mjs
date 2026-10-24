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

test("Teilkopien werden frei, sobald die Regel mit vollständigen Kopien erfüllt ist", () => {
  const a = analyze({
    disks: [disk("A", "VB02"), disk("B", "TOWER 10"), disk("C", "NXT 013")],
    projects: [proj("A", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 260), proj("B", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 259),
               proj("C", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 135)],
  });
  const f = a.findings.find((x) => x.type === "PARTIAL_COPY");
  assert.equal(f.savings_gb, 135);
  assert.equal(a.cleanup.phase4.partial_gb, 135);
});

test("Sollstand: kleinere Kopie als Referenz, Mehrmaterial wird frei", () => {
  const p = (d, gb) => ({ ...proj(d, "2023-05-19_SPEC X_FOOTAGE", gb), date: "2023-05-19", project_name: "SPEC X", norm_name: "SPEC X", cluster_id: "2023-05-19|SPEC X" });
  const a = analyze({
    disks: [disk("A", "NB 01"), disk("B", "Tower 3"), disk("C", "NXT HUB 02")],
    projects: [p("A", 3690), p("B", 1720), p("C", 1720)],
    rules: [{ rule_key: "2023-05-19|SPEC X", sicherung: "doppelt", ref_copies: JSON.stringify({ FOOTAGE: "B" }) }],
  });
  const t = a.projects[0].types[0];
  assert.equal(t.disks, 3);
  assert.equal(t.size_gb, 1720);
  assert.equal(a.findings.find((x) => x.type === "REF_EXTRA").savings_gb, 1970);
});
