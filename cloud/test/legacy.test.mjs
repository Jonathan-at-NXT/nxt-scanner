import { test } from "node:test";
import assert from "node:assert/strict";
import { legacyProjects } from "../src/legacy.js";

const disks = [{ uuid: "T1", name: "Tower 1", last_scan: "2026-10-24" }, { uuid: "VB", name: "VB02", last_scan: "2026-10-24" },
               { uuid: "OLD", name: "Tower 1", last_scan: "2026-01-01" }];
const row = (d, p, gb) => ({ disk_uuid: d, rel_path: p, size_bytes: gb * 1e9, file_count: 1 });

test("Altprojekte: Nummer-Ordner gruppiert, Unterordner und datierte Projekte ignoriert", () => {
  const out = legacyProjects([
    row("T1", "117-Doku Andrea Ammann", 3562), row("VB", "OLDER/117-Doku Andrea Ammann", 3564),
    row("T1", "117-Doku Andrea Ammann/01-ROHMATERIAL", 3000), row("T1", "2024-06-09_WOT/Tag 3", 50),
    row("T1", "2023-05-19_SPEC X/140-SPEC X", 1500), row("T1", "06-EXPORT", 20), row("OLD", "99-Alt", 50),
    row("T1", "12-Klein", 1)], disks);
  assert.deepEqual(out.map((x) => x.num), [117]);
  assert.equal(out[0].full_copies, 2);
  assert.deepEqual(out[0].disks, ["Tower 1", "VB02"]);
});

import { handoverItems, handoverPage } from "../src/handover.js";
import { handoverViewer } from "../src/access.js";

test("Übergabe-Seite zeigt nur Projekte der Person", () => {
  const items = [
    { key: "1|A", num: 1, title: "Alpha", size_gb: 50, decision: "paul", copies: [{ disk: "TOWER 1", size_gb: 50, path: "1-Alpha" }] },
    { key: "2|B", num: 2, title: "Beta <x>", size_gb: 20, decision: "clemens", copies: [{ disk: "HUB 1", size_gb: 20, path: "2-Beta" }] },
    { key: "3|C", num: 3, title: "Gamma", size_gb: 10, decision: "aussortieren", copies: [] },
  ];
  assert.deepEqual(handoverItems(items, "paul").map((x) => x.title), ["Alpha"]);
  const html = handoverPage(items, "clemens");
  assert.ok(html.includes("Beta &lt;x&gt;") && !html.includes("Alpha") && html.includes("2-Beta"));
});

test("Externe Betrachter nur mit eigener Person", () => {
  const env = { HANDOVER_VIEWERS: "paul:paul@example.com, clemens:c@example.org" };
  assert.equal(handoverViewer(env, "paul@example.com"), "paul");
  assert.equal(handoverViewer(env, "c@example.org"), "clemens");
  assert.equal(handoverViewer(env, "x@example.com"), null);
  assert.equal(handoverViewer({}, "paul@example.com"), null);
});
