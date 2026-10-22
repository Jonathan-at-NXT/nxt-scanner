import { test } from "node:test";
import assert from "node:assert/strict";
import { norm, seqRatio, classify, assignClusters } from "../src/derive.js";

test("norm wie derive.py", () => {
  assert.equal(norm("2025-04-01_Loco-Chips"), "2025 04 01 LOCO CHIPS");
  assert.equal(norm("Süd_tirol  ß"), "SD TIROL SS");
});

test("seqRatio wie difflib.SequenceMatcher.ratio (Referenzwerte aus Python)", () => {
  const cases = [["LOCO CHIPS", "LOCO CHIP", 0.9473684210526315], ["KTM ORANGE BLOOD", "KTM ORANGE LIGHT", 0.75],
                 ["ABCD", "BCDA", 0.75], ["", "", 1.0], ["MACHERFESTIVAL 2025", "MACHERFESTIVAL", 0.8484848484848485]];
  for (const [a, b, r] of cases) assert.ok(Math.abs(seqRatio(a, b) - r) < 1e-12, `${a} / ${b}: ${seqRatio(a, b)}`);
});

test("Cluster-ID ist unabhängig von der Reihenfolge", () => {
  const nodes = [
    { disk_uuid: "B", rel_path: "2025-04-02_LOCO CHIPS_WORKING" },
    { disk_uuid: "A", rel_path: "2025-04-01_LOCO CHIPS_FOOTAGE" },
    { disk_uuid: "C", rel_path: "2025-04-01_LOCO CHIP_FOOTAGE" },
    { disk_uuid: "C", rel_path: "LOSE" },
  ];
  const ids1 = assignClusters(nodes.map(classify)).map((p) => p.cluster_id);
  const ids2 = assignClusters([...nodes].reverse().map(classify)).map((p) => p.cluster_id).reverse();
  assert.deepEqual(ids1, ids2);
  assert.equal(new Set(ids1.filter(Boolean)).size, 1);
  assert.equal(ids1[3], null);
});

test("Soll-Name ordnet unbenannten Ordner einem Projekt zu", () => {
  const p = classify({ disk_uuid: "A", rel_path: "PAULA" }, "2026-08-06_MACHERFESTIVAL_FOOTAGE");
  assert.equal(p.status, "validated");
  assert.equal(p.type, "FOOTAGE");
  assert.equal(p.soll_name, "2026-08-06_MACHERFESTIVAL_FOOTAGE");
  assert.equal(p.name, "PAULA");
});
