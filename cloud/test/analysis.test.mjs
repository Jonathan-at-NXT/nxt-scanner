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

test("WORKING nur 1× macht ein Projekt nicht unsicher, FOOTAGE entscheidet", () => {
  const w = (d, gb) => ({ ...proj(d, "2026-04-15_DÖRR ROADSHOW_WORKING", gb), type: "WORKING" });
  const a = analyze({
    disks: [disk("A", "VB02"), disk("B", "TOWER 10")],
    projects: [proj("A", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 260), proj("B", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 259), w("A", 50)],
  });
  assert.equal(a.projects[0].level, "ok");
  assert.equal(a.projects[0].missing_gb, 0);
  assert.ok(!a.findings.some((f) => f.type === "MISSING_BACKUP"));
});

test("Typ-Regel: PHOTOS löschbar zählt nicht für gesichert und wird frei", () => {
  const ph = (d, gb) => ({ ...proj(d, "2026-04-15_DÖRR ROADSHOW_PHOTOS", gb), type: "PHOTOS" });
  const a = analyze({
    disks: [disk("A", "VB02"), disk("B", "TOWER 10")],
    projects: [proj("A", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 260), proj("B", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 259), ph("A", 400)],
    rules: [{ rule_key: "2026-04-15|DORR ROADSHOW", sicherung: "doppelt", type_sicherung: JSON.stringify({ PHOTOS: "loeschbar" }) }],
  });
  assert.equal(a.projects[0].level, "ok");
  assert.equal(a.cleanup.phase4.deletable_gb, 400);
});

test("Entfernter Datenträger zählt nicht, kommt nach neuem Scan zurück", () => {
  const disks = [disk("A", "VB02"), disk("B", "TOWER 10")];
  const projects = [proj("A", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 260), proj("B", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 259)];
  const off = analyze({ disks, projects, diskMeta: [{ disk_uuid: "B", retired_at: "2026-10-25T00:00:00Z", retired_reason: "Falsch angelegt" }] });
  assert.equal(off.projects[0].types[0].disks, 1);
  const back = analyze({ disks, projects, diskMeta: [{ disk_uuid: "B", retired_at: "2026-10-01T00:00:00Z", retired_reason: "x" }] });
  assert.equal(back.projects[0].types[0].disks, 2);
});

test("Kopie mit leeren Dateien (0 Byte) zählt nicht als vollständig", () => {
  const a = analyze({
    disks: [disk("A", "VB02"), disk("B", "TOWER 10")],
    projects: [proj("A", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 260), proj("B", "2026-04-15_DÖRR ROADSHOW_FOOTAGE", 260)],
    zeroFiles: [{ disk_uuid: "B", rel_path: "NXT STUDIOS/2026-04-15_DÖRR ROADSHOW_FOOTAGE/FX3/C0001.MP4" }],
  });
  assert.equal(a.projects[0].types[0].disks, 1);
  assert.equal(a.projects[0].types[0].copies.find((c) => c.disk_uuid === "B").zero, 1);
});

test("überall leere Clips (ohne intakte Version) machen keine Kopie unvollständig", () => {
  const z = (d) => ({ disk_uuid: d, rel_path: "NXT STUDIOS/2025-02-13_KTM ORANGE BLOOD_FOOTAGE/DAY 2/BTS/C0001.MP4" });
  const a = analyze({
    disks: [disk("A", "TOWER 8"), disk("B", "TOWER 11")],
    projects: [proj("A", "2025-02-13_KTM ORANGE BLOOD_FOOTAGE", 5000), proj("B", "2025-02-13_KTM ORANGE BLOOD_FOOTAGE", 5000)],
    zeroFiles: [z("A"), z("B")], zeroMissing: [],
  });
  assert.equal(a.projects[0].types[0].disks, 2);
  // ist der Clip nur auf B leer und auf A intakt, bleibt B unvollständig
  const b = analyze({
    disks: [disk("A", "TOWER 8"), disk("B", "TOWER 11")],
    projects: [proj("A", "2025-02-13_KTM ORANGE BLOOD_FOOTAGE", 5000), proj("B", "2025-02-13_KTM ORANGE BLOOD_FOOTAGE", 5000)],
    zeroFiles: [z("B")], zeroMissing: [z("B")],
  });
  assert.equal(b.projects[0].types[0].disks, 1);
});

test("unplausible Kapazität (kleiner als die Dateien) wird ignoriert", () => {
  const a = analyze({ disks: [{ uuid: "nas:X/Y", name: "NXT STUDIOS", used_bytes: 62e12, fs_used_bytes: 1.7e12, capacity_bytes: 3.6e12, last_scan: "2026-10-26" }], projects: [] });
  assert.equal(a.disks[0].used_gb, 62000);
  assert.equal(a.disks[0].capacity_gb, null);
});

test("Kennzahlen je Speicherstufe: Anzahl, belegt, Kapazität, frei", () => {
  const d = (uuid, name, usedGb, capGb) => ({ uuid, name, used_bytes: usedGb * GB, capacity_bytes: capGb * GB, last_scan: "2026-10-24T10:00:00Z" });
  const a = analyze({ disks: [d("A", "TOWER 10", 3000, 4000), d("B", "TOWER 11", 1000, 4000), d("C", "NXT 013", 500, 2000)], projects: [] });
  assert.deepEqual(a.overview.tier_stats.HDD, { disks: 2, used_tb: 4, cap_tb: 8, free_tb: 4, cap_known: 2, fill: 0.5, value_eur: 200 });
  assert.equal(a.overview.tier_stats.SSD.free_tb, 1.5);
});

test("Kennzahlen: NAS-Freigaben zählen als ein Gerät", () => {
  const n = (uuid, gb) => ({ uuid, name: uuid.split("/")[1], used_bytes: gb * GB, last_scan: "2026-10-24T10:00:00Z" });
  const a = analyze({ disks: [n("nas:NXTCLOUD/NXT STUDIOS", 80000), n("nas:NXTCLOUD/homes", 500)], diskMeta: [
    { disk_uuid: "nas:NXTCLOUD/NXT STUDIOS", tier: "NAS" }, { disk_uuid: "nas:NXTCLOUD/homes", tier: "NAS" }], projects: [] });
  assert.equal(a.overview.tier_stats.NAS.disks, 1);
  assert.equal(a.overview.tier_stats.NAS.used_tb, 80.5);
  assert.equal(a.overview.tier_stats.NAS.cap_tb, 95.7);   // feste NAS-Kapazität, SMB liefert keine
});

test("Proxies zählen nicht: Kopie ohne gelöschte Proxies bleibt vollständig", () => {
  const F = "2026-04-15_DÖRR ROADSHOW_FOOTAGE";
  const a = analyze({
    disks: [disk("A", "NXT 007"), disk("B", "NXT 037")],
    projects: [proj("A", F, 568), proj("B", F, 518)],
    proxyFiles: [{ disk_uuid: "A", rel_path: `${F}/FX3/Proxy/C0001_proxy.mp4`, size: 50 * GB },
                 { disk_uuid: "A", rel_path: `${F}/FX3/C0001.MP4`, size: 0 }],
    catalogDisks: new Set(["A", "B"]),
  });
  const t = a.projects[0].types[0];
  assert.equal(t.disks, 2);
  assert.equal(t.partial, 0);
  assert.equal(t.copies.find((c) => c.disk_uuid === "A").proxy_gb, 50);
});

test("Platte ohne Katalog: Proxies wie bei der bekannten Kopie angenommen", () => {
  const F = "2026-04-15_DÖRR ROADSHOW_FOOTAGE";
  const a = analyze({
    disks: [disk("A", "TOWER 11"), disk("B", "Connor #1")],
    projects: [proj("A", F, 140), proj("B", F, 140)],
    proxyFiles: [{ disk_uuid: "A", rel_path: `${F}/ProxyMedia/x.mov`, size: 137 * GB }],
    catalogDisks: new Set(["A"]),
  });
  assert.equal(a.projects[0].types[0].disks, 2);
});

test("NAS: Parkordner zählt als frei, Belegung ohne Parkordner", async () => {
  const { nasSpaceGb } = await import("../src/analysis.js");
  const v = nasSpaceGb({ cap_bytes: 96e12, used_bytes: 94e12, avail_bytes: 2e12, park_bytes: 19e12 });
  assert.equal(Math.round(v.free_gb), 21000);
  assert.equal(Math.round(v.used_gb), 75000);
  assert.equal(nasSpaceGb(null), null);
});
