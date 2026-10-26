// Übergabe-Seiten (/uebergabe/clemens, /uebergabe/paul): nur die Altprojekte, die im Dashboard
// „An Clemens geben“ bzw. „An Paul geben“ bekommen haben. Serverseitig gerendert, ohne Skript.

import { HANDOVER } from "./legacy.js";

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const gb = (x) => (x >= 1000 ? `${(x / 1000).toLocaleString("de-DE", { maximumFractionDigits: 2 })} TB` : `${Math.round(x).toLocaleString("de-DE")} GB`);

export function handoverItems(items, person) {
  return items.filter((x) => x.decision === person)
    .sort((a, b) => (a.num ?? 1000 + Number((a.date || "").slice(0, 4))) - (b.num ?? 1000 + Number((b.date || "").slice(0, 4))) || a.title.localeCompare(b.title, "de"));
}

export function handoverPage(items, person, { me = "", generatedAt = new Date().toISOString() } = {}) {
  const who = HANDOVER[person];
  const list = handoverItems(items, person);
  const total = list.reduce((s, x) => s + (x.size_gb || 0), 0);
  const rows = list.map((x) => {
    const best = [...x.copies].sort((a, b) => b.size_gb - a.size_gb);
    return `<tr><td class="num">${x.num != null ? x.num : `<span class="sub">${esc(x.date)}</span>`}</td>
      <td><b>${esc(x.title)}</b>${x.note ? `<div class="sub">${esc(x.note)}</div>` : ""}</td>
      <td class="num r">${gb(x.size_gb)}</td>
      <td>${best.map((c) => `<div><b>${esc(c.disk)}</b> <span class="sub">${gb(c.size_gb)}${c.size_gb < x.size_gb * 0.95 ? " · nur Teil" : ""}</span>
        ${c.path ? `<div class="sub mono" style="word-break:break-all">${esc(c.path)}</div>` : ""}</div>`).join("")}</td></tr>`;
  }).join("");
  return `<!doctype html>
<html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow"><title>Übergabe an ${esc(who)}</title><link rel="stylesheet" href="/app.css"></head>
<body><header class="topbar"><span class="brand"><span class="brand-nxt">NXT</span><span class="brand-sub">Übergabe an ${esc(who)}</span></span>
<div class="me">${esc(me)}</div></header>
<main id="view">
<div class="head"><div><h1>Übergabe an ${esc(who)}</h1><p>Altprojekte, die an ${esc(who)} gehen. Je Projekt steht, auf welchem Datenträger
und in welchem Ordner es liegt; die größte Kopie steht oben. Stand ${esc(new Date(generatedAt).toLocaleString("de-DE", { timeZone: "Europe/Berlin" }))}.</p></div></div>
<div class="kpis"><div class="kpi"><div class="label">Projekte</div><div class="value num">${list.length}</div></div>
<div class="kpi"><div class="label">Umfang</div><div class="value num">${gb(total)}</div><div class="sub">je Projekt einmal gezählt</div></div></div>
<div class="panel" style="margin-top:18px">${list.length ? `<table class="table"><thead><tr><th>Nr. / Datum</th><th>Projekt</th><th class="r">Größe</th><th>Liegt auf</th></tr></thead>
<tbody>${rows}</tbody></table>` : `<div class="empty">Noch keine Projekte für ${esc(who)} ausgewählt.</div>`}</div>
</main></body></html>`;
}
