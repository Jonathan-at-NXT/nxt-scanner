// Übergabe-Seiten (/uebergabe/clemens, /uebergabe/paul): nur die Altprojekte, die im Dashboard
// „An Clemens geben“ bzw. „An Paul geben“ bekommen haben. Die Person entscheidet je Projekt selbst
// (Aussortieren / Nur 1× / Behalten / Nochmal ansehen) und kann eine Nachricht hinterlassen.

import { HANDOVER, LEGACY_DECISIONS } from "./legacy.js";

export const HANDOVER_CHOICES = ["aussortieren", "einfach", "behalten", "ansehen"];

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
  const open = list.filter((x) => !x.handover?.choice).length;
  const rows = list.map((x) => {
    const best = [...x.copies].sort((a, b) => b.size_gb - a.size_gb);
    const h = x.handover || {};
    return `<tr><td class="num">${x.num != null ? x.num : `<span class="sub">${esc(x.date)}</span>`}</td>
      <td><b>${esc(x.title)}</b>${x.note ? `<div class="sub">${esc(x.note)}</div>` : ""}</td>
      <td class="num r">${gb(x.size_gb)}</td>
      <td>${best.map((c) => `<div><b>${esc(c.disk)}</b> <span class="sub">${gb(c.size_gb)}${c.size_gb < x.size_gb * 0.95 ? " · nur Teil" : ""}</span>
        ${c.path ? `<div class="sub mono" style="word-break:break-all">${esc(c.path)}</div>` : ""}</div>`).join("")}</td>
      <td><div class="row" style="gap:6px;flex-wrap:wrap">${HANDOVER_CHOICES.map((k) => `<button class="btn small ${h.choice === k ? "" : "ghost"}"
          data-ho-key="${esc(x.key)}" data-ho-choice="${h.choice === k ? "" : k}" title="${h.choice === k ? "Auswahl zurücknehmen" : esc(LEGACY_DECISIONS[k])}">${esc(LEGACY_DECISIONS[k])}</button>`).join("")}</div>
        <textarea class="ho-msg" data-ho-msg="${esc(x.key)}" rows="2" maxlength="2000" placeholder="Nachricht (optional)" style="width:100%;margin-top:6px">${esc(h.message || "")}</textarea>
        ${h.by ? `<div class="sub faint">${esc(h.by)} · ${esc(new Date(h.at).toLocaleString("de-DE", { timeZone: "Europe/Berlin" }))}</div>` : ""}</td></tr>`;
  }).join("");
  return `<!doctype html>
<html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow"><title>Übergabe an ${esc(who)}</title><link rel="stylesheet" href="/app.css">
<script src="/handover.js" defer></script></head>
<body><header class="topbar"><span class="brand"><span class="brand-nxt">NXT</span><span class="brand-sub">Übergabe an ${esc(who)}</span></span>
<div class="me">${esc(me)}</div></header>
<main id="view">
<div class="head"><div><h1>Übergabe an ${esc(who)}</h1><p>Altprojekte, die an ${esc(who)} gehen. Je Projekt bitte auswählen: <b>Aussortieren</b> (darf gelöscht werden),
<b>Nur 1×</b> (eine Kopie reicht), <b>Behalten (2×)</b> (doppelt sichern) oder <b>Nochmal ansehen</b>. Für alles andere gibt es das Nachrichtenfeld,
es speichert beim Verlassen des Feldes. Liegt auf: Datenträger und Ordner, die größte Kopie oben. Stand ${esc(new Date(generatedAt).toLocaleString("de-DE", { timeZone: "Europe/Berlin" }))}.</p></div></div>
<div class="grid kpis"><div class="panel kpi"><div class="label">Projekte</div><div class="value num">${list.length}</div><div class="sub">noch ohne Auswahl: ${open}</div></div>
<div class="panel kpi"><div class="label">Umfang</div><div class="value num">${gb(total)}</div><div class="sub">je Projekt einmal gezählt</div></div></div>
<div class="panel" style="margin-top:18px">${list.length ? `<table class="table"><thead><tr><th>Nr. / Datum</th><th>Projekt</th><th class="r">Größe</th><th>Liegt auf</th><th>Entscheidung</th></tr></thead>
<tbody>${rows}</tbody></table>` : `<div class="empty">Noch keine Projekte für ${esc(who)} ausgewählt.</div>`}</div>
<p id="ho-status" class="sub" role="status"></p>
</main></body></html>`;
}
