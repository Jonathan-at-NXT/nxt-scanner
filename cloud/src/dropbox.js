// Dropbox-Ausstieg (/dropbox): Ordner, die nur in der Dropbox liegen, je Ordner entscheiden.
// Daten kommen vom Sicherungs-Mac (Abgleich Name + Größe gegen alle Platten und das NAS).

export const DROPBOX_CHOICES = { sichern: "Sichern", loeschen: "Darf weg", ansehen: "Nochmal ansehen" };
const SUGGESTION = { sichern: "Vorschlag: sichern", accounts: "accounts (niedrige Priorität)", weg: "Vorschlag: darf weg (Proxys)" };

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const gb = (x) => (x >= 1000 ? `${(x / 1000).toLocaleString("de-DE", { maximumFractionDigits: 2 })} TB` : `${Math.round(x).toLocaleString("de-DE")} GB`);

// Bereich = erste zwei Ebenen unterhalb von „NXT STUDIOS“ bzw. „NXT STUDIOS (accounts)“
export const section = (path) => path.split("/").slice(0, 2).join(" / ");

export function dropboxSummary(rows) {
  const sum = (f) => rows.filter(f).reduce((s, r) => s + r.gb, 0);
  return { total: sum(() => true), sichern: sum((r) => r.choice === "sichern"), loeschen: sum((r) => r.choice === "loeschen"),
           ansehen: sum((r) => r.choice === "ansehen"), offen: sum((r) => !r.choice), offen_n: rows.filter((r) => !r.choice).length };
}

export function dropboxPage(rows, { me = "", generatedAt = new Date().toISOString() } = {}) {
  const s = dropboxSummary(rows);
  const groups = new Map();
  for (const r of [...rows].sort((a, b) => b.gb - a.gb)) {
    const k = section(r.path);
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(r);
  }
  const body = [...groups].sort((a, b) => b[1].reduce((t, r) => t + r.gb, 0) - a[1].reduce((t, r) => t + r.gb, 0)).map(([k, list]) => `
    <tr class="grp"><td colspan="3"><b>${esc(k)}</b> <span class="sub">${list.length} Ordner · ${gb(list.reduce((t, r) => t + r.gb, 0))}</span></td></tr>
    ${list.map((r) => `<tr><td><b>${esc(r.path.split("/").slice(2).join("/") || r.path)}</b><div class="sub">${esc(SUGGESTION[r.suggestion] || "")} · ${r.files.toLocaleString("de-DE")} Dateien</div></td>
      <td class="num r">${gb(r.gb)}</td>
      <td><div class="row" style="gap:6px;flex-wrap:wrap">${Object.entries(DROPBOX_CHOICES).map(([c, label]) => `<button class="btn small ${r.choice === c ? "" : "ghost"}"
          data-ho-key="${esc(r.path)}" data-ho-choice="${r.choice === c ? "" : c}" title="${r.choice === c ? "Auswahl zurücknehmen" : esc(label)}">${esc(label)}</button>`).join("")}</div>
        <textarea data-ho-msg="${esc(r.path)}" rows="1" maxlength="2000" placeholder="Notiz (optional)" style="width:100%;margin-top:6px">${esc(r.message || "")}</textarea>
        ${r.updated_by ? `<div class="sub faint">${esc(r.updated_by)} · ${esc(new Date(r.updated_at).toLocaleString("de-DE", { timeZone: "Europe/Berlin" }))}</div>` : ""}</td></tr>`).join("")}`).join("");
  return `<!doctype html>
<html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow"><title>Dropbox-Ausstieg</title><link rel="stylesheet" href="/app.css">
<script>window.HO_API = "/api/dropbox";</script><script src="/handover.js" defer></script></head>
<body><header class="topbar"><span class="brand"><span class="brand-nxt">NXT</span><span class="brand-sub">Dropbox-Ausstieg</span></span>
<div class="me"><a href="/">Zum Dashboard</a> · ${esc(me)}</div></header>
<main id="view">
<div class="head"><div><h1>Dropbox-Ausstieg</h1><p>Diese Ordner liegen <b>nur</b> in der Dropbox (Dateien ab 50 MB, Abgleich nach Name und Größe mit allen Platten und dem NAS).
Bevor die Dropbox gekündigt wird, bitte je Ordner wählen: <b>Sichern</b> (wird aufs NAS geladen und 2× gesichert), <b>Darf weg</b> oder <b>Nochmal ansehen</b>.
Die Notiz speichert beim Verlassen des Feldes. Stand ${esc(new Date(generatedAt).toLocaleString("de-DE", { timeZone: "Europe/Berlin" }))}.</p></div></div>
<div class="kpis"><div class="kpi"><div class="label">Nur in Dropbox</div><div class="value num">${gb(s.total)}</div><div class="sub">${rows.length} Ordner</div></div>
<div class="kpi"><div class="label">Sichern</div><div class="value num" style="color:var(--ok)">${gb(s.sichern)}</div></div>
<div class="kpi"><div class="label">Darf weg</div><div class="value num">${gb(s.loeschen)}</div></div>
<div class="kpi"><div class="label">Noch offen</div><div class="value num" style="color:var(--warn)">${gb(s.offen + s.ansehen)}</div><div class="sub">${s.offen_n} Ordner ohne Auswahl${s.ansehen ? ` · ${gb(s.ansehen)} ansehen` : ""}</div></div></div>
<div class="panel" style="margin-top:18px">${rows.length ? `<table class="table"><thead><tr><th>Ordner</th><th class="r">Größe</th><th>Entscheidung</th></tr></thead>
<tbody>${body}</tbody></table>` : `<div class="empty">Keine Daten.</div>`}</div>
<p id="ho-status" class="sub" role="status"></p>
</main></body></html>`;
}
