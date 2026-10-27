/**
 * Slack-Brücke für den Chat mit dem NXT Data Agent (gleiche Tabelle chat_message wie der Dashboard-Reiter „Agent“).
 *
 * POST /slack/events      — Slack Events API (Signatur mit Secret SLACK_SIGNING_SECRET geprüft).
 *   Direktnachrichten an den Bot und @-Erwähnungen landen als role=user in chat_message.
 * POST /ingest/chat-reply — Bearer INGEST_TOKEN, Body {text, reply_to?}. Antwort des Agenten in den Verlauf (thread)
 *   der beantworteten Nachricht (Default: die neueste) und, wenn die aus Slack kam, als Bot (SLACK_BOT_TOKEN) dorthin.
 *
 * Secrets nur über `wrangler secret put` bzw. das Cloudflare-Dashboard, nie im Repo.
 */

const json = (obj, status = 200) =>
  new Response(JSON.stringify(obj), { status, headers: { "content-type": "application/json" } });

const now = () => new Date().toISOString();

async function hmacHex(secret, msg) {
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(msg));
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function sameString(a, b) {
  if (a.length !== b.length) return false;
  let d = 0;
  for (let i = 0; i < a.length; i++) d |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return d === 0;
}

export async function verifySlack(headers, body, secret, nowSec = Date.now() / 1000) {
  const ts = headers.get("x-slack-request-timestamp") || "";
  const sig = headers.get("x-slack-signature") || "";
  if (!secret || !ts || !sig || Math.abs(nowSec - Number(ts)) > 300) return false;
  return sameString(sig, "v0=" + (await hmacHex(secret, `v0:${ts}:${body}`)));
}

async function slackApi(env, method, payload) {
  const r = await fetch(`https://slack.com/api/${method}`, {
    method: "POST",
    headers: { authorization: `Bearer ${env.SLACK_BOT_TOKEN}`, "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify(payload),
  });
  return r.json();
}

async function slackName(env, user) {
  try {
    const r = await fetch(`https://slack.com/api/users.info?user=${encodeURIComponent(user)}`,
      { headers: { authorization: `Bearer ${env.SLACK_BOT_TOKEN}` } });
    const j = await r.json();
    const p = j.user?.profile || {};
    return p.email || p.real_name || j.user?.name || user;
  } catch { return user; }
}

// Nur echte Nachrichten von Menschen: keine Bot-Nachrichten, keine Bearbeitungen/Löschungen.
export function chatMessageFromEvent(ev) {
  if (!ev || ev.bot_id || ev.subtype) return null;
  const dm = ev.type === "message" && ev.channel_type === "im";
  if (!dm && ev.type !== "app_mention") return null;
  const text = (ev.text || "").replace(/<@[A-Z0-9]+>\s*/g, "").trim();
  if (!text || !ev.user) return null;
  return { user: ev.user, channel: ev.channel, ts: ev.ts, thread: ev.thread_ts || null, text: text.slice(0, 4000) };
}

async function events(request, env, ctx) {
  const body = await request.text();
  let p;
  try { p = JSON.parse(body); } catch { return json({ error: "invalid json" }, 400); }
  // URL-Prüfung beim Anlegen der App: kommt ggf. bevor das Secret eingetragen ist und enthält nur den Challenge-Wert.
  if (p.type === "url_verification" && !env.SLACK_SIGNING_SECRET) return json({ challenge: p.challenge });
  if (!(await verifySlack(request.headers, body, env.SLACK_SIGNING_SECRET))) return json({ error: "bad signature" }, 401);
  if (p.type === "url_verification") return json({ challenge: p.challenge });
  if (p.type !== "event_callback") return json({ ok: true });
  const m = chatMessageFromEvent(p.event);
  if (!m) return json({ ok: true });
  // Slack wiederholt Events bei langsamer Antwort: event_id ist eindeutig, doppelte werden ignoriert.
  const res = await env.DB.prepare(
    `INSERT OR IGNORE INTO chat_message(at, author, role, text, slack_event, slack_channel, slack_ts, thread)
     VALUES(?,?,?,?,?,?,?,?)`).bind(now(), `slack:${m.user}`, "user", m.text, p.event_id || m.ts, m.channel, m.thread || m.ts,
    `slack:${m.user}`).run();
  if (res.meta?.changes && env.SLACK_BOT_TOKEN) {
    ctx.waitUntil((async () => {
      const name = await slackName(env, m.user);
      // Mit E-Mail im Slack-Profil landet die Nachricht im selben Verlauf wie im Dashboard
      const thread = name.includes("@") ? name.toLowerCase() : `slack:${m.user}`;
      await env.DB.prepare("UPDATE chat_message SET author=?, thread=? WHERE slack_event=?")
        .bind(`${name} (Slack)`, thread, p.event_id || m.ts).run();
    })());
  }
  return json({ ok: true });
}

async function reply(request, env) {
  let p;
  try { p = await request.json(); } catch { return json({ error: "invalid json" }, 400); }
  const text = String(p?.text || "").trim();
  if (!text) return json({ error: "missing text" }, 400);
  const to = p.reply_to
    ? await env.DB.prepare("SELECT id, thread, slack_channel FROM chat_message WHERE id=? AND role='user'").bind(Number(p.reply_to)).first()
    : await env.DB.prepare("SELECT id, thread, slack_channel FROM chat_message WHERE role='user' ORDER BY id DESC LIMIT 1").first();
  if (!to) return json({ error: "Nachricht unbekannt" }, 404);
  await env.DB.prepare("INSERT INTO chat_message(at, author, role, text, thread) VALUES(?,?,?,?,?)")
    .bind(now(), "NXT Data Agent", "agent", text.slice(0, 8000), to.thread).run();
  let slack = null;
  if (to.slack_channel && env.SLACK_BOT_TOKEN) {
    const r = await slackApi(env, "chat.postMessage", { channel: to.slack_channel, text });
    slack = r.ok ? "gesendet" : `Fehler: ${r.error}`;
  }
  return json({ ok: true, reply_to: to.id, thread: to.thread, slack });
}

export async function handleSlack(request, env, ctx, url, authorized) {
  if (url.pathname === "/slack/events" && request.method === "POST") return events(request, env, ctx);
  if (url.pathname === "/ingest/chat-reply" && request.method === "POST") {
    if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);
    return reply(request, env);
  }
  return null;
}
