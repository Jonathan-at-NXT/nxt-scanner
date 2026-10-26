/**
 * Login über Cloudflare Access: Access sitzt vor dem Dashboard und hängt jedem
 * Request ein signiertes JWT an (Header Cf-Access-Jwt-Assertion). Wir prüfen
 * Signatur (Schlüssel des Teams), Audience, Issuer und Ablauf selbst nach, damit
 * das Dashboard auch dann zu bleibt, wenn Access falsch konfiguriert wäre.
 * Ohne Konfiguration: alles gesperrt (fail closed).
 */

let jwksCache = { at: 0, keys: null, team: null };

const b64url = (s) => Uint8Array.from(atob(s.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((s.length + 3) % 4)), (c) => c.charCodeAt(0));

async function keys(team, fetchImpl = fetch) {
  if (jwksCache.keys && jwksCache.team === team && Date.now() - jwksCache.at < 3600_000) return jwksCache.keys;
  const resp = await fetchImpl(`https://${team}/cdn-cgi/access/certs`);
  if (!resp.ok) throw new Error(`Access-Zertifikate nicht ladbar (${resp.status})`);
  const { keys: jwks } = await resp.json();
  jwksCache = { at: Date.now(), team, keys: jwks };
  return jwks;
}

/** Gibt {email} zurück oder wirft. */
export async function verifyAccess(request, env, { fetchImpl = fetch, now = Date.now() } = {}) {
  const team = env.ACCESS_TEAM_DOMAIN, aud = env.ACCESS_AUD;
  if (!team || !aud) throw new Error("Login nicht konfiguriert");
  const token = request.headers.get("cf-access-jwt-assertion");
  if (!token) throw new Error("kein Access-Token");
  const [h, p, s] = token.split(".");
  if (!h || !p || !s) throw new Error("Token ungültig");
  const header = JSON.parse(new TextDecoder().decode(b64url(h)));
  const payload = JSON.parse(new TextDecoder().decode(b64url(p)));
  if (header.alg !== "RS256") throw new Error("Algorithmus nicht erlaubt");
  const jwk = (await keys(team, fetchImpl)).find((k) => k.kid === header.kid);
  if (!jwk) throw new Error("Schlüssel unbekannt");
  const key = await crypto.subtle.importKey("jwk", jwk, { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["verify"]);
  const ok = await crypto.subtle.verify("RSASSA-PKCS1-v1_5", key, b64url(s), new TextEncoder().encode(`${h}.${p}`));
  if (!ok) throw new Error("Signatur ungültig");
  const auds = Array.isArray(payload.aud) ? payload.aud : [payload.aud];
  if (!auds.includes(aud)) throw new Error("falsche Audience");
  if (payload.iss !== `https://${team}`) throw new Error("falscher Aussteller");
  if (!payload.exp || payload.exp * 1000 < now) throw new Error("Token abgelaufen");
  const email = (payload.email || "").toLowerCase();
  if (!email) throw new Error("keine E-Mail im Token");
  const allowed = (env.ALLOWED_EMAIL_DOMAINS || "nxtstudios.de").split(",").map((d) => d.trim().toLowerCase());
  if (allowed.some((d) => email.endsWith("@" + d))) return { email };
  const only = handoverViewer(env, email);
  if (only) return { email, only };
  throw new Error("E-Mail-Domain nicht erlaubt");
}

/** Externe Personen, die nur ihre Übergabe-Seite sehen dürfen: HANDOVER_VIEWERS = "paul:a@x.de,clemens:b@y.de". */
export function handoverViewer(env, email) {
  for (const part of String(env.HANDOVER_VIEWERS || "").split(",")) {
    const [person, mail] = part.split(":").map((x) => (x || "").trim().toLowerCase());
    if (person && mail && mail === email) return person;
  }
  return null;
}

/** Nur für lokale Entwicklung (wrangler dev auf localhost), nie in Produktion. */
export function devUser(request, env) {
  const host = new URL(request.url).hostname;
  if (env.DEV_USER && (host === "localhost" || host === "127.0.0.1")) return { email: env.DEV_USER };
  return null;
}

export const _resetCache = () => { jwksCache = { at: 0, keys: null, team: null }; };
