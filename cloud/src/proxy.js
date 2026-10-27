/**
 * Weiterleitung nach dem Account-Umzug: läuft unter dem ALTEN Namen/Account (nxt-scanner-ingest.nxtstudios.workers.dev)
 * und reicht jede Anfrage unverändert (Methode, Header, Body, Pfad, Query) an den neuen Worker TARGET weiter.
 * Kein 307: manche Clients verlieren bei umgeleiteten POSTs den Body. Kein Cron, keine Bindings.
 * Bleibt stehen, bis Scanner, NAS-Container, Slack und verschickte Briefkasten-Links die neue URL nutzen.
 */
export default {
  async fetch(request, env) {
    if (!env.TARGET) return new Response("TARGET fehlt", { status: 500 });
    const url = new URL(request.url);
    const to = new URL(url.pathname + url.search, env.TARGET);
    const headers = new Headers(request.headers);
    headers.delete("host");
    const init = { method: request.method, headers, redirect: "manual" };
    if (!["GET", "HEAD"].includes(request.method)) { init.body = request.body; init.duplex = "half"; }
    return fetch(to, init);
  },
};
