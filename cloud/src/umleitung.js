/** Altes Dashboard nach dem Account-Umzug: jede Seite dauerhaft aufs neue Dashboard umleiten (Pfad + Query bleiben). */
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    return Response.redirect(new URL(url.pathname + url.search, env.TARGET).toString(), 301);
  },
};
