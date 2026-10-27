/** Wartung während des Account-Umzugs: Schreibzugriffe abweisen (Scanner puffern und senden später nach). */
export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/health") return new Response("ok (Wartung)", { status: 200 });
    return new Response("Wartung: Umzug läuft, bitte in einer Stunde erneut versuchen.", {
      status: 503, headers: { "retry-after": "1800", "content-type": "text/plain; charset=utf-8" },
    });
  },
};
