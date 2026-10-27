import { test } from "node:test";
import assert from "node:assert/strict";
import proxy from "../src/proxy.js";

test("Proxy reicht Methode, Pfad, Query, Header und Body unverändert weiter", async () => {
  let seen;
  globalThis.fetch = async (url, init) => { seen = { url: String(url), ...init, body: await new Response(init.body).text() }; return new Response("ok"); };
  const req = new Request("https://alt.workers.dev/ingest/files?x=1", { method: "POST", headers: { authorization: "Bearer T" }, body: '{"a":1}' });
  await proxy.fetch(req, { TARGET: "https://neu.workers.dev" });
  assert.equal(seen.url, "https://neu.workers.dev/ingest/files?x=1");
  assert.equal(seen.method, "POST");
  assert.equal(seen.headers.get("authorization"), "Bearer T");
  assert.equal(seen.body, '{"a":1}');
  assert.equal(seen.redirect, "manual");
});
