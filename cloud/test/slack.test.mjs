import test from "node:test";
import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { verifySlack, chatMessageFromEvent } from "../src/slack.js";

const sign = (secret, ts, body) => "v0=" + createHmac("sha256", secret).update(`v0:${ts}:${body}`).digest("hex");

test("Slack-Signatur: gültig, falsch, zu alt", async () => {
  const body = '{"type":"event_callback"}', ts = "1700000000";
  const h = (sig, t = ts) => new Headers({ "x-slack-request-timestamp": t, "x-slack-signature": sig });
  assert.equal(await verifySlack(h(sign("geheim", ts, body)), body, "geheim", 1700000010), true);
  assert.equal(await verifySlack(h(sign("anders", ts, body)), body, "geheim", 1700000010), false);
  assert.equal(await verifySlack(h(sign("geheim", ts, body)), body, "geheim", 1700001000), false);
  assert.equal(await verifySlack(h(sign("geheim", ts, body)), body, "", 1700000010), false);
});

test("Slack-Events: nur Menschen im freigegebenen Kanal", () => {
  const msg = { type: "message", channel_type: "channel", user: "U1", channel: "C1", ts: "1.2", text: "Stand?" };
  assert.deepEqual(chatMessageFromEvent(msg, "C1"), { user: "U1", channel: "C1", ts: "1.2", thread: null, text: "Stand?" });
  assert.equal(chatMessageFromEvent(msg, "C2"), null);
  assert.equal(chatMessageFromEvent(msg, undefined), null);
  assert.equal(chatMessageFromEvent({ ...msg, channel: "D1", channel_type: "im" }, "C1"), null);
  assert.equal(chatMessageFromEvent({ ...msg, bot_id: "B1" }, "C1"), null);
  assert.equal(chatMessageFromEvent({ ...msg, subtype: "message_changed" }, "C1"), null);
  const m = chatMessageFromEvent({ type: "app_mention", user: "U1", channel: "C1", ts: "3.4", text: "<@U9> Zwischenstand" }, "C1");
  assert.equal(m.text, "Zwischenstand");
});
