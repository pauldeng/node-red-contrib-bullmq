const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const html = fs.readFileSync(
  path.join(__dirname, "..", "bull-queue.html"),
  "utf8"
);

test("editor defines templates and registrations for all node types", () => {
  for (const type of [
    "bull-queue-server",
    "bull run",
    "bull cmd",
    "bull job",
    "bull events",
    "bull flow",
  ]) {
    assert.match(html, new RegExp(`data-template-name="${type}"`));
    assert.match(html, new RegExp(`registerType\\("${type}"`));
  }
});

test("config editor exposes deployment, cluster, sentinel, auth, and TLS fields", () => {
  for (const field of [
    "deployment",
    "clusterNodes",
    "sentinels",
    "sentinelMasterName",
    "username",
    "password",
    "sentinelUsername",
    "sentinelPassword",
    "tls",
    "sentinelTls",
    "tlsRejectUnauthorized",
    "tlsServerName",
    "prefix",
  ]) {
    assert.match(html, new RegExp(`node-config-input-${field}`));
  }
});

test("worker, job, events, and flow editors expose their stable config fields", () => {
  for (const field of [
    "completionMode",
    "ackTimeout",
    "concurrency",
    "limiterMax",
    "limiterDuration",
    "action",
    "events",
  ]) {
    assert.match(html, new RegExp(`node-input-${field}`));
  }
});
