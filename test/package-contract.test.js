const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const packageJson = require("../package.json");

test("package metadata targets BullMQ, Node.js 24+, and Node-RED 4.1.x", () => {
  assert.equal(packageJson.dependencies?.bull, undefined);
  assert.equal(packageJson.dependencies?.["sprintf-js"], undefined);
  assert.equal(packageJson.dependencies?.bullmq, "5.78.0");
  assert.match(packageJson.dependencies?.ioredis || "", /^5\./);

  assert.equal(packageJson.engines?.node, ">=24");
  assert.equal(packageJson["node-red"]?.version, ">=4.1.0 <5");

  assert.equal(packageJson.devDependencies?.["@playwright/test"], "1.60.0");
  assert.equal(packageJson.devDependencies?.["node-red"], "4.1.11");
  assert.equal(packageJson.devDependencies?.prettier, "3.8.3");
  assert.ok(packageJson.devDependencies?.["node-red-node-test-helper"]);

  // Tests run on node:test; mocha and sinon are not used directly.
  assert.equal(packageJson.devDependencies?.mocha, undefined);
  assert.equal(packageJson.devDependencies?.sinon, undefined);
});

test("runtime code no longer imports bull or sprintf-js", () => {
  const runtime = fs.readFileSync(
    path.join(__dirname, "..", "bull-queue.js"),
    "utf8"
  );

  assert.doesNotMatch(runtime, /require\(["']bull["']\)/);
  assert.doesNotMatch(runtime, /require\(["']sprintf-js["']\)/);
});

test("published files allowlist ships runtime assets and excludes tests and secrets", () => {
  const files = packageJson.files;
  assert.ok(Array.isArray(files), "package.json must declare a files allowlist");

  for (const entry of ["bull-queue.js", "bull-queue.html", "lib/", "icons/"]) {
    assert.ok(files.includes(entry), `files must include ${entry}`);
  }

  // Never publish tests, fixtures (incl. TLS keys), runner scripts, or local
  // editor/agent config.
  for (const entry of files) {
    assert.doesNotMatch(
      entry,
      /^(test|scripts|\.claude|docs\/superpowers)/,
      `files must not publish ${entry}`
    );
  }
});
