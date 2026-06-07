const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const packageJson = require("../package.json");
const packageLock = require("../package-lock.json");

test("package metadata targets BullMQ, Node.js 18+, and Node-RED 4.1.x", () => {
  assert.equal(packageJson.name, "@pauldeng/node-red-contrib-bullmq");
  assert.equal(packageJson.version, "1.0.0");
  assert.equal(packageJson.main, "bull-queue.js");
  assert.equal(packageJson.description, "BullMQ-backed Redis job queue nodes for Node-RED");
  assert.equal(packageJson.repository?.type, "git");
  assert.equal(
    packageJson.repository?.url,
    "git+https://github.com/pauldeng/node-red-contrib-bullmq.git"
  );
  assert.equal(
    packageJson.homepage,
    "https://github.com/pauldeng/node-red-contrib-bullmq#readme"
  );
  assert.equal(
    packageJson.bugs?.url,
    "https://github.com/pauldeng/node-red-contrib-bullmq/issues"
  );
  assert.equal(packageJson.publishConfig?.registry, "https://registry.npmjs.org/");
  assert.equal(packageJson.publishConfig?.access, "public");

  assert.equal(packageJson.dependencies?.bull, undefined);
  assert.equal(packageJson.dependencies?.["sprintf-js"], undefined);
  assert.equal(packageJson.dependencies?.bullmq, "5.78.0");
  assert.match(packageJson.dependencies?.ioredis || "", /^5\./);

  assert.equal(packageJson.engines?.node, ">=18");
  assert.equal(packageJson["node-red"]?.version, ">=4.1.0 <5");

  assert.equal(packageJson.devDependencies?.["@playwright/test"], "1.60.0");
  assert.equal(packageJson.devDependencies?.["node-red"], "4.1.11");
  assert.equal(packageJson.devDependencies?.prettier, "3.8.3");
  assert.ok(packageJson.devDependencies?.["node-red-node-test-helper"]);

  // Tests run on node:test; mocha and sinon are not used directly.
  assert.equal(packageJson.devDependencies?.mocha, undefined);
  assert.equal(packageJson.devDependencies?.sinon, undefined);
});

test("package lock root metadata mirrors publish package metadata", () => {
  const rootPackage = packageLock.packages?.[""];

  assert.equal(packageLock.name, packageJson.name);
  assert.equal(packageLock.version, packageJson.version);
  assert.equal(rootPackage?.name, packageJson.name);
  assert.equal(rootPackage?.version, packageJson.version);
  assert.equal(rootPackage?.engines?.node, packageJson.engines.node);
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

  for (const entry of [
    "bull-queue.js",
    "bull-queue.html",
    "lib/",
    "icons/",
    "examples/",
    "docs/*.md",
    "docs/logo.png",
  ]) {
    assert.ok(files.includes(entry), `files must include ${entry}`);
  }

  // Never publish tests, fixtures (incl. TLS keys), runner scripts, or local
  // editor/agent config.
  for (const entry of files) {
    assert.doesNotMatch(
      entry,
      /^(test|scripts|\.claude|\.github|docs\/superpowers)/,
      `files must not publish ${entry}`
    );
  }
});

test("GitHub automation and security assets exist for publishing", () => {
  for (const file of [
    ".github/workflows/ci.yml",
    ".github/workflows/publish.yml",
    ".github/dependabot.yml",
    ".github/pull_request_template.md",
    ".github/ISSUE_TEMPLATE/bug_report.yml",
    ".github/ISSUE_TEMPLATE/feature_request.yml",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
  ]) {
    assert.ok(fs.existsSync(path.join(__dirname, "..", file)), `${file} missing`);
  }
});
