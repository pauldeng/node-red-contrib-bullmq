const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const repoRoot = path.join(__dirname, "..");

function read(relativePath) {
  return fs.readFileSync(path.join(repoRoot, relativePath), "utf8");
}

test("agent and user documentation files exist", () => {
  for (const file of [
    "CLAUDE.md",
    "AGENTS.md",
    "docs/REFERENCE_MAP.md",
    "docs/ARCHITECTURE.md",
    "docs/NODE_GUIDE.md",
    "docs/CHANGE_WORKFLOW.md",
    "docs/TESTING.md",
    "docs/TROUBLESHOOTING.md",
    "docs/MIGRATION.md",
    "docs/COMMANDS.md",
    "docs/CONNECTIONS.md",
  ]) {
    assert.ok(fs.existsSync(path.join(repoRoot, file)), `${file} missing`);
  }
});

test("README documents BullMQ migration, supported deployments, and unsupported features", () => {
  const readme = read("README.md");
  for (const text of [
    "BullMQ 5.78.0",
    "Node-RED 4.1",
    "Node.js 24",
    "Redis Cluster",
    "AWS MemoryDB",
    "Sentinel",
    "Unsupported",
    "maxmemory-policy=noeviction",
    "Bull v4 Redis data is not automatically migrated",
  ]) {
    assert.match(readme, new RegExp(text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  }
});

test("examples include the required basecasts scheduled job flow", () => {
  const example = read("examples/example_flow.json");
  assert.match(example, /"name"\s*:\s*"basecasts"/);
  assert.match(example, /gateway-FCC23DFFFE0AA2A8/);
  assert.match(example, /30 9,19,29,39,49,59 \* \* \* \*/);
  assert.match(example, /"type"\s*:\s*"bull events"/);
  assert.match(example, /"type"\s*:\s*"bull flow"/);
});

test("examples include simple BullMQ feature import flows", () => {
  const fileText = read("examples/bullmq_features.json");
  const example = JSON.parse(fileText);
  const readme = read("examples/README.md");
  const nodeText = JSON.stringify(example);
  const functionText = example
    .filter((node) => node.type === "function")
    .map((node) => node.func)
    .join("\n");
  const searchableText = `${nodeText}\n${functionText}`;

  for (const label of [
    "delay: send later",
    "priority: high priority",
    "dedupe: same job once",
    "rate limit: 2 per second",
    "scheduler: every minute",
    "manual ack worker",
    "flow: parent plus child",
  ]) {
    assert.match(nodeText, new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
    assert.match(readme, new RegExp(label.split(":")[0], "i"));
  }

  for (const text of [
    "delay: 10000",
    "priority: 1",
    "deduplication: { id: msg.payload }",
    "msg.cmd = \"setGlobalRateLimit\"",
    "repeat: { pattern: \"*/1 * * * *\" }",
    "\"bull events\"",
    "\"bull job\"",
    "\"bull flow\"",
  ]) {
    assert.match(searchableText, new RegExp(text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  }
});

test("testing docs describe the executable Docker deployment matrix", () => {
  const testing = read("docs/TESTING.md");
  for (const text of [
    "npm run test:deployments",
    "single-noauth",
    "single-auth",
    "single-tls",
    "cluster-auth",
    "cluster-tls",
    "sentinel-auth",
    "sentinel-tls",
    "MEMORYDB_ENABLED=1",
  ]) {
    assert.match(testing, new RegExp(text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  }
});

test("repository text does not contain MemoryDB secret assignments", () => {
  const forbiddenPatterns = [
    /MEMORYDB_PASSWORD\s*=\s*["'][^"']+["']/,
    /MEMORYDB_USERNAME\s*=\s*["'][^"']+["']/,
    /clustercfg\.memdb\.bchgcd\.memorydb\.ap-southeast-2\.amazonaws\.com/,
  ];
  const files = [
    "README.md",
    "CLAUDE.md",
    "AGENTS.md",
    "docs/REFERENCE_MAP.md",
    "docs/ARCHITECTURE.md",
    "docs/NODE_GUIDE.md",
    "docs/CHANGE_WORKFLOW.md",
    "docs/TESTING.md",
    "docs/TROUBLESHOOTING.md",
    "docs/MIGRATION.md",
    "docs/COMMANDS.md",
    "docs/CONNECTIONS.md",
    "examples/example_flow.json",
    "package.json",
  ];

  const allText = files
    .filter((file) => fs.existsSync(path.join(repoRoot, file)))
    .map(read)
    .join("\n");
  for (const pattern of forbiddenPatterns) {
    assert.doesNotMatch(allText, pattern);
  }
});
