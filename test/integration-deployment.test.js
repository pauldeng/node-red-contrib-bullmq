const assert = require("node:assert/strict");
const { once } = require("node:events");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const helper = require("node-red-node-test-helper");
const bullNodes = require("../bull-queue");

const enabled = process.env.BULLMQ_EXTERNAL_REDIS === "1";

function envBoolean(name, defaultValue = false) {
  const value = process.env[name];
  if (value === undefined || value === "") {
    return defaultValue;
  }
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function queueName() {
  const deployment = process.env.BULLMQ_DEPLOYMENT_NAME || "external";
  return `basecasts-${deployment}-${process.pid}-${Date.now()}`;
}

function externalQueueNode(name) {
  const mode = process.env.BULLMQ_REDIS_MODE || "single";
  return {
    id: "queue",
    type: "bull-queue-server",
    name,
    deployment: mode,
    address: process.env.BULLMQ_HOST || "127.0.0.1",
    port: process.env.BULLMQ_PORT || "6379",
    clusterNodes: process.env.BULLMQ_CLUSTER_NODES || "",
    sentinels: process.env.BULLMQ_SENTINELS || "",
    sentinelMasterName: process.env.BULLMQ_SENTINEL_MASTER_NAME || "",
    username: process.env.BULLMQ_USERNAME || "",
    password: process.env.BULLMQ_PASSWORD || "",
    sentinelUsername: process.env.BULLMQ_SENTINEL_USERNAME || "",
    sentinelPassword: process.env.BULLMQ_SENTINEL_PASSWORD || "",
    tls: envBoolean("BULLMQ_TLS", false),
    sentinelTls: envBoolean("BULLMQ_SENTINEL_TLS", false),
    tlsRejectUnauthorized: envBoolean("BULLMQ_TLS_REJECT_UNAUTHORIZED", true),
    prefix: process.env.BULLMQ_QUEUE_PREFIX || "",
  };
}

async function waitForInput(node, timeoutMs = 10000) {
  const [msg] = await once(node, "input", {
    signal: AbortSignal.timeout(timeoutMs),
  });
  return msg;
}

async function startHelper() {
  const userDir = fs.mkdtempSync(path.join(os.tmpdir(), "nr-bullmq-deploy-"));
  helper.init(require.resolve("node-red"), {
    userDir,
    flowFile: "flows.json",
    credentialSecret: false,
    logging: { console: { level: "fatal" } },
  });
  await helper.startServer();
  return userDir;
}

async function stopHelper(userDir) {
  await helper.unload();
  await helper.stopServer();
  fs.rmSync(userDir, { recursive: true, force: true });
}

test(
  "external Redis deployment adds, runs, and manages the required scheduler",
  { skip: !enabled },
  async () => {
    const name = queueName();
    const userDir = await startHelper();

    try {
      const flow = [
        { id: "tab", type: "tab", label: "deployment" },
        externalQueueNode(name),
        {
          id: "cmd",
          type: "bull cmd",
          z: "tab",
          name: "cmd",
          queue: "queue",
          x: 360,
          y: 120,
          wires: [["cmd-out"]],
        },
        { id: "cmd-out", type: "helper", z: "tab", x: 560, y: 120, wires: [] },
        {
          id: "run",
          type: "bull run",
          z: "tab",
          name: "run",
          queue: "queue",
          completionMode: "immediate",
          ackTimeout: 300000,
          concurrency: 1,
          x: 360,
          y: 220,
          wires: [["run-out"]],
        },
        { id: "run-out", type: "helper", z: "tab", x: 560, y: 220, wires: [] },
      ];

      await helper.load(bullNodes, flow);
      const cmd = helper.getNode("cmd");
      const cmdOut = helper.getNode("cmd-out");
      const runOut = helper.getNode("run-out");

      const addOutput = waitForInput(cmdOut);
      const runOutput = waitForInput(runOut);
      cmd.receive({
        cmd: "add",
        payload: "docker deployment payload",
        jobopts: { removeOnComplete: true },
      });

      assert.equal((await addOutput).payload.name, "default");
      assert.equal((await runOutput).payload, "docker deployment payload");

      const scheduleOutput = waitForInput(cmdOut);
      cmd.receive({
        cmd: "add",
        payload: "gateway-FCC23DFFFE0AA2A8",
        jobopts: {
          jobId: "gateway-FCC23DFFFE0AA2A8",
          repeat: { cron: "30 9,19,29,39,49,59 * * * *" },
          removeOnComplete: true,
        },
      });
      assert.equal((await scheduleOutput).payload.name, "default");

      const getOutput = waitForInput(cmdOut);
      cmd.receive({
        cmd: "getRepeatableJobByKey",
        jobid: "gateway-FCC23DFFFE0AA2A8",
      });
      const scheduler = (await getOutput).payload;
      assert.equal(scheduler.key, "gateway-FCC23DFFFE0AA2A8");
      if (scheduler.next) {
        const next = new Date(scheduler.next);
        assert.equal(next.getSeconds(), 30);
        assert.ok([9, 19, 29, 39, 49, 59].includes(next.getMinutes()));
      }

      const removeOutput = waitForInput(cmdOut);
      cmd.receive({
        cmd: "removeRepeatableByKey",
        jobid: "gateway-FCC23DFFFE0AA2A8",
      });
      assert.equal((await removeOutput).payload, true);
    } finally {
      await stopHelper(userDir);
    }
  }
);
