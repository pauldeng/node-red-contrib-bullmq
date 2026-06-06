const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const { setImmediate: tick } = require("node:timers/promises");
const test = require("node:test");

const registerBullMQNodes = require("../bull-queue");

function createRED(options = {}) {
  const registered = new Map();
  return {
    registered,
    nodes: {
      createNode(node) {
        Object.setPrototypeOf(node, EventEmitter.prototype);
        EventEmitter.call(node);
        node.id = "node-under-test";
        node.status = options.status || (() => {});
        node.error = options.error || (() => {});
        node.send = () => {};
      },
      getNode() {
        return options.getNode ? options.getNode() : null;
      },
      registerType(type, constructor, options) {
        registered.set(type, { constructor, options });
      },
    },
  };
}

test("registers the legacy and BullMQ node types", () => {
  const RED = createRED();

  registerBullMQNodes(RED);

  assert.deepEqual(Array.from(RED.registered.keys()).sort(), [
    "bull cmd",
    "bull events",
    "bull flow",
    "bull job",
    "bull run",
    "bull-queue-server",
  ]);
});

test("config node declares credential-backed secret fields", () => {
  const RED = createRED();

  registerBullMQNodes(RED);

  const configNode = RED.registered.get("bull-queue-server");
  assert.deepEqual(Object.keys(configNode.options.credentials).sort(), [
    "password",
    "sentinelPassword",
    "tlsCa",
    "tlsCert",
    "tlsKey",
  ]);
});

test("bull flow reports Redis connection status when FlowProducer is ready", async () => {
  const statuses = [];
  let readyCalls = 0;
  const flowProducer = {
    async waitUntilReady() {
      readyCalls += 1;
    },
    async close() {},
    on() {},
  };
  const queueConfig = {
    config: { queueName: "flowcasts" },
    register(node) {
      node.status({ fill: "grey", shape: "ring", text: "configured" });
    },
    createFlowProducer() {
      return flowProducer;
    },
    deregister(node, done) {
      done();
    },
  };
  const RED = createRED({
    getNode() {
      return queueConfig;
    },
    status(status) {
      statuses.push(status);
    },
  });

  registerBullMQNodes(RED);
  const FlowNode = RED.registered.get("bull flow").constructor;
  const node = {};
  FlowNode.call(node, { queue: "queue" });

  await tick();

  assert.equal(readyCalls, 1);
  assert.deepEqual(statuses.at(-2), {
    fill: "yellow",
    shape: "ring",
    text: "connecting",
  });
  assert.deepEqual(statuses.at(-1), {
    fill: "green",
    shape: "dot",
    text: "connected",
  });
});

test("bull flow reports FlowProducer errors on its own node status", async () => {
  const statuses = [];
  const errors = [];
  const flowProducer = new EventEmitter();
  flowProducer.waitUntilReady = async function waitUntilReady() {};
  flowProducer.close = async function close() {};
  const queueConfig = {
    config: { queueName: "flowcasts" },
    register(node) {
      node.status({ fill: "grey", shape: "ring", text: "configured" });
    },
    createFlowProducer() {
      return flowProducer;
    },
    deregister(node, done) {
      done();
    },
  };
  const RED = createRED({
    getNode() {
      return queueConfig;
    },
    status(status) {
      statuses.push(status);
    },
    error(err) {
      errors.push(err);
    },
  });

  registerBullMQNodes(RED);
  const FlowNode = RED.registered.get("bull flow").constructor;
  FlowNode.call({}, { queue: "queue" });
  await tick();

  // Exactly one error listener: the flow node owns error reporting, and the
  // config node's createFlowProducer must not attach a duplicate listener.
  assert.equal(flowProducer.listenerCount("error"), 1);

  flowProducer.emit("error", new Error("connection lost"));

  assert.deepEqual(statuses.at(-1), {
    fill: "red",
    shape: "ring",
    text: "BullMQ flow: error",
  });
  assert.equal(errors.length, 1, "a single error must be reported once");
});

test("config node createFlowProducer does not attach its own error listener", async () => {
  const RED = createRED();
  registerBullMQNodes(RED);
  const Server = RED.registered.get("bull-queue-server").constructor;

  const node = {};
  Server.call(node, { name: "flowcasts" });
  // Avoid opening a real Redis connection.
  node.createConnection = function createConnection() {
    const connection = new EventEmitter();
    connection.options = {};
    return connection;
  };

  const flowProducer = node.createFlowProducer();
  try {
    // The flow node owns error reporting; the shared factory must not add a
    // second listener that would double-report flow errors.
    assert.equal(flowProducer.listenerCount("error"), 0);
  } finally {
    await flowProducer.close().catch(() => {});
  }
});
