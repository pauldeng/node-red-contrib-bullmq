"use strict";

const { EventEmitter, once } = require("node:events");

const {
  FlowProducer,
  Queue,
  QueueEvents,
  UnrecoverableError,
  Worker,
} = require("bullmq");
const IORedis = require("ioredis");

const { dispatchCommand } = require("./lib/commands");
const {
  buildBullMQOptions,
  buildRedisDescriptor,
  createRedisConnection,
  normalizeQueueConfig,
} = require("./lib/connections");
const { serializeFlowJob, serializeJob } = require("./lib/serialization");

const DEFAULT_EVENTS = [
  "active",
  "added",
  "cleaned",
  "completed",
  "deduplicated",
  "delayed",
  "drained",
  "duplicated",
  "failed",
  "paused",
  "progress",
  "removed",
  "resumed",
  "stalled",
  "waiting",
  "waiting-children",
];

async function closeResource(resource) {
  if (!resource) {
    return;
  }
  if (typeof resource.close === "function") {
    await resource.close();
    return;
  }
  if (typeof resource.quit === "function") {
    await resource.quit();
    return;
  }
  if (typeof resource.disconnect === "function") {
    resource.disconnect();
  }
}

function nodeSend(node, send, msg) {
  (send || node.send).call(node, msg);
}

function nodeDone(node, done, err, msg) {
  if (done) {
    done(err);
  } else if (err) {
    node.error(err, msg);
  }
}

function parsePositiveInteger(value, defaultValue) {
  if (value === undefined || value === null || value === "") {
    return defaultValue;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`Expected a positive integer, got ${value}`);
  }
  return parsed;
}

function parseEventFilter(value) {
  if (!value) {
    return DEFAULT_EVENTS;
  }
  return String(value)
    .split(/[\n,]+/)
    .map((event) => event.trim())
    .filter(Boolean);
}

function attachErrorListener(resource, node, label) {
  if (!resource || typeof resource.on !== "function") {
    return;
  }
  resource.on("error", (err) => {
    node.status({ fill: "red", shape: "ring", text: `${label}: error` });
    node.error(err);
  });
}

function createJobMessage(job, queueName, extraBull = {}) {
  const data = job.data || {};
  return {
    payload:
      data && Object.prototype.hasOwnProperty.call(data, "payload")
        ? data.payload
        : data,
    job: serializeJob(job),
    bull: {
      queue: queueName,
      jobId: job.id,
      ...extraBull,
    },
  };
}

class AcknowledgementEntry {
  constructor(context, timeoutMs) {
    this.events = new EventEmitter();
    this.settled = false;
    this.job = context.job;
    this.queue = context.queue;
    this.queueName = context.queueName;
    this.runNodeId = context.runNodeId;
    this.timeout = undefined;

    if (timeoutMs > 0) {
      this.timeout = setTimeout(() => {
        this.fail(
          new Error(`BullMQ job acknowledgement timed out after ${timeoutMs}ms`)
        );
      }, timeoutMs);
    }
  }

  async wait() {
    const [settlement] = await once(this.events, "settled");
    if (settlement.type === "reject") {
      throw settlement.error;
    }
    return settlement.value;
  }

  complete(value) {
    this.settle({ type: "resolve", value });
  }

  fail(error) {
    this.settle({ type: "reject", error });
  }

  settle(settlement) {
    if (this.settled) {
      return;
    }
    this.settled = true;
    clearTimeout(this.timeout);
    this.events.emit("settled", settlement);
  }
}

class AcknowledgementRegistry {
  constructor() {
    this.entries = new Map();
  }

  create(context, timeoutMs) {
    const ackId = `${context.runNodeId}:${context.job.id}:${Date.now()}:${Math.random()
      .toString(36)
      .slice(2)}`;

    const entry = new AcknowledgementEntry(context, timeoutMs);
    this.entries.set(ackId, entry);
    return { ackId, entry };
  }

  get(ackId) {
    const entry = this.entries.get(ackId);
    if (!entry) {
      throw new Error("Missing, stale, or already-settled BullMQ acknowledgement");
    }
    return entry;
  }

  rejectByRunNode(runNodeId, err) {
    for (const [ackId, entry] of this.entries.entries()) {
      if (entry.runNodeId === runNodeId) {
        this.entries.delete(ackId);
        entry.fail(err);
      }
    }
  }
}

module.exports = function registerBullMQNodes(RED) {
  const acknowledgements = new AcknowledgementRegistry();

  function BullQueueServerSetup(n) {
    RED.nodes.createNode(this, n);
    const node = this;

    node.users = {};
    node.resources = new Set();
    node.config = normalizeQueueConfig(n, node.credentials || {});
    node.queue = null;
    node.producerConnection = null;

    node.register = function register(bullNode) {
      node.users[bullNode.id] = bullNode;
      bullNode.status({
        fill: "grey",
        shape: "ring",
        text: "configured",
      });
    };

    node.deregister = function deregister(bullNode, done) {
      delete node.users[bullNode.id];
      done();
    };

    node.createConnection = function createConnection(role) {
      const descriptor = buildRedisDescriptor(node.config, role);
      const connection = createRedisConnection(descriptor, IORedis);
      attachErrorListener(connection, node, `Redis ${role}`);
      node.resources.add(connection);
      return connection;
    };

    node.getQueue = function getQueue() {
      if (!node.queue) {
        node.producerConnection = node.createConnection("producer");
        node.queue = new Queue(
          node.config.queueName,
          buildBullMQOptions(node.config, node.producerConnection)
        );
        attachErrorListener(node.queue, node, "BullMQ queue");
      }
      return node.queue;
    };

    node.createWorker = function createWorker(processor, options) {
      const connection = node.createConnection("worker");
      const worker = new Worker(node.config.queueName, processor, {
        ...buildBullMQOptions(node.config, connection),
        ...options,
      });
      attachErrorListener(worker, node, "BullMQ worker");
      node.resources.add(worker);
      return worker;
    };

    node.createQueueEvents = function createQueueEvents() {
      const connection = node.createConnection("events");
      const queueEvents = new QueueEvents(
        node.config.queueName,
        buildBullMQOptions(node.config, connection)
      );
      attachErrorListener(queueEvents, node, "BullMQ events");
      node.resources.add(queueEvents);
      return queueEvents;
    };

    node.createFlowProducer = function createFlowProducer() {
      const connection = node.createConnection("producer");
      const flowProducer = new FlowProducer(
        buildBullMQOptions(node.config, connection)
      );
      attachErrorListener(flowProducer, node, "BullMQ flow");
      node.resources.add(flowProducer);
      return flowProducer;
    };

    node.on("close", async function onClose(removed, done) {
      try {
        if (node.queue) {
          await closeResource(node.queue);
        }
        const resources = Array.from(node.resources).reverse();
        for (const resource of resources) {
          await closeResource(resource);
        }
        node.status({});
        done();
      } catch (err) {
        done(err);
      }
    });
  }

  RED.nodes.registerType("bull-queue-server", BullQueueServerSetup, {
    credentials: {
      password: { type: "password" },
      sentinelPassword: { type: "password" },
      tlsCa: { type: "password" },
      tlsCert: { type: "password" },
      tlsKey: { type: "password" },
    },
  });

  function BullQueueCmdNode(n) {
    RED.nodes.createNode(this, n);
    const node = this;
    node.queue = n.queue;
    node.bullConn = RED.nodes.getNode(node.queue);

    if (!node.bullConn) {
      node.status({ fill: "red", shape: "ring", text: "missing queue" });
      node.error("Missing bull-queue-server config node");
      return;
    }

    node.bullConn.register(node);
    node.status({ fill: "green", shape: "dot", text: "configured" });

    node.on("input", async function onInput(msg, send, done) {
      try {
        const result = await dispatchCommand(node.bullConn.getQueue(), msg);
        msg.payload = result;
        nodeSend(node, send, msg);
        nodeDone(node, done);
      } catch (err) {
        nodeDone(node, done, err, msg);
      }
    });

    node.on("close", function onClose(removed, done) {
      node.bullConn.deregister(node, done);
    });
  }

  function BullQueueRunNode(n) {
    RED.nodes.createNode(this, n);
    const node = this;
    node.queue = n.queue;
    node.bullQueue = RED.nodes.getNode(node.queue);
    node.completionMode = n.completionMode || "immediate";

    if (!node.bullQueue) {
      node.status({ fill: "red", shape: "ring", text: "missing queue" });
      node.error("Missing bull-queue-server config node");
      return;
    }

    node.bullQueue.register(node);

    const workerOptions = {
      concurrency: parsePositiveInteger(n.concurrency, 1),
    };
    if (n.limiterMax && n.limiterDuration) {
      workerOptions.limiter = {
        max: parsePositiveInteger(n.limiterMax),
        duration: parsePositiveInteger(n.limiterDuration),
      };
    }

    const processor = async (job) => {
      if (node.completionMode === "manual") {
        const timeoutMs = Number(n.ackTimeout || 300000);
        const acknowledgement = acknowledgements.create(
          {
            job,
            queue: node.bullQueue.getQueue(),
            queueName: node.bullQueue.config.queueName,
            runNodeId: node.id,
          },
          timeoutMs
        );
        node.send(
          createJobMessage(job, node.bullQueue.config.queueName, {
            ackId: acknowledgement.ackId,
            runNodeId: node.id,
          })
        );
        return await acknowledgement.entry.wait();
      }

      const msg = createJobMessage(job, node.bullQueue.config.queueName);
      node.send(msg);
      return msg.payload;
    };

    node.worker = node.bullQueue.createWorker(processor, workerOptions);
    node.worker.on("ready", () =>
      node.status({ fill: "green", shape: "dot", text: "connected" })
    );
    node.worker.on("closed", () =>
      node.status({ fill: "red", shape: "ring", text: "closed" })
    );
    node.status({ fill: "yellow", shape: "ring", text: "connecting" });

    node.on("close", async function onClose(removed, done) {
      acknowledgements.rejectByRunNode(
        node.id,
        new Error("BullMQ run node closed before acknowledgement")
      );
      try {
        await closeResource(node.worker);
        node.bullQueue.deregister(node, () => {});
        done();
      } catch (err) {
        done(err);
      }
    });
  }

  function BullJobNode(n) {
    RED.nodes.createNode(this, n);
    const node = this;
    node.action = n.action || "complete";

    node.on("input", async function onInput(msg, send, done) {
      try {
        const ackId = msg.bull && msg.bull.ackId;
        const context = acknowledgements.get(ackId);
        const action = msg.cmd || node.action;

        switch (action) {
          case "progress":
            await context.job.updateProgress(
              msg.progress !== undefined ? msg.progress : msg.payload
            );
            msg.payload = serializeJob(context.job);
            nodeSend(node, send, msg);
            nodeDone(node, done);
            return;
          case "removeDeduplicationKey":
            msg.payload = await context.job.removeDeduplicationKey();
            nodeSend(node, send, msg);
            nodeDone(node, done);
            return;
          case "getChildrenValues":
            msg.payload = await context.job.getChildrenValues();
            nodeSend(node, send, msg);
            nodeDone(node, done);
            return;
          case "getFailedChildrenValues":
            msg.payload = await context.job.getFailedChildrenValues();
            nodeSend(node, send, msg);
            nodeDone(node, done);
            return;
          case "removeUnprocessedChildren":
            msg.payload = await context.job.removeUnprocessedChildren();
            nodeSend(node, send, msg);
            nodeDone(node, done);
            return;
          case "complete": {
            const result = msg.result !== undefined ? msg.result : msg.payload;
            context.complete(result);
            msg.payload = result;
            nodeSend(node, send, msg);
            nodeDone(node, done);
            return;
          }
          case "fail": {
            const error =
              msg.error instanceof Error
                ? msg.error
                : new Error(String(msg.error || msg.payload));
            context.fail(error);
            nodeDone(node, done);
            return;
          }
          case "failUnrecoverable": {
            const errorText = String(
              msg.error || msg.payload || "Unrecoverable BullMQ job failure"
            );
            context.fail(new UnrecoverableError(errorText));
            nodeDone(node, done);
            return;
          }
          case "rateLimit":
            await context.queue.rateLimit(msg.duration);
            context.fail(Worker.RateLimitError());
            nodeDone(node, done);
            return;
          default:
            throw new Error(`Unsupported bull job action: ${action}`);
        }
      } catch (err) {
        nodeDone(node, done, err, msg);
      }
    });
  }

  function BullEventsNode(n) {
    RED.nodes.createNode(this, n);
    const node = this;
    node.queue = n.queue;
    node.bullConn = RED.nodes.getNode(node.queue);

    if (!node.bullConn) {
      node.status({ fill: "red", shape: "ring", text: "missing queue" });
      node.error("Missing bull-queue-server config node");
      return;
    }

    node.bullConn.register(node);
    node.queueEvents = node.bullConn.createQueueEvents();
    const events = parseEventFilter(n.events);
    for (const event of events) {
      node.queueEvents.on(event, (payload, eventId) => {
        node.send({
          topic: event,
          payload,
          bull: {
            queue: node.bullConn.config.queueName,
            event,
            eventId,
          },
        });
      });
    }
    async function updateReadyStatus() {
      try {
        await node.queueEvents.waitUntilReady();
        node.status({ fill: "green", shape: "dot", text: "connected" });
      } catch (err) {
        node.error(err);
      }
    }
    updateReadyStatus();

    node.on("close", async function onClose(removed, done) {
      try {
        await closeResource(node.queueEvents);
        node.bullConn.deregister(node, () => {});
        done();
      } catch (err) {
        done(err);
      }
    });
  }

  function BullFlowNode(n) {
    RED.nodes.createNode(this, n);
    const node = this;
    node.queue = n.queue;
    node.bullConn = RED.nodes.getNode(node.queue);

    if (!node.bullConn) {
      node.status({ fill: "red", shape: "ring", text: "missing queue" });
      node.error("Missing bull-queue-server config node");
      return;
    }

    node.bullConn.register(node);
    node.flowProducer = node.bullConn.createFlowProducer();

    node.on("input", async function onInput(msg, send, done) {
      try {
        if (!msg.payload || typeof msg.payload !== "object") {
          throw new Error("bull flow requires msg.payload to contain a flow tree");
        }
        msg.payload = serializeFlowJob(
          await node.flowProducer.add(msg.payload, msg.flowopts)
        );
        nodeSend(node, send, msg);
        nodeDone(node, done);
      } catch (err) {
        nodeDone(node, done, err, msg);
      }
    });

    node.on("close", async function onClose(removed, done) {
      try {
        await closeResource(node.flowProducer);
        node.bullConn.deregister(node, () => {});
        done();
      } catch (err) {
        done(err);
      }
    });
  }

  RED.nodes.registerType("bull cmd", BullQueueCmdNode);
  RED.nodes.registerType("bull run", BullQueueRunNode);
  RED.nodes.registerType("bull job", BullJobNode);
  RED.nodes.registerType("bull events", BullEventsNode);
  RED.nodes.registerType("bull flow", BullFlowNode);
};
