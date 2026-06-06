"use strict";

const { EventEmitter, once } = require("node:events");

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
    // Drop the entry as soon as it settles (complete, fail, timeout, or
    // rejectByRunNode) so the registry never accumulates finished jobs.
    entry.events.once("settled", () => {
      this.entries.delete(ackId);
    });
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
    // Snapshot first: failing an entry settles it, which removes it from the
    // map via the "settled" handler registered in create().
    for (const entry of Array.from(this.entries.values())) {
      if (entry.runNodeId === runNodeId) {
        entry.fail(err);
      }
    }
  }
}

module.exports = {
  AcknowledgementEntry,
  AcknowledgementRegistry,
};
