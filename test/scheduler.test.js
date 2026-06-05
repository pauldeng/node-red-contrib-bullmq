const assert = require("node:assert/strict");
const test = require("node:test");

const {
  getLegacySchedulerId,
  normalizeAddRequest,
  serializeScheduler,
} = require("../lib/scheduler");

test("translates Bull repeat.cron into a BullMQ Job Scheduler template", () => {
  const normalized = normalizeAddRequest({
    payload: "gateway-FCC23DFFFE0AA2A8",
    cmd: "add",
    jobopts: {
      jobId: "gateway-FCC23DFFFE0AA2A8",
      removeOnComplete: true,
      repeat: {
        cron: "30 9,19,29,39,49,59 * * * *",
      },
    },
  });

  assert.equal(normalized.kind, "scheduler");
  assert.equal(normalized.schedulerId, "gateway-FCC23DFFFE0AA2A8");
  assert.deepEqual(normalized.repeat, {
    pattern: "30 9,19,29,39,49,59 * * * *",
  });
  assert.deepEqual(normalized.template, {
    name: "default",
    data: { payload: "gateway-FCC23DFFFE0AA2A8" },
    opts: { removeOnComplete: true },
  });
});

test("accepts explicit schedulerId and repeat.pattern", () => {
  const normalized = normalizeAddRequest({
    payload: { value: 1 },
    schedulerId: "sensor-reading",
    jobName: "sample",
    jobData: { sample: true },
    jobopts: {
      repeat: {
        pattern: "*/10 * * * * *",
      },
    },
  });

  assert.equal(normalized.kind, "scheduler");
  assert.equal(normalized.schedulerId, "sensor-reading");
  assert.deepEqual(normalized.repeat, { pattern: "*/10 * * * * *" });
  assert.deepEqual(normalized.template, {
    name: "sample",
    data: { sample: true },
    opts: {},
  });
});

test("rejects conflicting repeat cron and pattern values", () => {
  assert.throws(
    () =>
      normalizeAddRequest({
        schedulerId: "conflict",
        jobopts: {
          repeat: {
            cron: "*/5 * * * * *",
            pattern: "*/10 * * * * *",
          },
        },
      }),
    /repeat\.cron and repeat\.pattern must match/
  );
});

test("requires a stable scheduler id for scheduled jobs", () => {
  assert.throws(
    () =>
      normalizeAddRequest({
        payload: "missing-id",
        jobopts: {
          repeat: { cron: "*/5 * * * * *" },
        },
      }),
    /scheduled jobs require msg\.schedulerId or msg\.jobopts\.jobId/
  );
});

test("normalizes normal add requests without repeat options", () => {
  const normalized = normalizeAddRequest({
    payload: "plain",
    jobopts: { jobId: "plain", priority: 2 },
  });

  assert.equal(normalized.kind, "job");
  assert.equal(normalized.name, "default");
  assert.deepEqual(normalized.data, { payload: "plain" });
  assert.deepEqual(normalized.opts, { jobId: "plain", priority: 2 });
});

test("legacy scheduler lookup uses exact ids only", () => {
  assert.equal(getLegacySchedulerId({ schedulerId: "exact" }), "exact");
  assert.equal(getLegacySchedulerId({ jobid: "legacy" }), "legacy");
  assert.throws(() => getLegacySchedulerId({}), /scheduler id/);
});

test("serializes scheduler metadata without live BullMQ objects", () => {
  assert.deepEqual(
    serializeScheduler({
      id: "gateway",
      key: "repeat:gateway",
      name: "default",
      next: 1760000000000,
      pattern: "*/10 * * * * *",
    }),
    {
      id: "gateway",
      key: "repeat:gateway",
      name: "default",
      next: 1760000000000,
      pattern: "*/10 * * * * *",
    }
  );
});
