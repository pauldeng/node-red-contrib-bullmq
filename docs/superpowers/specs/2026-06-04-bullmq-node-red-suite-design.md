# BullMQ Node-RED Suite Migration Design

**Status:** Approved

**Date:** 2026-06-04

## Summary

`node-red-contrib-bull` will migrate from Bull 4.5.4 to BullMQ 5.78.0 and become a
Node-RED-native suite for the open-source BullMQ feature set. The migration will preserve the
existing `bull-queue-server`, `bull cmd`, and `bull run` node types and the existing message-driven
command style, including the required `repeat.cron` flow. It will add focused nodes for active-job
acknowledgement, global events, and parent/child flows.

The package will support Node.js 24 and newer and Node-RED 4.1.x. It will support standalone Redis,
Redis Cluster, AWS MemoryDB, and Redis Sentinel, with or without authentication and with or without
TLS. Runtime behavior, editor behavior, deployment compatibility, and timing-sensitive queue
features will be covered by automated tests.

## Goals

1. Replace `bull` with exactly `bullmq` 5.78.0.
2. Remove `sprintf-js` and use native JavaScript string formatting.
3. Support Node.js `>=24` and Node-RED `>=4.1.0 <5`.
4. Preserve existing Node-RED flows wherever BullMQ permits:
   - keep `bull-queue-server`, `bull cmd`, and `bull run`;
   - keep `msg.cmd` command dispatch;
   - keep `msg.payload` as the compatibility input and output property;
   - accept Bull-style `msg.jobopts.repeat.cron`.
5. Use BullMQ Job Schedulers rather than deprecated repeatable-job APIs.
6. Expose open-source BullMQ features that have clear Node-RED message-flow semantics.
7. Add reliable downstream acknowledgement so Node-RED flow success or failure can determine the
   BullMQ job result.
8. Support standalone, cluster, MemoryDB, and Sentinel Redis deployments with optional ACL auth and
   TLS.
9. Add comprehensive runtime, browser, Docker deployment, and optional MemoryDB tests.
10. Add examples, user documentation, editor help, and agent-maintenance documentation.

## Non-Goals

The following BullMQ capabilities will not be implemented as Node-RED nodes:

- **Sandboxed processors:** an external processor JavaScript file bypasses the Node-RED flow and its
  downstream acknowledgement model.
- **Custom JavaScript backoff strategies:** executable worker configuration is not a safe or
  discoverable Node-RED message contract. BullMQ's built-in fixed and exponential strategies remain
  supported through `msg.jobopts.backoff`.
- **BullMQ Pro features:** observables, groups, group rate limits, and batches are not part of the
  open-source BullMQ 5.78.0 dependency.
- **Built-in queue dashboard:** a Node-RED node package is not a replacement for a dedicated queue
  administration UI.
- **Arbitrary BullMQ method proxying:** exposing unrestricted method names and argument arrays would
  be difficult to validate, document, secure, and test.
- **Automatic migration of queued Bull v4 Redis data:** Bull and BullMQ do not share a supported
  queue-data migration contract. Users must drain or otherwise retire Bull v4 queues before
  upgrading. Flow definitions remain compatible where described in this document.

Atomic operations and persistence are BullMQ and Redis guarantees. They will be documented and
verified through integration behavior, not represented as palette nodes.

## Evidence And Design Reasons

The design is based on the current BullMQ, Node-RED, ioredis, and AWS documentation:

- BullMQ's feature comparison lists parent/child dependencies, deduplication, priorities,
  concurrency, delayed jobs, global events, rate limiting, pause/resume, sandboxed workers,
  repeatable jobs, atomic operations, and persistence:
  <https://github.com/taskforcesh/bullmq#feature-comparison>
- BullMQ 5.78.0 is the required release and its API reference is authoritative:
  <https://api.docs.bullmq.io/>
- Job Schedulers replace deprecated repeatable-job APIs:
  <https://docs.bullmq.io/guide/job-schedulers>
- BullMQ classes have different Redis connection requirements, especially Workers and
  `maxRetriesPerRequest`:
  <https://docs.bullmq.io/guide/connections>
- BullMQ recommends Redis `maxmemory-policy=noeviction`:
  <https://docs.bullmq.io/guide/going-to-production>
- `QueueEvents` provides global events using a dedicated Redis connection:
  <https://docs.bullmq.io/guide/events>
- `FlowProducer` atomically adds parent/child job trees:
  <https://docs.bullmq.io/guide/flows>
- ioredis documents standalone, Cluster, Sentinel, ACL, and TLS support:
  <https://github.com/redis/ioredis>
- Node-RED nodes should catch asynchronous errors and provide predictable message behavior:
  <https://nodered.org/docs/creating-nodes/>

The `claude-review` branch of `node-red-contrib-redis` is the project-specific reference for Docker
deployment testing, Node-RED test-helper use, Playwright editor testing, documentation structure,
and shared agent guidance:
<https://github.com/pauldeng/node-red-contrib-redis/tree/claude-review>

## Package And Compatibility Contract

### Package Metadata

`package.json` will declare:

- `engines.node`: `>=24`
- `node-red.version`: `>=4.1.0 <5`
- runtime dependencies:
  - `bullmq`: `5.78.0`
  - `ioredis`: current compatible 5.x release
- development dependencies including:
  - Node-RED 4.1.x
  - `node-red-node-test-helper`
  - Mocha
  - Playwright
  - Prettier

`bull`, `sprintf-js`, obsolete Husky configuration, and obsolete development dependencies will be
removed.

### Public Compatibility

Existing node types remain registered:

- `bull-queue-server`
- `bull cmd`
- `bull run`

Existing commands remain available where BullMQ has an equivalent:

- `add`
- `count`
- `getRepeatableJobs`
- `getRepeatableJobByKey`
- `removeRepeatableByKey`
- `stopAndRemoveAllJobs`
- `clean`

Compatibility commands for repeatable jobs will use BullMQ Job Scheduler APIs internally. They will
match scheduler IDs exactly, never by substring, to avoid removing the wrong scheduler.

The migration will accept:

```js
msg.payload = "gateway-FCC23DFFFE0AA2A8";
msg.cmd = "add";
msg.jobopts = {
  jobId: msg.payload,
  repeat: {
    cron: "30 9,19,29,39,49,59 * * * *",
  },
};
```

For scheduled jobs:

- `msg.schedulerId` is preferred when supplied.
- `msg.jobopts.jobId` is accepted as the compatibility scheduler ID.
- `repeat.cron` is translated to `repeat.pattern`.
- supplying both `cron` and `pattern` with different values is an error.
- the scheduler ID is removed from the generated job template options because BullMQ Job
  Schedulers generate their own job IDs.
- a scheduled job without a non-empty scheduler ID is rejected with a clear error.

This explicit ID requirement makes scheduler updates, lookup, and removal deterministic.

### Job Data Compatibility

For `add`:

- when `msg.jobData` is present, it is used as the full BullMQ job data;
- otherwise the job data is `{ payload: msg.payload }`, preserving current behavior;
- `msg.jobName` is the BullMQ job name and defaults to `"default"`;
- `msg.jobopts` is passed through after compatibility normalization and validation.

For `bull run` output:

- `msg.payload` is `job.data.payload` when that property exists;
- otherwise `msg.payload` is the full `job.data`;
- `msg.job` is a serializable metadata object, not a live BullMQ `Job` instance.

## Runtime Architecture

The package will remain one Node-RED module entry point, but the expanded runtime will be split into
focused internal modules so connection handling, command dispatch, acknowledgement, and
serialization can be tested independently.

Expected responsibilities:

- `bull-queue.js`: Node-RED module entry point and node registration.
- connection helper: normalize config, build role-specific Redis/BullMQ connection options, create
  Cluster connections, and attach lifecycle status.
- command helper: validate and dispatch `bull cmd` commands.
- scheduler helper: normalize Bull repeat options and map compatibility commands to Job Scheduler
  APIs.
- acknowledgement registry: hold live active jobs and their pending processor promises without
  exposing lock tokens in Node-RED messages.
- serialization helper: convert BullMQ Jobs, schedulers, flows, and events into predictable
  message-safe objects.
- node implementations for config, command, worker, active-job action, events, and flows.

The exact file split may be adjusted during implementation if tests show a smaller boundary is
clearer, but responsibilities must remain isolated.

## Node Types

### `bull-queue-server`

The config node owns the queue name, Redis deployment configuration, credentials, and the shared
BullMQ key prefix. It creates and closes the producer `Queue` used by `bull cmd`.

It provides role-specific connection construction for:

- producer Queue calls, which should fail within a bounded time when Redis is unavailable;
- Workers, which require persistent retry behavior and `maxRetriesPerRequest: null`;
- QueueEvents, which require a dedicated blocking connection;
- FlowProducer instances;
- tests and connection-status observation.

The config node will track users and connections, update user node status, and close resources only
when no longer needed.

### `bull cmd`

`bull cmd` remains the message-driven producer and queue-administration node. It has one input and
one output.

Successful commands:

- place the command result in `msg.payload`;
- call `send(msg)`;
- call `done()` when available.

Failed commands:

- call `done(err)` when available;
- otherwise call `node.error(err, msg)`;
- do not send a success message.

### `bull run`

`bull run` creates a BullMQ Worker for the configured queue. It has no input and one output.

Configurable worker fields:

- local concurrency, default `1`;
- worker limiter `max` and `duration`, optional;
- completion mode: `immediate` or `manual`;
- acknowledgement timeout for manual mode;
- normal BullMQ worker lifecycle settings that are useful and safe for Node-RED, limited to fields
  documented in editor help.

The Worker always has an `error` listener. Status reflects connecting, connected, reconnecting,
disconnected, and error states.

### `bull job`

`bull job` acts on the active job emitted by a manual-mode `bull run` node. It has one input and one
output. A configured action may be overridden by `msg.cmd`.

It never accepts a raw BullMQ lock token. It resolves an opaque acknowledgement ID from `msg.bull`
through the in-process acknowledgement registry.

### `bull events`

`bull events` creates a BullMQ `QueueEvents` instance and emits global queue events as Node-RED
messages. It has no input and one output.

It supports an optional event filter. Without a filter it subscribes to the documented supported
event set. It uses a dedicated Redis connection and closes it on redeploy or shutdown.

### `bull flow`

`bull flow` creates atomic parent/child job trees with `FlowProducer`. It has one input and one
output.

- `msg.payload` contains a BullMQ-compatible flow tree.
- `msg.flowopts` contains optional FlowProducer options.
- every job in the tree must use a non-empty `queueName`.
- all queues in one flow use the same Redis deployment and shared BullMQ prefix.
- the output payload is a serialized job tree.

## Redis Connection Design

### Deployment Modes

The config node supports:

1. `single`
2. `cluster`
3. `sentinel`

AWS MemoryDB uses `cluster` mode with TLS and ACL credentials.

Legacy flows without a deployment mode are interpreted as `single` using their existing `address`,
`port`, and password values.

### Common Redis Options

The config node supports:

- host and port;
- Redis database index where supported;
- optional ACL username;
- optional Redis password;
- optional TLS;
- optional TLS CA certificate;
- optional TLS client certificate and private key;
- optional TLS server name;
- TLS certificate verification enabled by default;
- an explicit, clearly warned option to disable verification only for deployments that cannot be
  configured with a trusted CA.

Passwords, CA content, client certificates, and client private keys are stored as Node-RED
credentials, not in exported flow JSON.

### Cluster And MemoryDB

Cluster mode accepts a list of startup nodes and creates `ioredis.Cluster` connections.

Cluster connections use:

- auth and TLS under `redisOptions`;
- `dnsLookup: (address, callback) => callback(null, address)` for TLS-enabled AWS cluster
  discovery, as documented by ioredis;
- bounded slot-refresh and connect timeouts for clear failure behavior;
- certificate verification by default;
- no `checkServerIdentity` bypass.

All queues using a Cluster or MemoryDB deployment share the BullMQ prefix `{bull}`. BullMQ uses
multiple Redis keys per queue and `FlowProducer` may operate across queues; the shared hash tag
keeps those keys in one Redis Cluster slot and prevents `CROSSSLOT` failures.

The shared prefix trades cluster-wide sharding of BullMQ keys for correctness of atomic operations.
This tradeoff will be documented.

### Sentinel

Sentinel mode accepts:

- Sentinel endpoint list;
- master name;
- optional Redis data-node username and password;
- optional Sentinel username and password;
- optional TLS for Redis data nodes;
- optional TLS for Sentinel discovery using ioredis `enableTLSForSentinelMode`.

Sentinel failover and reconnect behavior will be tested with Docker.

### Connection Lifecycle

Connections are long-lived and failure-prone. Runtime code must:

- attach `error` listeners to every BullMQ and ioredis object that can emit errors;
- reflect `ready`, `close`, `reconnecting`, and terminal connection states in node status;
- avoid sharing producer and blocking-worker/event connections when their retry requirements differ;
- close Workers and QueueEvents before closing their backing config Queue;
- use bounded force-disconnect fallback only when graceful close cannot complete;
- never leave an input handler promise rejection unobserved.

## Job Processing And Acknowledgement Contract

### Immediate Mode

`immediate` mode preserves current behavior. The Worker processor sends the Node-RED message and
returns immediately, completing the BullMQ job without waiting for downstream nodes.

This mode is the default for existing `bull run` nodes loaded from legacy flows.

### Manual Mode

`manual` mode is recommended for new flows. The Worker processor sends the Node-RED message and
returns a pending promise. The job remains active until a downstream `bull job` node performs a
terminal action.

`bull run` adds:

```js
msg.bull = {
  ackId: "...",        // opaque runtime identifier
  queue: "basecasts",
  runNodeId: "...",
};
```

The runtime registry holds the live BullMQ Job, its queue context, and the promise settlement
functions. It does not expose the Worker lock token.

Manual jobs have a configurable acknowledgement timeout. A timeout rejects the processor with an
`Error`, allowing normal BullMQ retry behavior. Redeploying or closing a `bull run` node rejects its
pending jobs rather than silently completing them.

### `bull job` Actions

Non-terminal actions keep the job active and send the message onward:

| Action | Input | BullMQ behavior |
| --- | --- | --- |
| `progress` | `msg.progress` or `msg.payload` | `job.updateProgress(...)` |
| `removeDeduplicationKey` | active job context | `job.removeDeduplicationKey()` |
| `getChildrenValues` | active parent job context | `job.getChildrenValues()` |
| `getFailedChildrenValues` | active parent job context | `job.getFailedChildrenValues()` |
| `removeUnprocessedChildren` | active parent job context | `job.removeUnprocessedChildren()` |

Terminal actions settle the Worker processor:

| Action | Input | Processor behavior |
| --- | --- | --- |
| `complete` | `msg.result` or `msg.payload` | resolves with the result |
| `fail` | `msg.error` or `msg.payload` | rejects with an `Error`, allowing retries |
| `failUnrecoverable` | `msg.error` or `msg.payload` | rejects with BullMQ `UnrecoverableError` |
| `rateLimit` | `msg.duration` | calls non-deprecated queue-level `rateLimit` and rejects with `Worker.RateLimitError()` |

Using an absent, stale, already-settled, or foreign acknowledgement ID is an error.

## `bull cmd` Feature Surface

`bull cmd` supports these commands and options. Job-targeting commands use `msg.jobId` unless noted.

### Job Creation And Inspection

| Feature | Command or option | Notes |
| --- | --- | --- |
| Add one job | `add` | uses `msg.jobName`, `msg.jobData` or `msg.payload`, and `msg.jobopts` |
| Add jobs in bulk | `addBulk` | `msg.payload` is an array of BullMQ bulk job definitions |
| Get one job | `getJob` | serializes the job or returns `null` |
| Get jobs | `getJobs` | uses `msg.types`, `msg.start`, `msg.end`, and `msg.asc` |
| Get state | `getJobState` | returns the state for `msg.jobId` |
| Remove job | `removeJob` | removes `msg.jobId` |
| Retry one job | `retryJob` | retries `msg.jobId` from `msg.state` when supplied |
| Retry jobs | `retryJobs` | passes validated retry options |

### Delayed Jobs

| Feature | Command or option |
| --- | --- |
| Add delayed job | `msg.jobopts.delay` |
| List delayed jobs | `getDelayed` |
| Change delay | `changeDelay` using `msg.jobId` and `msg.delay` |
| Promote one delayed job | `promoteJob` |
| Promote delayed jobs | `promoteJobs` |

### Priorities

| Feature | Command or option |
| --- | --- |
| Add prioritized job | `msg.jobopts.priority` |
| List prioritized jobs | `getPrioritized` |
| Count per priority | `getCountsPerPriority` using `msg.priorities` |
| Change priority or LIFO state | `changePriority` using `msg.priority` and `msg.lifo` |

Priority validation follows BullMQ's documented range and behavior. Lower positive numbers are
higher priority, while jobs without a priority are processed before prioritized jobs.

### Deduplication

| Feature | Command or option |
| --- | --- |
| Simple, throttle, or debounce mode | `msg.jobopts.deduplication` |
| Get retained job ID | `getDeduplicationJobId` using `msg.deduplicationId` |
| Remove key | `removeDeduplicationKey` using `msg.deduplicationId` |

Deduplicated and duplicated events are observable through `bull events`.

### Retries And Retention

BullMQ job options remain available through `msg.jobopts`, including:

- `attempts`;
- built-in fixed and exponential `backoff`;
- backoff jitter;
- `removeOnComplete`;
- `removeOnFail`;
- FIFO and LIFO behavior.

Custom executable backoff strategies are unsupported as described in Non-Goals.

### Job Schedulers And Legacy Repeat Commands

Native commands:

- `upsertJobScheduler`
- `getJobScheduler`
- `getJobSchedulers`
- `getJobSchedulersCount`
- `removeJobScheduler`

Compatibility aliases:

| Compatibility command | Job Scheduler implementation |
| --- | --- |
| `add` with `msg.jobopts.repeat` | `upsertJobScheduler` |
| `count` | `getJobSchedulersCount` |
| `getRepeatableJobs` | `getJobSchedulers` |
| `getRepeatableJobByKey` | exact-ID `getJobScheduler` |
| `removeRepeatableByKey` | exact-ID `removeJobScheduler` |

Deprecated BullMQ `getRepeatableJobs` and `removeRepeatableByKey` methods will not be called.

### Queue State And Administration

Commands:

- `getJobCounts`
- `pause`
- `resume`
- `drain`
- `clean`
- `stopAndRemoveAllJobs`

`stopAndRemoveAllJobs` removes Job Schedulers, drains waiting and delayed jobs, and cleans supported
states. It must not claim to remove active jobs that BullMQ cannot safely remove.

### Concurrency And Rate Limits

Commands:

- `setGlobalConcurrency`
- `getGlobalConcurrency`
- `removeGlobalConcurrency`
- `setGlobalRateLimit`
- `getGlobalRateLimit`
- `removeGlobalRateLimit`
- `rateLimit`
- `getRateLimitTtl`
- `removeRateLimitKey`

`bull run` separately supports local Worker concurrency and Worker limiter configuration.

### Logs And Metrics

Commands:

- `addJobLog`
- `getJobLogs`
- `exportPrometheusMetrics`

Prometheus output is returned as a string in `msg.payload`. This package will not host an HTTP
metrics endpoint.

## `bull events` Message Contract

For each supported QueueEvents event:

```js
msg.topic = "completed"; // event name
msg.payload = { /* BullMQ event payload */ };
msg.bull = {
  queue: "basecasts",
  event: "completed",
  eventId: "...",        // when BullMQ provides one
};
```

Supported filters and help text will cover at least:

- `active`
- `added`
- `cleaned`
- `completed`
- `deduplicated`
- `delayed`
- `drained`
- `duplicated`
- `failed`
- `paused`
- `progress`
- `removed`
- `resumed`
- `stalled`
- `waiting`
- `waiting-children`

Event payloads are passed through as serializable objects. Errors are reported through Node-RED and
connection status, not emitted as normal event messages.

## Editor Design

The config-node editor will add deployment, auth, and TLS fields while preserving legacy field
names needed to load existing flows.

The editor will:

- show only fields relevant to the selected deployment mode;
- distinguish Redis data-node authentication from Sentinel authentication;
- clearly label TLS verification and warn when it is disabled;
- use password or credential-backed fields for secrets;
- document the `{bull}` Cluster/MemoryDB prefix tradeoff;
- include help for each node and command family;
- avoid exposing arbitrary JSON connection options that could bypass validation or leak secrets.

`bull run`, `bull job`, `bull events`, and `bull flow` will have focused edit dialogs with only
their stable configuration fields. Message-driven options remain documented in help rather than
duplicated as large forms.

Because the editor changes are user-visible and include credentials, they require Playwright tests.

## Error Handling And Validation

Runtime validation will fail early with actionable messages for:

- missing config nodes;
- empty queue names;
- invalid deployment mode;
- malformed host, port, Cluster node, or Sentinel node values;
- missing Sentinel master name;
- invalid TLS certificate/key combinations;
- invalid command names;
- missing required command fields;
- invalid scheduler repeat options;
- conflicting `repeat.cron` and `repeat.pattern`;
- missing scheduler IDs;
- invalid priorities, delays, limits, concurrency values, or rate-limit values;
- missing or stale manual acknowledgement context;
- invalid flow trees.

No async operation may create an unhandled promise rejection. Errors retain the incoming `msg`
where Node-RED error APIs permit it.

## Test Strategy

### TDD Requirement

Implementation follows red-green-refactor:

1. add the smallest failing test for one behavior;
2. run it and confirm the expected failure;
3. implement the minimum production change;
4. run the focused test and relevant regression tests;
5. refactor only while tests remain green;
6. commit logical, human-readable changes.

### Runtime Tests

Mocha and `node-red-node-test-helper` will load real Node-RED flows for runtime behavior. Tests cover:

- config-node credentials and connection option construction;
- status transitions and reconnect behavior;
- every `bull cmd` command and validation error;
- `bull run` immediate and manual modes;
- every `bull job` action, timeout, duplicate acknowledgement, and close path;
- `bull events` filtering and event output;
- `bull flow` atomic parent/child behavior and child result access;
- graceful shutdown and resource cleanup;
- compatibility behavior for legacy flows.

Pure helper tests may use stubs where a real Redis deployment cannot improve the assertion, but
public node behavior must be proven with `node-red-node-test-helper`.

### Docker Deployment Matrix

Docker Compose tests will run one deployment at a time and tear it down with volumes:

| Topology | No auth / no TLS | Auth / no TLS | No auth / TLS | Auth / TLS |
| --- | --- | --- | --- | --- |
| Standalone | required | required | required | required |
| Redis Cluster | required | required | required | required |
| Redis Sentinel | required | required | required | required |

TLS deployments use generated test certificates. Private keys and user-provided credentials are not
committed.

A shared deployment behavior suite will run against every matrix entry and prove:

- config connection;
- add and worker delivery;
- delayed job delivery;
- priority ordering;
- deduplication;
- scheduler creation and removal;
- QueueEvents delivery;
- clean shutdown.

Topology-specific suites will additionally prove:

- Cluster atomic operations and FlowProducer behavior with the `{bull}` prefix;
- Sentinel discovery, auth, reconnect, and failover;
- TLS verification using the generated CA.

### AWS MemoryDB

MemoryDB tests are opt-in and read only these environment variables:

- `MEMORYDB_ENDPOINT`
- `MEMORYDB_PORT`
- `MEMORYDB_USERNAME`
- `MEMORYDB_PASSWORD`
- optional `MEMORYDB_TLS`

They are enabled only by an explicit flag such as `MEMORYDB_ENABLED=1`. The endpoint and credentials
must never be written to repository files, logs, examples, or documentation.

MemoryDB tests prove:

- Cluster discovery;
- ACL auth;
- TLS;
- add and worker delivery;
- Job Scheduler behavior;
- QueueEvents behavior;
- FlowProducer atomic behavior;
- clean shutdown.

### Repeatable And Scheduled Job Tests

Repeat-job coverage is a completion gate.

Tests include:

1. The exact compatibility flow:
   - queue name `basecasts`;
   - payload `gateway-FCC23DFFFE0AA2A8`;
   - `msg.cmd = "add"`;
   - `msg.jobopts.jobId = msg.payload`;
   - `repeat.cron = "30 9,19,29,39,49,59 * * * *"`.
2. Translation from `cron` to `pattern`.
3. Exact scheduler-ID lookup, update, count, list, and removal.
4. Duplicate scheduler prevention through `upsertJobScheduler`.
5. Worker delivery through `bull run`.
6. Interval timing with tolerance.
7. Real absolute cron-boundary delivery using a near-future pattern.
8. Validation of the exact required cron expression's next absolute timestamp without waiting up to
   ten minutes for the next occurrence.
9. Reconnect, restart, shutdown, and cleanup behavior.

Timing assertions use explicit tolerances and record expected and observed timestamps in failure
messages.

### Feature Tests

Feature integration tests cover:

- FIFO and LIFO starts;
- priority ordering and priority counts;
- delayed jobs, delay changes, and promotion;
- simple, throttle, and debounce deduplication;
- duplicated and deduplicated events;
- retry attempts, fixed backoff, exponential backoff, jitter validation, and unrecoverable failure;
- local Worker concurrency;
- Worker limiter, global rate limit, and manual rate limit;
- global concurrency;
- pause and resume;
- job logs and Prometheus metrics;
- parent/child flows, child results, failed child results, and pending child removal;
- event delivery for completed, failed, progress, delayed, stalled where practical, duplicated, and
  deduplicated events.

### Playwright Tests

Playwright starts a real Node-RED 4.1.x editor and verifies:

- legacy config nodes load correctly;
- deployment-mode field visibility;
- standalone, Cluster, and Sentinel field persistence;
- credential persistence without secrets appearing in exported flow JSON;
- TLS field visibility and persistence;
- `bull run` completion-mode and limiter fields;
- new node edit dialogs;
- help text availability.

## Documentation And Examples

User-facing documentation will include:

- rewritten `README.md`;
- migration guide from Bull v4;
- command reference;
- node guide;
- connection and deployment guide;
- testing guide;
- troubleshooting guide;
- unsupported-feature table with reasons;
- warning that Redis must use `maxmemory-policy=noeviction`;
- warning that Bull v4 Redis queue data is not automatically migrated;
- editor help for every node.

Examples will include:

- simple add and run;
- required `basecasts` scheduled job;
- delayed and prioritized jobs;
- deduplication modes;
- manual acknowledgement with complete and fail paths;
- rate limiting;
- QueueEvents;
- parent/child flows;
- standalone, Cluster/MemoryDB, and Sentinel config examples without secrets.

## Agent Maintenance Files

The repository will add:

- `CLAUDE.md` as the canonical shared agent guide;
- `AGENTS.md` as a symlink to `CLAUDE.md`;
- `docs/REFERENCE_MAP.md`;
- `docs/ARCHITECTURE.md`;
- `docs/NODE_GUIDE.md`;
- `docs/CHANGE_WORKFLOW.md`;
- `docs/TESTING.md`;
- `docs/TROUBLESHOOTING.md`.

The agent guide will direct future agents to the reference map, architecture, node guide, workflow,
testing guide, and relevant tests before changing behavior. It will document public compatibility
fields, connection lifecycle hazards, acknowledgement semantics, Cluster prefix requirements, and
the TDD expectation.

## Completion Criteria

The migration is complete only when:

1. `bull` and `sprintf-js` are absent from dependencies and runtime code.
2. BullMQ is pinned to exactly 5.78.0.
3. package metadata targets Node.js 24+ and Node-RED 4.1.x.
4. all six Node-RED node types are implemented and documented.
5. legacy flow and repeat-cron compatibility tests pass.
6. every supported feature in this design has runtime and integration evidence.
7. every unsupported BullMQ feature is documented with a reason.
8. the Docker standalone, Cluster, and Sentinel auth/TLS matrix passes.
9. the opt-in MemoryDB suite passes when the supplied deployment is reachable.
10. Playwright editor tests pass.
11. examples, help, user docs, and agent docs are complete and internally consistent.
12. no secrets are committed or emitted in test logs.
