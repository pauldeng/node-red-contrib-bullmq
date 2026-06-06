# Examples

Import these files from the Node-RED editor with **Import > Clipboard**.

## `example_flow.json`

End-to-end compatibility flow for the `basecasts` queue. It includes the required scheduled job payload and cron expression, plus basic events, manual acknowledgement, and flow examples.

## `bullmq_features.json`

Small BullMQ feature examples that all use one local Redis queue config named `bullmq-features`.

- Delay: `delay: send later` sets `msg.jobopts.delay = 10000`.
- Priority: `priority: high priority` sets `msg.jobopts.priority = 1`.
- Deduplication: `dedupe: same job once` sets `msg.jobopts.deduplication.id` from `msg.payload`.
- Rate limit: `rate limit: 2 per second` sends `msg.cmd = "setGlobalRateLimit"` with `{ "max": 2, "duration": 1000 }`.
- Scheduler: `scheduler: every minute` adds a repeatable scheduler with `repeat.pattern`.
- Events: the `bull events` node emits completed, failed, delayed, deduplicated, duplicated, and progress events.
- Manual acknowledgement: `manual ack worker` sends a job through `bull job` progress and complete actions.
- Flow: `flow: parent plus child` sends a parent/child tree to `bull flow`.

Point `bullmq-features` at your Redis deployment before deploying the flow.
