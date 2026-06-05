# Change Workflow

## Behavior Changes

1. Read `GOAL.md` and the relevant docs in `docs/REFERENCE_MAP.md`.
2. Add or update the smallest failing test.
3. Run the focused test and confirm the expected failure.
4. Implement the minimal change.
5. Run the focused test and then `npm test`.
6. Update README, node help, and docs when public behavior changes.
7. Record unsupported BullMQ behavior with a reason instead of silently omitting it.

## Connection Changes

Update `docs/CONNECTIONS.md` and tests in `test/connections.test.js`. Do not pass arbitrary ioredis options through messages or editor fields.

## Scheduler Changes

Update `test/scheduler.test.js`. Legacy repeat behavior must use exact scheduler ids and must not call deprecated repeatable-job APIs.

## Editor Changes

Update `bull-queue.html`, static editor contract tests, and Playwright coverage. Credential fields must not export secrets in flow JSON.

## Security

Never commit MemoryDB credentials, Redis passwords, private keys, or generated TLS private keys.
