## Summary

Describe the change and the affected Node-RED nodes or docs.

## Verification

- [ ] `npm test`
- [ ] `npm run test:playwright` for editor changes
- [ ] `npm run test:deployments` for Redis deployment changes
- [ ] `npm audit --omit=dev --audit-level=moderate` for release-impacting dependency changes
- [ ] `npm pack --dry-run` for release packaging changes

## Documentation

- [ ] README, node help, examples, and docs are updated when public behavior changes
- [ ] No Redis, Sentinel, or MemoryDB secrets were added

