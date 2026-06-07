# Release Guide

Use this checklist before publishing `@pauldeng/node-red-contrib-bullmq`.

## Preflight

1. Confirm `package.json` has the intended version and package name.
2. Confirm the GitHub repository is `https://github.com/pauldeng/node-red-contrib-bullmq`.
3. Confirm BullMQ is pinned to exactly `5.78.0`.
4. Confirm no examples, docs, fixtures, or logs contain Redis, Sentinel, or MemoryDB secrets.

## Verification

Run the local release checks:

```sh
npm test
npm run test:playwright
npm audit --omit=dev --audit-level=moderate
npm pack --dry-run
```

For deployment changes, also run:

```sh
npm run test:deployments
```

MemoryDB verification is optional and environment-only:

```sh
MEMORYDB_ENABLED=1 npm run test:deployments
```

## npm Publishing

Prefer npm trusted publishing from GitHub Actions over long-lived npm tokens.

For manual emergency publishing only, use:

```sh
npm publish --access public
```

After publishing, inspect the npm package page and verify the README, license, repository link, and package tarball contents.

## Node-RED Flow Library

After npm publishing succeeds, submit the package manually to the Node-RED Flow Library at:

<https://flows.nodered.org/add/node>

The Flow Library submission should use:

- package: `@pauldeng/node-red-contrib-bullmq`
- repository: `https://github.com/pauldeng/node-red-contrib-bullmq`
- npm package page for the published version

If the package already exists in the Node-RED Flow Library, request a refresh from the package page after publishing a new npm version.

