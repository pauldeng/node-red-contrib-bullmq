# Security Policy

## Supported Versions

Security fixes are provided for the latest published major version of `@pauldeng/node-red-contrib-bullmq`.

## Reporting A Vulnerability

Please report security issues privately through GitHub Security Advisories for:

<https://github.com/pauldeng/node-red-contrib-bullmq/security/advisories>

Do not open a public issue for suspected credential exposure, remote execution paths, Redis credential handling, TLS verification bypasses, or queue/job data leaks.

## Sensitive Data Rules

- Redis, Sentinel, and MemoryDB credentials must stay in Node-RED credentials or environment variables.
- Examples, docs, logs, snapshots, and fixtures must not include real secrets.
- BullMQ lock tokens must not be exposed in Node-RED messages.
- TLS private keys must not be committed unless they are clearly disposable local test fixtures.
