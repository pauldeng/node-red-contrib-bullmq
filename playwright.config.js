"use strict";

const { defineConfig } = require("@playwright/test");

module.exports = defineConfig({
  testDir: "test/playwright",
  timeout: 30000,
  use: {
    baseURL: "http://127.0.0.1:18889",
    trace: "on-first-retry",
  },
  webServer: {
    command: "node test/playwright/start-node-red.js",
    url: "http://127.0.0.1:18889",
    reuseExistingServer: false,
    timeout: 30000,
  },
});
