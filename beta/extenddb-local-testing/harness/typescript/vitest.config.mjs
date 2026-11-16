import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // List every test by name, locally and in CI.
    reporters: ["verbose"],
    globalSetup: "./globalSetup.mjs",
    testTimeout: 30000,
    hookTimeout: 30000,
  },
});
