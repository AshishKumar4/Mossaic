import { defineConfig, devices } from "@playwright/test";

/**
 * Playwright config for the SPA browser E2E suite.
 *
 * Targets a live deployed Mossaic App (default
 * https://mossaic.ashishkumarsingh.com) and exercises the user-facing
 * flow that the CLI E2E suite cannot — signup → login → upload via
 * the actual gallery UI → progress → thumbnail → 15-min token
 * rotation → logout.
 *
 * Driven via:
 *   pnpm -F @mossaic/cli test:e2e:browser
 *
 * Override target with `MOSSAIC_E2E_URL=...`. If the URL is
 * unreachable the per-test `beforeAll` skips the suite (we treat the
 * browser tests as opt-in alongside CLI live E2E).
 */

export default defineConfig({
  testDir: "./tests/e2e/browser",
  fullyParallel: false, // single-tenant signup races; serialize.
  forbidOnly: !!process.env.CI,
  retries: 0,
  workers: 1,
  reporter: process.env.CI ? "list" : "list",
  timeout: 120_000,
  expect: { timeout: 15_000 },
  use: {
    baseURL: process.env.MOSSAIC_E2E_URL ?? "https://mossaic.ashishkumarsingh.com",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    actionTimeout: 15_000,
    navigationTimeout: 30_000,
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
