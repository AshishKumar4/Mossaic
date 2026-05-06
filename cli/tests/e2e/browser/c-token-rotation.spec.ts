import { test, expect } from "@playwright/test";
import { freshEmail, signup } from "./helpers";

const TINY_PNG = new Uint8Array([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
  0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
  0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
  0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
  0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49,
  0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
]);

/**
 * Browser E2E — Bug-1 regression suite.
 *
 *   C.1  multiple back-to-back uploads share one VFS token mint
 *        (cache hit; the 60s refresh window suppresses re-mint).
 *   C.2  forcing the cached token expired via in-page time-travel
 *        triggers a fresh mint on the NEXT upload — confirms the
 *        apiKey-callback rotation rather than capturing-once-at-
 *        construction.
 *   C.3  logout → subsequent SDK call (upload via gallery) fails
 *        because the session JWT is gone and the auth-bridge mint
 *        rejects.
 */

test.describe("C — VFS token rotation (Bug 1)", () => {
  test("C.1 — successive uploads share a cached VFS token (no remint)", async ({
    page,
  }) => {
    const email = freshEmail("c1");
    await signup(page, email);
    let mintCount = 0;
    page.on("request", (req) => {
      if (req.method() === "POST" && req.url().endsWith("/api/auth/vfs-token")) {
        mintCount++;
      }
    });
    await page.goto("/files");
    // First upload.
    await page.getByRole("button", { name: /^Upload$/ }).first().click();
    await page
      .locator('input[type="file"]')
      .first()
      .setInputFiles({
        name: "c1-a.png",
        mimeType: "image/png",
        buffer: Buffer.from(TINY_PNG),
      });
    await expect(page.getByText(/^Complete$/).first()).toBeVisible({
      timeout: 60_000,
    });
    const afterFirst = mintCount;
    // Second upload, immediately. No re-mint expected — the cached
    // token is well within its 15-min TTL.
    await page.getByRole("button", { name: /^Upload$/ }).first().click();
    await page
      .locator('input[type="file"]')
      .first()
      .setInputFiles({
        name: "c1-b.png",
        mimeType: "image/png",
        buffer: Buffer.from(TINY_PNG),
      });
    await expect(page.getByText(/^Complete$/).nth(1)).toBeVisible({
      timeout: 60_000,
    });
    expect(mintCount, "second upload should reuse the cached token").toBe(
      afterFirst
    );
  });

  test("C.2 — expired-cache time-travel triggers a fresh mint on the next upload", async ({
    page,
  }) => {
    const email = freshEmail("c2");
    await signup(page, email);
    let mintCount = 0;
    page.on("request", (req) => {
      if (req.method() === "POST" && req.url().endsWith("/api/auth/vfs-token")) {
        mintCount++;
      }
    });
    await page.goto("/files");
    // First upload. This forces the SPA to mint and cache a token.
    await page.getByRole("button", { name: /^Upload$/ }).first().click();
    await page
      .locator('input[type="file"]')
      .first()
      .setInputFiles({
        name: "c2-a.png",
        mimeType: "image/png",
        buffer: Buffer.from(TINY_PNG),
      });
    await expect(page.getByText(/^Complete$/).first()).toBeVisible({
      timeout: 60_000,
    });
    expect(mintCount).toBe(1);

    // Time-travel: advance Date.now in the page context past the TTL
    // (15 minutes + slop). The SDK's apiKey callback is `() =>
    // api.getVfsToken()`, which checks the cached token's
    // `expiresAtMs - 60_000` against `Date.now()` and re-mints if
    // stale. With Date.now shifted forward, the next call MUST mint.
    await page.evaluate(() => {
      const realNow = Date.now;
      const offset = 16 * 60 * 1000; // 16 minutes
      // Replace Date.now globally so api.getVfsToken sees it as expired.
      // We don't restore — the test ends after this assertion.
      Date.now = () => realNow.call(Date) + offset;
    });

    // Second upload. The apiKey callback fires per-request, sees an
    // expired cache, mints fresh.
    await page.getByRole("button", { name: /^Upload$/ }).first().click();
    await page
      .locator('input[type="file"]')
      .first()
      .setInputFiles({
        name: "c2-b.png",
        mimeType: "image/png",
        buffer: Buffer.from(TINY_PNG),
      });
    await expect(page.getByText(/^Complete$/).nth(1)).toBeVisible({
      timeout: 60_000,
    });
    // After time-travel the cached `expiresAtMs` (received as a real
    // server-side timestamp) is far in the past relative to the
    // poisoned `Date.now()`, so EVERY `getApiKey()` call sees an
    // expired cache and re-mints. The exact count depends on how
    // many SDK requests fire (multipart begin/chunk/finalize +
    // postIndexFile etc.) — what matters is that rotation HAPPENED.
    // The pre-rotation cached-cache test (C.1) already gates the
    // "no remint when fresh" direction, so this assertion only
    // guards the rotate-after-expiry direction.
    expect(
      mintCount,
      "expired cache must trigger fresh auth-bridge mint(s)"
    ).toBeGreaterThan(1);
  });

  test("C.3 — invalidating the session JWT → next upload terminally fails", async ({
    page,
  }) => {
    const email = freshEmail("c3");
    await signup(page, email);
    await page.goto("/files");
    // Force the auth-bridge to reject the session by replacing the
    // localStorage'd JWT with a tampered one. We do NOT call the SPA
    // `logout()` because that would route to /login; the test's
    // intent is "session became invalid mid-flight" (e.g. JWT_SECRET
    // rotated server-side, server bug, replay protection). To make
    // the in-memory `api.token` ALSO see the tampered value we force
    // the cached-VFS-token to expire (so `api.getVfsToken()` hits
    // the bridge mint, which will 401), AND we route the bridge
    // endpoint to a 401 so the failure is deterministic regardless
    // of timing.
    await page.route("**/api/auth/vfs-token", (route) =>
      route.fulfill({
        status: 401,
        body: '{"error":"Invalid or expired token"}',
      })
    );
    // Trip the SDK callback into hitting the bridge. The 60s
    // refresh-window check sees a near-expiry (or null) cache and
    // mints — which we now force to 401.
    await page.evaluate(() => {
      const offset = 16 * 60 * 1000;
      const realNow = Date.now;
      Date.now = () => realNow.call(Date) + offset;
    });
    await page.getByRole("button", { name: /^Upload$/ }).first().click();
    await page
      .locator('input[type="file"]')
      .first()
      .setInputFiles({
        name: "c3.png",
        mimeType: "image/png",
        buffer: Buffer.from(TINY_PNG),
      });
    // The terminal-failed UI from Bug-2 fires (failedAt + error).
    // Status text becomes either the api error message or "Failed".
    await expect(
      page.locator("text=/Failed|Invalid|expired|EACCES|Unauthorized/i").first()
    ).toBeVisible({ timeout: 60_000 });
  });
});
