import { test, expect } from "@playwright/test";
import { freshEmail, signup } from "./helpers";

/**
 * Browser E2E — upload surface.
 *
 *   B.1  signup → upload via the file picker → progress visible →
 *        transfer completes (Transfer panel shows "Complete").
 *   B.2  the auth-bridge mints a VFS token (POST /api/auth/vfs-token)
 *        before the first chunk PUT (network-trace assertion).
 *   B.3  uploaded image appears in the gallery within reasonable time.
 *   B.4  thumbnail bytes load (200 OK on /api/gallery/thumbnail/:id).
 *   B.5  network failure surfaces in the UI as a terminal failed row
 *        (Bug 2 regression — page route blocking simulates 401).
 */

// Tiny PNG (1×1 transparent). Bytes drawn from the standard reference;
// inlined so the test has no fixture-file dependency.
const TINY_PNG = new Uint8Array([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
  0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
  0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
  0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
  0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49,
  0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
]);

test.describe("B — upload", () => {
  test("B.1 — upload via file picker drives the SDK to completion", async ({
    page,
  }) => {
    const email = freshEmail("b1");
    await signup(page, email);
    await page.goto("/files");
    // Reveal upload zone and pick a file.
    await page.getByRole("button", { name: /^Upload$/ }).first().click();
    await page
      .locator('input[type="file"]')
      .first()
      .setInputFiles({
        name: "tiny.png",
        mimeType: "image/png",
        buffer: Buffer.from(TINY_PNG),
      });
    // Transfers panel mounts when active uploads exist.
    await expect(page.getByText(/^Transfers /)).toBeVisible({ timeout: 30_000 });
    // Reaches Complete state.
    await expect(page.getByText(/^Complete$/).first()).toBeVisible({
      timeout: 60_000,
    });
  });

  test("B.2 — auth-bridge mint is observed before any chunk PUT", async ({
    page,
  }) => {
    const email = freshEmail("b2");
    await signup(page, email);
    const observed: { url: string; method: string }[] = [];
    page.on("request", (req) => {
      observed.push({ url: req.url(), method: req.method() });
    });
    await page.goto("/files");
    await page.getByRole("button", { name: /^Upload$/ }).first().click();
    await page
      .locator('input[type="file"]')
      .first()
      .setInputFiles({
        name: "tiny.png",
        mimeType: "image/png",
        buffer: Buffer.from(TINY_PNG),
      });
    await expect(page.getByText(/^Complete$/).first()).toBeVisible({
      timeout: 60_000,
    });
    const mintIdx = observed.findIndex(
      (o) => o.method === "POST" && o.url.endsWith("/api/auth/vfs-token")
    );
    const beginIdx = observed.findIndex((o) =>
      o.url.endsWith("/api/vfs/multipart/begin")
    );
    expect(mintIdx, "auth-bridge mint must be observed").toBeGreaterThanOrEqual(0);
    expect(beginIdx, "multipart begin must be observed").toBeGreaterThanOrEqual(0);
    expect(
      mintIdx,
      "auth-bridge mint MUST happen before the first multipart begin"
    ).toBeLessThan(beginIdx);
  });

  test("B.3 — uploaded image appears in the gallery", async ({ page }) => {
    const email = freshEmail("b3");
    await signup(page, email);
    await page.goto("/files");
    // Register the listener BEFORE triggering the upload so we catch
    // the post-finalize `/api/index/file` POST whether it lands
    // before or after "Complete" renders.
    const indexed = page.waitForResponse(
      (r) => r.url().endsWith("/api/index/file") && r.ok(),
      { timeout: 60_000 }
    );
    await page.getByRole("button", { name: /^Upload$/ }).first().click();
    await page
      .locator('input[type="file"]')
      .first()
      .setInputFiles({
        name: "gallery-test.png",
        mimeType: "image/png",
        buffer: Buffer.from(TINY_PNG),
      });
    await expect(page.getByText(/^Complete$/).first()).toBeVisible({
      timeout: 60_000,
    });
    await indexed;
    await page.goto("/gallery");
    // The gallery hook lazy-loads thumbnails as Blob URLs (so the
    // `<img>` element ends up with `src="blob:..."` rather than the
    // raw `/api/gallery/...` path). Match by `alt` (file name).
    await expect(
      page.locator(`img[alt="gallery-test.png"]`).first()
    ).toBeVisible({ timeout: 30_000 });
  });

  test("B.4 — thumbnail endpoint returns 200 with image content-type", async ({
    page,
  }) => {
    const email = freshEmail("b4");
    await signup(page, email);
    await page.goto("/files");
    await page.getByRole("button", { name: /^Upload$/ }).first().click();
    await page
      .locator('input[type="file"]')
      .first()
      .setInputFiles({
        name: "thumb-test.png",
        mimeType: "image/png",
        buffer: Buffer.from(TINY_PNG),
      });
    await expect(page.getByText(/^Complete$/).first()).toBeVisible({
      timeout: 60_000,
    });
    // Ask the gallery to render; collect the first thumbnail response.
    const thumbResp = page.waitForResponse(
      (r) => /\/api\/gallery\/thumbnail\//.test(r.url()) && r.status() === 200,
      { timeout: 30_000 }
    );
    await page.goto("/gallery");
    const r = await thumbResp;
    expect(r.status()).toBe(200);
    expect(r.headers()["content-type"] ?? "").toMatch(/^image\//);
  });

  test("B.5 — Bug-2 regression: blocked finalize → terminal failed row in UI", async ({
    page,
  }) => {
    const email = freshEmail("b5");
    await signup(page, email);
    // Inject a network route that 500s the multipart-finalize call.
    // This forces parallelUpload to throw inside use-upload's try/catch
    // and exercises the new failedAt+error UI path.
    await page.route("**/api/vfs/multipart/finalize", (route) =>
      route.fulfill({ status: 500, body: '{"code":"EINTERNAL","message":"injected"}' })
    );
    await page.goto("/files");
    await page.getByRole("button", { name: /^Upload$/ }).first().click();
    await page
      .locator('input[type="file"]')
      .first()
      .setInputFiles({
        name: "fail-test.png",
        mimeType: "image/png",
        buffer: Buffer.from(TINY_PNG),
      });
    // The transfer-panel shows the failure inline in destructive color.
    // Status text becomes the error message OR "Failed".
    await expect(page.getByText(/Transfers /).first()).toBeVisible({
      timeout: 30_000,
    });
    await expect(
      page.locator("text=/injected|Failed|Internal/i").first()
    ).toBeVisible({ timeout: 60_000 });
    // The clear-button (X) appears for terminal rows.
    await expect(
      page.locator('[role="button"]').filter({ has: page.locator("svg") })
    ).toHaveCount(await page.locator('[role="button"]').count());
  });
});
