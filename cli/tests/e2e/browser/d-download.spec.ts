import { test, expect } from "@playwright/test";
import { freshEmail, signup } from "./helpers";

/**
 * Browser E2E — download surface.
 *
 *   D.1  Upload via UI → click Download in /files → bytes round-trip
 *        (Playwright `page.waitForEvent("download")`).
 *   D.2  Multi-chunk file (>1 MB) — progress advances + completes
 *        without getting stuck mid-progress.
 *   D.3  Stale token (mock-time +16 min) → token refresh fires →
 *        download still works.
 *   D.4  GET /api/files/:fileId/path returns 200 + correct path.
 *   D.5  Failed download (server returns 500 on readChunk) surfaces
 *        as a terminal failed transfer row, not a silent "complete".
 */

// Tiny PNG (1×1 transparent). Used for D.1, D.4, D.5.
const TINY_PNG = new Uint8Array([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
  0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
  0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
  0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
  0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49,
  0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
]);

/** Build a deterministic 1.2 MB byte payload — exercises the
 *  chunked tier (>16 KB inline limit, ~2 chunks at 1 MB each). */
function multiChunkBytes(): Uint8Array {
  const data = new Uint8Array(1_200_000);
  for (let i = 0; i < data.length; i++) data[i] = (i * 31 + 7) & 0xff;
  return data;
}

async function uploadFile(
  page: import("@playwright/test").Page,
  filename: string,
  bytes: Uint8Array,
  mimeType: string
): Promise<void> {
  await page.goto("/files");
  await page.getByRole("button", { name: /^Upload$/ }).first().click();
  await page
    .locator('input[type="file"]')
    .first()
    .setInputFiles({ name: filename, mimeType, buffer: Buffer.from(bytes) });
  await expect(page.getByText(/^Complete$/).first()).toBeVisible({
    timeout: 60_000,
  });
}

test.describe("D — download", () => {
  test("D.1 — Upload → click Download → bytes round-trip", async ({ page }) => {
    const email = freshEmail("d1");
    await signup(page, email);
    await uploadFile(page, "d1.png", TINY_PNG, "image/png");

    // Trigger download via the row's icon-only "Download" button
    // (the file-row toolbar reveals on hover; Playwright's
    // getByRole still finds it because the DOM exists).
    const downloadPromise = page.waitForEvent("download", { timeout: 60_000 });
    await page.getByRole("button", { name: /^Download$/ }).first().click();
    const download = await downloadPromise;
    expect(download.suggestedFilename()).toBe("d1.png");

    // Read the bytes back and compare. `download.path()` returns a
    // path to the downloaded file in Playwright's tmp dir.
    const path = await download.path();
    expect(path).toBeTruthy();
    const fs = await import("node:fs/promises");
    const downloaded = new Uint8Array(await fs.readFile(path!));
    expect(downloaded).toEqual(TINY_PNG);
  });

  test("D.2 — Multi-chunk file (>1 MB) progress advances and completes", async ({
    page,
  }) => {
    const email = freshEmail("d2");
    await signup(page, email);
    const data = multiChunkBytes();
    await uploadFile(page, "d2.bin", data, "application/octet-stream");

    const downloadPromise = page.waitForEvent("download", { timeout: 90_000 });
    await page.getByRole("button", { name: /^Download$/ }).first().click();
    // Mid-flight: the transfer-panel should show a download row
    // moving past 0 bytes within reasonable time. We don't assert a
    // specific intermediate value because progress is async; we
    // assert the row mounts and reaches "Complete".
    await expect(page.getByText(/^Transfers /).first()).toBeVisible({
      timeout: 30_000,
    });
    const download = await downloadPromise;
    const fs = await import("node:fs/promises");
    const path = await download.path();
    const downloaded = new Uint8Array(await fs.readFile(path!));
    expect(downloaded.byteLength).toBe(data.byteLength);
    expect(downloaded).toEqual(data);
  });

  test("D.3 — Stale VFS token (Date.now +16 min) → refresh → download succeeds", async ({
    page,
  }) => {
    const email = freshEmail("d3");
    await signup(page, email);
    await uploadFile(page, "d3.png", TINY_PNG, "image/png");

    // Time-travel 16 min forward in the page context. The SPA's
    // VFS-token cache (`api.vfsToken.expiresAtMs`) compares against
    // `Date.now()`; advancing it past the 15-min TTL forces a fresh
    // mint on the next SDK request.
    let mintCount = 0;
    page.on("request", (req) => {
      if (req.method() === "POST" && req.url().endsWith("/api/auth/vfs-token")) {
        mintCount++;
      }
    });
    await page.evaluate(() => {
      const realNow = Date.now;
      const offset = 16 * 60 * 1000;
      Date.now = () => realNow.call(Date) + offset;
    });

    const downloadPromise = page.waitForEvent("download", { timeout: 60_000 });
    await page.getByRole("button", { name: /^Download$/ }).first().click();
    const download = await downloadPromise;
    const fs = await import("node:fs/promises");
    const path = await download.path();
    const downloaded = new Uint8Array(await fs.readFile(path!));
    expect(downloaded).toEqual(TINY_PNG);
    // At least one fresh mint must have fired during the post-
    // time-travel download flow.
    expect(mintCount).toBeGreaterThanOrEqual(1);
  });

  test("D.4 — GET /api/files/:fileId/path returns 200 + correct path", async ({
    page,
  }) => {
    const email = freshEmail("d4");
    await signup(page, email);
    await uploadFile(page, "d4.txt", new TextEncoder().encode("path lookup"), "text/plain");

    // Pull the fileId out of /api/files (via the SPA's localStorage
    // session JWT — same path the UI takes).
    const { fileId, path } = await page.evaluate(async () => {
      const raw = localStorage.getItem("mossaic_auth");
      const token = raw ? (JSON.parse(raw) as { token: string }).token : null;
      const r = await fetch("/api/files", {
        headers: { Authorization: `Bearer ${token}` },
      });
      const body = (await r.json()) as {
        files: { fileId: string; fileName: string }[];
      };
      const row = body.files.find((f) => f.fileName === "d4.txt");
      if (!row) throw new Error("d4.txt missing from /api/files");
      const r2 = await fetch(`/api/files/${row.fileId}/path`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const j = (await r2.json()) as { path: string };
      return { fileId: row.fileId, path: j.path };
    });
    expect(fileId).toBeTruthy();
    expect(path).toBe("/d4.txt");
  });

  test("D.5 — Failed readChunk → terminal failed transfer row", async ({
    page,
  }) => {
    const email = freshEmail("d5");
    await signup(page, email);
    const data = multiChunkBytes();
    await uploadFile(page, "d5.bin", data, "application/octet-stream");

    // Inject a 500 on /api/vfs/readChunk — parallelDownload will
    // exhaust retries and surface the failure to use-download's
    // catch, which sets failedAt + error.
    await page.route("**/api/vfs/readChunk", (route) =>
      route.fulfill({
        status: 500,
        body: '{"code":"EINTERNAL","message":"injected"}',
      })
    );

    await page.getByRole("button", { name: /^Download$/ }).first().click();
    // The transfer-panel renders the row with a destructive error
    // message — Bug-2's terminal-failed UX, applied to download.
    await expect(page.getByText(/^Transfers /).first()).toBeVisible({
      timeout: 30_000,
    });
    await expect(
      page.locator("text=/Failed|injected|Internal/i").first()
    ).toBeVisible({ timeout: 60_000 });
  });
});
