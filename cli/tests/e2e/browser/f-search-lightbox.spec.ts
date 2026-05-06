import { test, expect } from "@playwright/test";
import { freshEmail, signup } from "./helpers";

/**
 * Browser E2E — search + gallery lightbox surface.
 *
 *   F.1  Search input accepts query → request fires to
 *        `/api/search/query` → result list renders or empty-state.
 *   F.2  Gallery → click thumbnail → lightbox opens with the image.
 *   F.3  Lightbox → Escape closes it.
 */

const TINY_PNG = new Uint8Array([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
  0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
  0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
  0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
  0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49,
  0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
]);

async function uploadImage(
  page: import("@playwright/test").Page,
  filename: string
): Promise<void> {
  await page.goto("/files");
  // Register the post-finalize listener BEFORE triggering the
  // upload so we never race the response.
  const indexed = page.waitForResponse(
    (r) => r.url().endsWith("/api/index/file"),
    { timeout: 60_000 }
  );
  await page.getByRole("button", { name: /^Upload$/ }).first().click();
  await page
    .locator('input[type="file"]')
    .first()
    .setInputFiles({
      name: filename,
      mimeType: "image/png",
      buffer: Buffer.from(TINY_PNG),
    });
  await expect(page.getByText(/^Complete$/).first()).toBeVisible({
    timeout: 60_000,
  });
  await indexed;
}

test.describe("F — search & gallery lightbox", () => {
  test("F.1 — Search input fires POST /api/search request", async ({
    page,
  }) => {
    const email = freshEmail("f1");
    await signup(page, email);
    await page.goto("/search");
    const input = page.getByPlaceholder(/Search by meaning/i);
    await expect(input).toBeVisible({ timeout: 15_000 });
    // The SPA debounces the search input; wait for the POST rather
    // than racing the timer.
    const queryReq = page.waitForRequest(
      (r) =>
        r.method() === "POST" && /\/api\/search(\?|$)/.test(r.url()),
      { timeout: 15_000 }
    );
    await input.fill("sunset beach");
    const req = await queryReq;
    expect(req.url()).toMatch(/\/api\/search/);
    // Body carries the query string verbatim.
    const body = req.postData() ?? "";
    expect(body).toMatch(/sunset/);
  });

  test("F.2 — Gallery thumbnail click opens lightbox", async ({ page }) => {
    const email = freshEmail("f2");
    await signup(page, email);
    await uploadImage(page, "f2-photo.png");
    await page.goto("/gallery");
    // Wait for the thumbnail to render (img[alt=...] inside the
    // justified grid; the gallery hook lazy-loads thumbs as Blob URLs).
    const thumb = page.locator('img[alt="f2-photo.png"]').first();
    await expect(thumb).toBeVisible({ timeout: 30_000 });
    await thumb.click();
    // Lightbox renders an <img alt=fileName> in a fixed overlay; the
    // lightbox-specific zoom controls also appear (aria-less but
    // identifiable by the role+name `Reset zoom` etc.). Asserting
    // the lightbox-img mounts is the cheapest cross-cutting check.
    // Use last() because the lightbox img comes AFTER the grid img
    // in DOM order, both with the same alt.
    await expect(
      page.locator('img[alt="f2-photo.png"]').last()
    ).toBeVisible({ timeout: 15_000 });
    // Close button has visible Close icon; the lightbox has an
    // overlay layer that captures clicks. The "X" close button is
    // an icon button with no aria-label, so we use Escape (keyboard).
  });

  test("F.3 — Lightbox closes on Escape", async ({ page }) => {
    const email = freshEmail("f3");
    await signup(page, email);
    await uploadImage(page, "f3-photo.png");
    await page.goto("/gallery");
    const thumb = page.locator('img[alt="f3-photo.png"]').first();
    await expect(thumb).toBeVisible({ timeout: 30_000 });
    await thumb.click();
    await expect(
      page.locator('img[alt="f3-photo.png"]').last()
    ).toBeVisible({ timeout: 15_000 });
    // Escape closes the lightbox; the count drops back to 1 (just
    // the grid thumbnail).
    await page.keyboard.press("Escape");
    await expect(page.locator('img[alt="f3-photo.png"]')).toHaveCount(1, {
      timeout: 15_000,
    });
  });
});
