import { test, expect } from "@playwright/test";
import { freshEmail, signup } from "./helpers";

/**
 * Browser E2E — file/folder actions surface.
 *
 *   E.1  Create folder via "New Folder" dialog → folder appears in list.
 *   E.2  Delete a file via row More menu → file removed from list.
 *   E.3  Logout via sidebar dropdown → redirect to /login.
 */

const TINY_TXT = new TextEncoder().encode("hello world");

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

test.describe("E — files actions", () => {
  test("E.1 — Create folder via dialog → folder appears", async ({ page }) => {
    const email = freshEmail("e1");
    await signup(page, email);
    await page.goto("/files");
    // The "New Folder" button is a top-bar action; the empty-state
    // also renders one (when the file list is empty), so disambiguate
    // with .first() — clicking either opens the same dialog.
    await page
      .getByRole("button", { name: /New Folder/i })
      .first()
      .click();
    // Dialog mounts with a "Folder name" input + "Create" button.
    const input = page.getByPlaceholder("Folder name");
    await expect(input).toBeVisible({ timeout: 10_000 });
    await input.fill("vacation-2026");
    await page.getByRole("button", { name: /^Create$/ }).click();
    // The folder row appears in the list. The folder name renders as
    // bold text; matching it via getByText is robust.
    await expect(page.getByText("vacation-2026").first()).toBeVisible({
      timeout: 15_000,
    });
  });

  test("E.2 — Delete file via row Delete button → file removed", async ({
    page,
  }) => {
    const email = freshEmail("e2");
    await signup(page, email);
    await uploadFile(page, "delete-me.txt", TINY_TXT, "text/plain");

    // The row's action toolbar is hover-revealed (`opacity-0` until
    // `group-hover`). Hover the row to reveal it, then click the
    // Delete button. EnhancedFileRow uses `confirm("Delete this
    // file?")` — register the dialog handler first.
    page.on("dialog", (d) => {
      void d.accept();
    });
    const main = page.locator("main");
    const fileRow = main.getByText("delete-me.txt").first();
    await fileRow.hover();
    // Wait for the DELETE request to land before asserting UI state.
    const deleted = page.waitForResponse(
      (r) => r.url().includes("/api/files/") && r.request().method() === "DELETE",
      { timeout: 15_000 }
    );
    await page.getByRole("button", { name: /^Delete$/ }).first().click();
    await deleted;
    // Verify deletion via the empty-state, which only renders when
    // the file list is genuinely empty (the deleted file's name still
    // shows in the Transfers panel until that auto-clears, but that's
    // a rendering concern in a different DOM region).
    await expect(main.getByText("No files yet")).toBeVisible({
      timeout: 15_000,
    });
  });

  test("E.3 — Logout via sidebar dropdown redirects to /login", async ({
    page,
  }) => {
    const email = freshEmail("e3");
    await signup(page, email);
    // The sidebar's user button shows the email; clicking opens a
    // dropdown with "Log out" inside.
    await page.getByRole("button", { name: new RegExp(email, "i") }).click();
    await page.getByRole("menuitem", { name: /Log out/i }).click();
    // Auth state cleared → ProtectedRoute kicks back to /login.
    await page.waitForURL(/\/login/, { timeout: 15_000 });
    // localStorage is cleared.
    const raw = await page.evaluate(() =>
      localStorage.getItem("mossaic_auth")
    );
    expect(raw).toBeNull();
  });
});
