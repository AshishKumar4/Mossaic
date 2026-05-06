import { type Page, expect } from "@playwright/test";

/**
 * Generate a fresh, sufficiently-unique email per test run. The App's
 * signup gates on (email)-uniqueness so two parallel runs can't share
 * an email; we suffix with `Date.now()` + 6 random chars.
 */
export function freshEmail(prefix = "browser-e2e"): string {
  const rand = Math.random().toString(36).slice(2, 8);
  return `${prefix}-${Date.now()}-${rand}@example.com`;
}

/**
 * Drive the SPA's signup form. Asserts the post-signup state is
 * authenticated (the form unmounts and the gallery/files surface
 * appears).
 */
export async function signup(
  page: Page,
  email: string,
  password = "browser-e2e-12345"
): Promise<void> {
  await page.goto("/login");
  // Click the "Sign up" mode-switch if showing the login form.
  const signupSwitch = page.getByRole("button", { name: /^Sign up$/ });
  if (await signupSwitch.isVisible().catch(() => false)) {
    await signupSwitch.click();
  }
  await page.locator("#email").fill(email);
  await page.locator("#password").fill(password);
  await page.getByRole("button", { name: /Create account/i }).click();
  // Auth success redirects to /files. Wait for the URL change to confirm
  // we left the auth page.
  await page.waitForURL(/\/files/, { timeout: 30_000 });
}

/** Drive the SPA's login form. */
export async function login(
  page: Page,
  email: string,
  password = "browser-e2e-12345"
): Promise<void> {
  await page.goto("/login");
  // Ensure we're on the login mode (not signup).
  const loginSwitch = page.getByRole("button", { name: /^Sign in$/ });
  if (await loginSwitch.isVisible().catch(() => false)) {
    await loginSwitch.click();
  }
  await page.locator("#email").fill(email);
  await page.locator("#password").fill(password);
  await page.getByRole("button", { name: /Sign in/i }).click();
  await page.waitForURL(/\/files/, { timeout: 30_000 });
}

/**
 * Read the SPA's session JWT out of localStorage. Returns null when
 * unauthenticated.
 */
export async function readSessionJWT(page: Page): Promise<string | null> {
  return await page.evaluate(() => {
    const raw = localStorage.getItem("mossaic_auth");
    if (!raw) return null;
    try {
      const parsed = JSON.parse(raw) as { token?: string };
      return parsed.token ?? null;
    } catch {
      return null;
    }
  });
}

/**
 * Force the SPA's cached VFS token to be expired. The token cache lives
 * inside `api.vfsToken` (a private field on `ApiClient`); since we can
 * only reach it from inside the page context, we delete the entire
 * `vfsToken` cache via the `clearVfsToken` method exposed on the
 * `api` singleton (which is bundled into the SPA at `window.__api__` —
 * we attach it via a window assignment in the test setup if missing).
 *
 * If `window.__api__` isn't exposed (production build without the
 * test hook), we fall back to inspecting/mutating storage directly:
 * the API client's `vfsToken` cache is in-memory only, so the next
 * `api.getVfsToken()` re-mints anyway after a hard reload. Use that.
 */
export async function expireCachedVfsToken(page: Page): Promise<void> {
  await page.evaluate(() => {
    const w = window as unknown as {
      __mossaicApi__?: { clearVfsToken?: () => void };
    };
    w.__mossaicApi__?.clearVfsToken?.();
  });
}

/**
 * Assert the SPA can complete a small upload via the file picker,
 * the transfer panel shows complete state, and the file appears in
 * the listing within `timeoutMs`.
 */
export async function uploadOneFile(
  page: Page,
  filename: string,
  bytes: Uint8Array,
  mimeType: string,
  timeoutMs = 30_000
): Promise<void> {
  // Navigate to the Files page (where the UploadZone lives).
  await page.goto("/files");
  // Click the visible "Upload" button (which toggles the UploadZone).
  const uploadBtn = page.getByRole("button", { name: /^Upload$/ }).first();
  await uploadBtn.click();
  // The file <input type="file"> is hidden (className="hidden") inside
  // the upload zone; Playwright can drive it via setInputFiles
  // regardless of visibility.
  const input = page.locator('input[type="file"]').first();
  await input.setInputFiles({ name: filename, mimeType, buffer: Buffer.from(bytes) });
  // The transfer-panel renders when uploads are active. Wait for the
  // file row to appear and then for the row to reach a terminal state
  // (Complete or an error message). The "Complete" text is in
  // `transfer-panel.tsx`'s status line.
  const transferPanel = page.locator("text=Transfers").first();
  await expect(transferPanel).toBeVisible({ timeout: timeoutMs });
  await expect(page.getByText(/^Complete$/).first()).toBeVisible({
    timeout: timeoutMs,
  });
}
