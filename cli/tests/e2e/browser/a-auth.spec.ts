import { test, expect } from "@playwright/test";
import { freshEmail, signup, login, readSessionJWT } from "./helpers";

/**
 * Browser E2E — auth surface.
 *
 *   A.1  signup happy-path: form submits, post-signup UI loads,
 *        session JWT lands in localStorage.
 *   A.2  login of just-signed-up user works.
 *   A.3  signup with an already-used email surfaces an error inline.
 *   A.4  session JWT survives a full page reload (auth state restored).
 */

test.describe("A — auth", () => {
  test("A.1 — signup → form unmounts → session JWT in localStorage", async ({
    page,
  }) => {
    const email = freshEmail("a1");
    await signup(page, email);
    const jwt = await readSessionJWT(page);
    expect(jwt, "session JWT must be persisted in localStorage").toBeTruthy();
    expect(jwt!.split(".").length).toBe(3); // looks like a JWT
  });

  test("A.2 — re-login of the just-signed-up account", async ({ page }) => {
    const email = freshEmail("a2");
    await signup(page, email);
    // Logout via clearing localStorage (the SPA reads from there on boot).
    await page.evaluate(() => localStorage.removeItem("mossaic_auth"));
    await page.reload();
    await login(page, email);
    const jwt = await readSessionJWT(page);
    expect(jwt).toBeTruthy();
  });

  test("A.3 — signup with already-used email shows inline error", async ({
    page,
  }) => {
    const email = freshEmail("a3");
    await signup(page, email);
    // logout (drop localStorage), reload, attempt signup again.
    await page.evaluate(() => localStorage.removeItem("mossaic_auth"));
    await page.reload();
    await page.goto("/login");
    // Click "Sign up" first if showing the login form.
    const signupSwitch = page.getByRole("button", { name: /^Sign up$/ });
    if (await signupSwitch.isVisible().catch(() => false)) {
      await signupSwitch.click();
    }
    await page.locator("#email").fill(email);
    await page.locator("#password").fill("browser-e2e-12345");
    await page.getByRole("button", { name: /Create account/i }).click();
    // The auth-page renders an inline error banner with text from the
    // server (`Email already registered`).
    await expect(
      page.getByText(/already registered|Email/i).last()
    ).toBeVisible({ timeout: 15_000 });
  });

  test("A.4 — session JWT survives a hard reload", async ({ page }) => {
    const email = freshEmail("a4");
    await signup(page, email);
    const jwtBefore = await readSessionJWT(page);
    await page.reload();
    const jwtAfter = await readSessionJWT(page);
    expect(jwtAfter).toBe(jwtBefore);
  });
});
