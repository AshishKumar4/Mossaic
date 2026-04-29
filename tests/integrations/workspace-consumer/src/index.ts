/**
 * Phase 14 workspace-consumer fixture.
 *
 * Pinned invariants:
 *   - `@mossaic/sdk` resolves via the `workspace` exports condition
 *     to TS source under `sdk/src/` — no built `dist/` required.
 *   - The fixture's tsconfig has `customConditions: ["workspace"]`
 *     to opt in.
 *   - Zero `@shared/*` / `@core/*` aliases configured on this side.
 *   - `MossaicUserDO` + `MossaicShardDO` are re-exported for wrangler
 *     discovery; binding `class_name: "MossaicUserDO"` works because
 *     wrangler reads the export NAME (not the underlying class).
 *   - Strict TS: `verbatimModuleSyntax` + `erasableSyntaxOnly`.
 */

import {
  createVFS,
  MossaicUserDO,
  MossaicShardDO,
  type MossaicEnv,
  type EncryptionConfig,
} from "@mossaic/sdk";

// Re-export so wrangler discovers the DO classes by the export
// names declared in this Worker's main module.
export { MossaicUserDO, MossaicShardDO };

interface Env extends MossaicEnv {
  // (Workspace-fixture-specific bindings could go here.)
}

// Phase 15: a fixed test-fixture encryption config. This is for
// fixture/demo use ONLY — never use a hard-coded master key in
// production. Real consumers derive it from KMS or a password via
// `deriveMasterFromPassword` (see @mossaic/sdk/encryption).
function fixtureEncryption(): EncryptionConfig {
  const masterKey = new Uint8Array(32);
  masterKey.fill(0x42);
  const tenantSalt = new Uint8Array(32);
  tenantSalt.fill(0xa1);
  return { masterKey, tenantSalt, mode: "convergent" };
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const tenant = url.searchParams.get("tenant") ?? "workspace-fixture";
    const vfs = createVFS(env, { tenant });
    if (req.method === "POST" && url.pathname === "/seed") {
      await vfs.writeFile("/hello.txt", "world");
      return new Response("seeded", { status: 200 });
    }
    if (req.method === "GET" && url.pathname === "/read") {
      const got = await vfs.readFile("/hello.txt", { encoding: "utf8" });
      return new Response(got, { status: 200 });
    }
    // Phase 15: encrypted writeFile + readFile demo. The same VFS
    // instance does both — encryption config is per-VFS, scoped to
    // this tenant.
    if (req.method === "POST" && url.pathname === "/seed-encrypted") {
      const encryptedVfs = createVFS(env, {
        tenant,
        encryption: fixtureEncryption(),
      });
      await encryptedVfs.writeFile("/secret.txt", "encrypted-payload", {
        encrypted: true,
      });
      return new Response("seeded-encrypted", { status: 200 });
    }
    if (req.method === "GET" && url.pathname === "/read-encrypted") {
      const encryptedVfs = createVFS(env, {
        tenant,
        encryption: fixtureEncryption(),
      });
      const got = await encryptedVfs.readFile("/secret.txt", {
        encoding: "utf8",
      });
      return new Response(got, { status: 200 });
    }
    return new Response("ok", { status: 200 });
  },
} satisfies ExportedHandler<Env>;
