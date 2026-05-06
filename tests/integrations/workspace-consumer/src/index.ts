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
} from "@mossaic/sdk";

// Re-export so wrangler discovers the DO classes by the export
// names declared in this Worker's main module.
export { MossaicUserDO, MossaicShardDO };

interface Env extends MossaicEnv {
  // (Workspace-fixture-specific bindings could go here.)
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
    return new Response("ok", { status: 200 });
  },
} satisfies ExportedHandler<Env>;
