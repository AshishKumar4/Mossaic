/**
 * Consumer fixture — exactly the shape a real customer's Worker takes
 * when adopting `@mossaic/sdk`.
 *
 * The fixture re-exports the DO classes (so wrangler can discover
 * them) and exposes a few /api/* endpoints the test harness drives
 * with `SELF.fetch(...)` (or, in our setup, with a counting-proxy
 * wrapper around the env). Each handler does ONE VFS call so the
 * test can assert: 1 consumer-side outbound = 1 DO RPC.
 *
 * We deliberately keep this file minimal and consumer-flavored. Any
 * helper that's not "what a customer would write" goes in the test
 * file, not here.
 */

import {
  createVFS,
  UserDO,
  ShardDO,
  type MossaicEnv,
} from "../../../../sdk/src/index";

// Phase 11.1: SearchDO is no longer part of the SDK contract. The
// consumer fixture mirrors a real customer adoption — no SearchDO
// re-export, no MOSSAIC_SEARCH binding.
export { UserDO, ShardDO };

export interface Env extends MossaicEnv {
  TEST_TENANT?: string;
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const tenant = env.TEST_TENANT ?? "consumer-fixture";
    const vfs = createVFS(env, { tenant });

    if (req.method === "POST" && url.pathname === "/seed") {
      const path = url.searchParams.get("path") ?? "/seed.txt";
      const body = new Uint8Array(await req.arrayBuffer());
      await vfs.writeFile(path, body);
      return new Response("seeded", { status: 200 });
    }

    if (req.method === "GET" && url.pathname === "/read") {
      const path = url.searchParams.get("path") ?? "/seed.txt";
      const buf = await vfs.readFile(path);
      return new Response(buf, {
        status: 200,
        headers: { "Content-Type": "application/octet-stream" },
      });
    }

    if (req.method === "GET" && url.pathname === "/stat") {
      const path = url.searchParams.get("path") ?? "/seed.txt";
      const stat = await vfs.stat(path);
      return Response.json({
        size: stat.size,
        mode: stat.mode,
        isFile: stat.isFile(),
        isDirectory: stat.isDirectory(),
      });
    }

    if (req.method === "GET" && url.pathname === "/readdir") {
      const path = url.searchParams.get("path") ?? "/";
      return Response.json({ entries: await vfs.readdir(path) });
    }

    return new Response("not found", { status: 404 });
  },
};
