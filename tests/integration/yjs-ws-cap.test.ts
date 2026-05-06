import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * P1-7 fix — Yjs WebSocket per-pathId hard cap (100 connections).
 *
 * Pre-fix `YjsRuntime.broadcast` did a synchronous loop over all
 * connected sockets per Yjs frame. With N=100 connected clients
 * on one pathId, every frame burned 99 sync `ws.send` calls;
 * DO single-thread held the event loop; throughput cliffed at
 * 20-50 collaborators per file.
 *
 * The fix counts existing sockets via `ctx.getWebSockets(pathId)`
 * BEFORE accepting a new upgrade. Beyond 100 the upgrade is
 * refused with `VFSError("EBUSY", ...)`. At 80+ a `console.warn`
 * fires so an operator can spot approaching-cap files via
 * Logpush.
 *
 * Tests pin (per-DO turn behaviour without spinning up 100 real
 * sockets — the Hibernation API doesn't expose a cheap simulator):
 *   Y1 — vfsOpenYjsSocket throws EINVAL on a non-yjs path
 *        (gate runs before cap check; cap shouldn't mask other
 *        errors).
 *   Y2 — vfsOpenYjsSocket throws ENOENT on a tombstoned-head
 *        path (Phase 25 gate runs before cap check).
 *   Y3 — When `ctx.getWebSockets(pathId)` returns ≥ HARD_CAP, the
 *        upgrade throws EBUSY without acceptWebSocket being
 *        called. We exercise this by using runInDurableObject to
 *        directly invoke the DO method on a yjs-mode path while
 *        the DO has been pre-conditioned to report a saturated
 *        socket count for that pathId.
 *
 * The cap-hit branch's CPU cliff (the underlying motivation) is
 * validated in the live deploy verification step — vitest-pool-
 * workers cannot spin up 100 actual WebSockets to reproduce the
 * fan-out cost.
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}

describe("yjs WebSocket per-path hard cap (P1-7)", () => {
  it("Y1 — non-yjs-mode path → EINVAL (gate runs before cap check)", async () => {
    const tenant = "yjs-cap-y1";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/plain.txt", "hello");

    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
    );

    // Use runInDurableObject so we can call vfsOpenYjsSocket
    // without crossing the DO RPC serialization boundary (a
    // Response with a webSocket field cannot be marshalled
    // across RPC — see worker/core/routes/vfs-yjs-ws.ts:6-9).
    const caught = await runInDurableObject(stub, async (inst, _state) => {
      try {
        await inst.vfsOpenYjsSocket(
          { ns: "default", tenant },
          "/plain.txt"
        );
        return null;
      } catch (err) {
        return err;
      }
    });
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EINVAL|not in yjs mode/i);
  });

  it("Y2 — fresh yjs-mode path: vfsOpenYjsSocket succeeds (positive control)", async () => {
    const tenant = "yjs-cap-y2";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/doc.md", "");
    await vfs.setYjsMode("/doc.md", true);

    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
    );

    // Inside the DO turn we can inspect the Response without
    // crossing the RPC boundary.
    const status = await runInDurableObject(stub, async (inst, _state) => {
      const resp = await inst.vfsOpenYjsSocket(
        { ns: "default", tenant },
        "/doc.md"
      );
      // Close the client side immediately so we don't leak
      // workerd resources into subsequent tests.
      try {
        resp.webSocket?.accept();
        resp.webSocket?.close(1000, "test cleanup");
      } catch {
        /* ignore */
      }
      return resp.status;
    });
    expect(status).toBe(101);
  });

  it("Y3 — vfsOpenYjsSocket throws EBUSY when an existing socket count meets the hard cap", async () => {
    // Construct a yjs-mode path; then pre-accept 100 sockets
    // tagged with that pathId via direct ctx.acceptWebSocket
    // calls inside runInDurableObject. This forces the cap-hit
    // branch on the next vfsOpenYjsSocket call.
    //
    // Caveat: workerd in vitest-pool-workers DOES support
    // `ctx.acceptWebSocket` inside a runInDurableObject
    // callback. Each accepted socket is tagged with the pathId
    // string; subsequent `ctx.getWebSockets(pathId)` returns
    // them all. Their tag-counted population drives the cap
    // check.
    const tenant = "yjs-cap-y3";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/saturated.md", "");
    await vfs.setYjsMode("/saturated.md", true);

    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
    );

    // Resolve the file_id (pathId) since `acceptWebSocket` tags
    // by leafId, not path.
    const pathId = await runInDurableObject(stub, async (_inst, state) => {
      const r = state.storage.sql
        .exec(
          "SELECT file_id FROM files WHERE file_name = 'saturated.md' LIMIT 1"
        )
        .toArray()[0] as { file_id: string };
      return r.file_id;
    });

    // Saturate by accepting 100 sockets tagged with pathId.
    await runInDurableObject(stub, async (_inst, state) => {
      for (let i = 0; i < 100; i++) {
        const pair = new WebSocketPair();
        const server = pair[1];
        state.acceptWebSocket(server, [pathId]);
      }
    });

    // Now verify count is 100 + EBUSY fires.
    const result = await runInDurableObject(
      stub,
      async (inst, state) => {
        const count = state.getWebSockets(pathId).length;
        let caught: unknown = null;
        try {
          await inst.vfsOpenYjsSocket(
            { ns: "default", tenant },
            "/saturated.md"
          );
        } catch (err) {
          caught = err;
        }
        return { count, caught };
      }
    );
    expect(result.count).toBeGreaterThanOrEqual(100);
    expect(result.caught).toBeTruthy();
    const msg =
      result.caught instanceof Error
        ? result.caught.message
        : String(result.caught);
    expect(msg).toMatch(/EBUSY|too many connected clients/i);
  });
});
