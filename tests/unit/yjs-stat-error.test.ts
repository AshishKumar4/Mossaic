import { describe, it, expect } from "vitest";
import { openYDoc } from "../../sdk/src/yjs";
import { ENOENT, EAGAIN, EBUSY } from "../../sdk/src/errors";
import type { VFS } from "../../sdk/src/vfs";

/**
 * Phase 41 Fix 3 — openYDoc must not silently swallow non-ENOENT
 * stat errors (audit 40C top-4).
 *
 * Background: openYDoc calls `vfs.stat(path)` BEFORE the WS upgrade
 * to detect whether the file is encrypted. If yes, it picks up the
 * tenant's EncryptionConfig and threads it through the WebSocket;
 * if no, it opens a plaintext session. Pre-Phase-41, the call was
 * wrapped in a bare `catch {}` so ANY stat failure (including
 * transient EBUSY / EAGAIN / network-blip from a service-mode HTTP
 * client) was treated the same as ENOENT — fileEnc stayed
 * undefined, and openYDoc proceeded in plaintext mode against what
 * may be an encrypted file. A real correctness defect: payloads
 * sent by the consumer's editor would land on the server as
 * plaintext frames against a path the server expects encrypted.
 *
 * Fix: only swallow ENOENT (the documented "file doesn't exist
 * yet — the WS upgrade will create it"). Any other error
 * propagates — fail-safe-secure rather than fail-open-degraded.
 *
 * Three pinning tests:
 *   YS1 — stat throws ENOENT → openYDoc continues (it doesn't
 *         re-throw; we don't drive the test to completion since
 *         the WS upgrade is out-of-scope for a unit test, but we
 *         assert the throw is NOT the ENOENT itself).
 *   YS2 — stat throws EAGAIN → openYDoc re-throws EAGAIN.
 *   YS3 — stat throws EBUSY  → openYDoc re-throws EBUSY.
 */

interface StubOpts {
  statBehaviour: () => Promise<unknown>;
  /** When set, the WS-upgrade stub records that openYDoc reached it. */
  reachedWsOpen?: { value: boolean };
}

/**
 * Build a minimal VFS-shaped mock. We only populate the fields
 * `openYDoc` reads:
 *   - `vfs.stat(path)` — varies per test
 *   - `vfs.opts.encryption` — kept undefined so the
 *     "file is encrypted but consumer config absent" branch never
 *     fires.
 *   - `vfs._openYjsSocketResponse(path)` — returns a fake Response
 *     that THROWS, so the test assertion can distinguish "we
 *     reached the WS upgrade" (= the stat error was correctly
 *     swallowed) from "we re-threw at the stat layer" (= Fix 3).
 */
function buildVfs(opts: StubOpts): VFS {
  return {
    stat: async (_p: string) => opts.statBehaviour(),
    opts: { encryption: undefined },
    _openYjsSocketResponse: async (_p: string) => {
      if (opts.reachedWsOpen) {
        opts.reachedWsOpen.value = true;
      }
      throw new Error(
        "WS_UPGRADE_REACHED — sentinel for 'stat error was correctly swallowed'"
      );
    },
  } as unknown as VFS;
}

describe("Phase 41 Fix 3 — openYDoc stat error handling", () => {
  it("YS1 — stat throws ENOENT → openYDoc continues to the WS upgrade (does NOT re-throw)", async () => {
    const reached = { value: false };
    const vfs = buildVfs({
      statBehaviour: async () => {
        throw new ENOENT({ path: "/never.yj" });
      },
      reachedWsOpen: reached,
    });
    let caught: unknown = null;
    try {
      await openYDoc(vfs, "/never.yj");
    } catch (err) {
      caught = err;
    }
    // openYDoc reached the WS upgrade and the stub there threw the
    // sentinel — this is the proof that the ENOENT was swallowed
    // (otherwise the catch would hold the ENOENT itself).
    expect(reached.value).toBe(true);
    expect(caught).not.toBeInstanceOf(ENOENT);
    expect((caught as Error).message).toMatch(/WS_UPGRADE_REACHED/);
  });

  it("YS2 — stat throws EAGAIN → openYDoc re-throws EAGAIN, never reaches WS upgrade", async () => {
    const reached = { value: false };
    const vfs = buildVfs({
      statBehaviour: async () => {
        throw new EAGAIN({ path: "/locked.yj" });
      },
      reachedWsOpen: reached,
    });
    let caught: unknown = null;
    try {
      await openYDoc(vfs, "/locked.yj");
    } catch (err) {
      caught = err;
    }
    // The transient EAGAIN must propagate — the consumer can retry
    // with backoff. Critically, the WS upgrade must NOT have been
    // attempted; opening a WS in plaintext mode against an
    // unknown-encryption file is the silent-corruption hazard.
    expect(reached.value).toBe(false);
    expect(caught).toBeInstanceOf(EAGAIN);
    expect((caught as EAGAIN).code).toBe("EAGAIN");
  });

  it("YS3 — stat throws EBUSY → openYDoc re-throws EBUSY, never reaches WS upgrade", async () => {
    const reached = { value: false };
    const vfs = buildVfs({
      statBehaviour: async () => {
        throw new EBUSY({ path: "/contended.yj" });
      },
      reachedWsOpen: reached,
    });
    let caught: unknown = null;
    try {
      await openYDoc(vfs, "/contended.yj");
    } catch (err) {
      caught = err;
    }
    expect(reached.value).toBe(false);
    expect(caught).toBeInstanceOf(EBUSY);
    expect((caught as EBUSY).code).toBe("EBUSY");
  });
});
