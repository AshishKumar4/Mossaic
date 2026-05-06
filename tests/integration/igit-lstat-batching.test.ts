import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * auto-batched lstat for isomorphic-git workloads.
 *
 * `createIgitFs(vfs, { batchLstat: true })` returns a wrapper that
 * coalesces concurrent lstat calls within a small window into a
 * single readManyStat RPC. Pinned by counting underlying RPCs via a
 * proxy on the inner VFS:
 *   - N concurrent vfs.lstat() calls → 1 readManyStat call
 *   - sequential calls (await between each) → 1 readManyStat per call
 *     (each kicks off + flushes its own batch)
 *   - the wrapper's lstat returns the same VFSStat shape as
 *     vfs.lstat (no behavior change on success)
 *   - missing-path lstat throws ENOENT (matches non-batched contract)
 *   - non-lstat methods pass through unchanged
 */

import {
  createVFS,
  createIgitFs,
  type MossaicEnv,
  ENOENT,
  VFSFsError,
  VFSStat,
} from "../../sdk/src/index";

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
  MOSSAIC_SHARD: DurableObjectNamespace;
}
const E = env as unknown as E;

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

/**
 * Build a counting proxy around a VFS that increments per-method
 * counters on every public-API call. We intercept method *calls*
 * after Promise resolution starts; concurrent calls each bump the
 * counter so the test can assert "underlying RPC count" via
 * counters.lstat / counters.readManyStat.
 */
function withCounters(vfs: ReturnType<typeof createVFS>) {
  const counters = {
    lstat: 0,
    stat: 0,
    readFile: 0,
    readManyStat: 0,
    readdir: 0,
  };
  // We can't proxy DurableObjectStub directly (workerd internals);
  // instead wrap the VFS instance — its methods just dispatch to
  // the underlying stub, so counting at this layer is honest.
  const orig = vfs;
  const wrapped = new Proxy(orig, {
    get(t, prop, recv) {
      const real = Reflect.get(t, prop, recv);
      if (typeof real !== "function") return real;
      if (prop in counters) {
        return (...args: unknown[]) => {
          (counters as Record<string, number>)[prop as string]++;
          return (real as (...a: unknown[]) => unknown).apply(t, args);
        };
      }
      return real;
    },
  }) as typeof vfs;
  return { vfs: wrapped, counters };
}

describe("createIgitFs lstat batching", () => {
  it("batchLstat: false (default) is pass-through; lstat hits the stub directly", async () => {
    const vfs = createVFS(envFor(), { tenant: "lb-passthrough" });
    await vfs.writeFile("/a.txt", "a");
    await vfs.writeFile("/b.txt", "b");

    const fs = createIgitFs(vfs); // no opts → pass-through
    expect(fs).toBe(vfs); // same instance — proves no wrapping cost when off

    const a = await fs.lstat("/a.txt");
    const b = await fs.lstat("/b.txt");
    expect(a.isFile()).toBe(true);
    expect(b.isFile()).toBe(true);
  });

  it("batchLstat: true coalesces concurrent lstat calls into ONE readManyStat", async () => {
    const innerVfs = createVFS(envFor(), { tenant: "lb-coalesce" });
    // Seed 5 files.
    for (const n of ["a", "b", "c", "d", "e"]) {
      await innerVfs.writeFile(`/${n}.txt`, n);
    }
    // Wrap inner with counters BEFORE handing to createIgitFs so we
    // count the calls that the batching wrapper makes through it.
    const { vfs: counted, counters } = withCounters(innerVfs);
    const fs = createIgitFs(counted as ReturnType<typeof createVFS>, {
      batchLstat: true,
      batchWindowMs: 5,
    });

    // Fire 5 lstats concurrently — they all enter the same batch
    // window since none are awaited until Promise.all.
    const results = await Promise.all([
      fs.lstat("/a.txt"),
      fs.lstat("/b.txt"),
      fs.lstat("/c.txt"),
      fs.lstat("/d.txt"),
      fs.lstat("/e.txt"),
    ]);
    expect(results).toHaveLength(5);
    for (const s of results) {
      expect(s).toBeInstanceOf(VFSStat);
      expect(s.isFile()).toBe(true);
    }

    // The architectural promise: 1 underlying RPC, not 5.
    expect(counters.readManyStat).toBe(1);
    // The wrapper does NOT call the underlying lstat at all when
    // batching is active.
    expect(counters.lstat).toBe(0);
  });

  it("duplicate paths within the batch window dedupe to one entry", async () => {
    const innerVfs = createVFS(envFor(), { tenant: "lb-dedupe" });
    await innerVfs.writeFile("/x.txt", "x");
    const { vfs: counted, counters } = withCounters(innerVfs);
    const fs = createIgitFs(counted as ReturnType<typeof createVFS>, {
      batchLstat: true,
      batchWindowMs: 5,
    });

    // 10 concurrent lstats on the SAME path → 1 readManyStat with
    // 1 entry, 10 resolved Promises with the same stat object.
    const results = await Promise.all(
      Array.from({ length: 10 }, () => fs.lstat("/x.txt"))
    );
    expect(results).toHaveLength(10);
    for (const s of results) {
      expect(s.isFile()).toBe(true);
    }
    expect(counters.readManyStat).toBe(1);
  });

  it("missing path throws ENOENT (matches non-batched contract)", async () => {
    const vfs = createVFS(envFor(), { tenant: "lb-enoent" });
    const fs = createIgitFs(vfs, { batchLstat: true, batchWindowMs: 5 });
    let caught: unknown = null;
    try {
      await fs.lstat("/nope");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ENOENT);
    expect((caught as VFSFsError).path).toBe("/nope");
  });

  it("mixed hits and misses in one batch resolve / reject independently", async () => {
    const innerVfs = createVFS(envFor(), { tenant: "lb-mixed" });
    await innerVfs.writeFile("/exists.txt", "yes");
    const { vfs: counted, counters } = withCounters(innerVfs);
    const fs = createIgitFs(counted as ReturnType<typeof createVFS>, {
      batchLstat: true,
      batchWindowMs: 5,
    });

    const results = await Promise.allSettled([
      fs.lstat("/exists.txt"),
      fs.lstat("/missing-1"),
      fs.lstat("/missing-2"),
      fs.lstat("/exists.txt"),
    ]);
    expect(results[0].status).toBe("fulfilled");
    expect(results[1].status).toBe("rejected");
    expect(results[2].status).toBe("rejected");
    expect(results[3].status).toBe("fulfilled");
    if (results[1].status === "rejected") {
      expect(results[1].reason).toBeInstanceOf(ENOENT);
    }
    // 4 calls (3 unique paths after dedupe) → 1 readManyStat.
    expect(counters.readManyStat).toBe(1);
  });

  it("non-lstat methods pass through unchanged on the batched wrapper", async () => {
    const innerVfs = createVFS(envFor(), { tenant: "lb-passthrough-other" });
    const { vfs: counted, counters } = withCounters(innerVfs);
    const fs = createIgitFs(counted as ReturnType<typeof createVFS>, {
      batchLstat: true,
      batchWindowMs: 5,
    });
    await fs.writeFile("/x.txt", "x");
    const back = await fs.readFile("/x.txt", { encoding: "utf8" });
    expect(back).toBe("x");
    expect(counters.readFile).toBe(1);
    // No lstat calls were made → readManyStat stays at 0.
    expect(counters.readManyStat).toBe(0);
  });

  it("sequential lstat calls (each awaited) flush their own batch", async () => {
    const innerVfs = createVFS(envFor(), { tenant: "lb-sequential" });
    for (const n of ["a", "b", "c"]) {
      await innerVfs.writeFile(`/${n}.txt`, n);
    }
    const { vfs: counted, counters } = withCounters(innerVfs);
    const fs = createIgitFs(counted as ReturnType<typeof createVFS>, {
      batchLstat: true,
      batchWindowMs: 5,
    });

    await fs.lstat("/a.txt");
    await fs.lstat("/b.txt");
    await fs.lstat("/c.txt");

    // Each awaited call flushed its own batch → 3 readManyStat.
    // (This is the trade-off: the optimization wins on
    // CONCURRENT bursts, not on serial sequences.)
    expect(counters.readManyStat).toBe(3);
  });
});
