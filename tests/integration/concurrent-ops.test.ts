import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 26 — concurrent operations / state-combination matrix
 * (audit gap G8 / §3.3 / §7.10).
 *
 * The audit identified that "state transitions are tested in
 * isolation but state combinations are not." This file exercises
 * the four most likely concurrent-ops bug classes:
 *
 *   K1. Two parallel writes to the same path. Under versioning ON
 *       both succeed (both insert versions); the chain remains
 *       coherent. Under versioning OFF the second wins
 *       (last-writer-wins) and there's no torn intermediate state.
 *   K2. Read during a parallel write to the same path. The reader
 *       observes either the prior content or the new content —
 *       never a partial/torn state.
 *   K3. Unlink during a concurrent read. The reader either
 *       completes against the prior state or fails cleanly with
 *       ENOENT — no thrown stack from a torn manifest.
 *   K4. Concurrent dropVersions + readFile of an explicit version
 *       id. The reader either sees the bytes (drop preserved its
 *       chunks via refcount) or fails cleanly with ENOENT.
 *
 * NOTE on determinism: workerd's I/O is single-threaded per DO so
 * "concurrent" here means "interleaved at the await boundary" —
 * which is exactly the scheduling model that produced the
 * tombstone bug (the production failure was a sequential listFiles
 * → stat loop, not a literal race). These tests pin the
 * INTERLEAVING semantics, not OS-level threading.
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
  ENOENT,
} from "../../sdk/src/index";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}

describe("concurrent ops — parallel writes (K1)", () => {
  it("K1a — versioning OFF: two parallel writes to same path → one row, last-writer wins, no torn state", async () => {
    const vfs = createVFS(envFor(), { tenant: "concur-write-off" });
    const results = await Promise.allSettled([
      vfs.writeFile("/x.txt", "AAA"),
      vfs.writeFile("/x.txt", "BBB"),
    ]);
    // Both promises resolve (last-writer-wins; the write API does
    // not throw on a parallel overwrite).
    for (const r of results) {
      expect(r.status).toBe("fulfilled");
    }
    // Final state: exactly one row; readFile returns one of the
    // two payloads (whichever finalized last).
    const list = await vfs.listFiles({ orderBy: "name" });
    const matches = list.items.filter((i) => i.path === "/x.txt");
    expect(matches.length).toBe(1);
    const back = await vfs.readFile("/x.txt", { encoding: "utf8" });
    expect(["AAA", "BBB"]).toContain(back);
  });

  it("K1b — versioning ON: two parallel writes both produce versions; chain is coherent", async () => {
    const vfs = createVFS(envFor(), {
      tenant: "concur-write-on",
      versioning: "enabled",
    });
    const results = await Promise.allSettled([
      vfs.writeFile("/x.txt", "AAA"),
      vfs.writeFile("/x.txt", "BBB"),
    ]);
    for (const r of results) {
      expect(r.status).toBe("fulfilled");
    }
    // Two version rows; head is one of them; both bytes readable.
    const versions = await vfs.listVersions("/x.txt");
    expect(versions.length).toBe(2);
    const sizes = versions.map((v) => v.size).sort();
    expect(sizes).toEqual([3, 3]);
    // Head matches one of the writes.
    const head = await vfs.readFile("/x.txt", { encoding: "utf8" });
    expect(["AAA", "BBB"]).toContain(head);
    // Each version's bytes readable individually.
    const v0 = await vfs.readFile("/x.txt", {
      version: versions[0].id,
      encoding: "utf8",
    });
    const v1 = await vfs.readFile("/x.txt", {
      version: versions[1].id,
      encoding: "utf8",
    });
    expect(["AAA", "BBB"]).toContain(v0);
    expect(["AAA", "BBB"]).toContain(v1);
    expect(v0).not.toBe(v1);
  });
});

describe("concurrent ops — read during write (K2)", () => {
  it("K2 — interleaved write + read: reader sees a coherent value (prior OR new, never torn)", async () => {
    const vfs = createVFS(envFor(), { tenant: "concur-read-write" });
    // Seed with prior content.
    await vfs.writeFile("/x.txt", "prior");
    // Fire write + reads in parallel.
    const [, reads] = await Promise.all([
      vfs.writeFile("/x.txt", "new-content"),
      Promise.all([
        vfs.readFile("/x.txt", { encoding: "utf8" }),
        vfs.readFile("/x.txt", { encoding: "utf8" }),
        vfs.readFile("/x.txt", { encoding: "utf8" }),
      ]),
    ]);
    // Each read MUST be one of the two complete states (never a
    // partial / torn payload).
    for (const r of reads) {
      expect(["prior", "new-content"]).toContain(r);
    }
  });
});

describe("concurrent ops — unlink during read (K3)", () => {
  it("K3 — unlink interleaved with reads: each read either succeeds (prior bytes) or throws ENOENT", async () => {
    const vfs = createVFS(envFor(), { tenant: "concur-unlink-read" });
    await vfs.writeFile("/x.txt", "before");

    const reads = [
      vfs.readFile("/x.txt", { encoding: "utf8" }).catch((e) => ({ err: e })),
      vfs.readFile("/x.txt", { encoding: "utf8" }).catch((e) => ({ err: e })),
    ];
    const unlink = vfs.unlink("/x.txt");
    const after = vfs
      .readFile("/x.txt", { encoding: "utf8" })
      .catch((e) => ({ err: e }));

    await unlink;
    const settled = await Promise.all([...reads, after]);

    for (const r of settled) {
      // Each is either the prior string or an ENOENT — never a
      // thrown non-ENOENT error.
      if (typeof r === "string") {
        expect(r).toBe("before");
      } else {
        expect(r.err).toBeInstanceOf(ENOENT);
      }
    }

    // Final state: ENOENT on any new read.
    let caught: unknown = null;
    try {
      await vfs.readFile("/x.txt");
    } catch (e) {
      caught = e;
    }
    expect(caught).toBeInstanceOf(ENOENT);
  });
});

describe("concurrent ops — dropVersions during read (K4)", () => {
  it("K4 — concurrent dropVersions + readFile(versionId): read either returns bytes or ENOENT cleanly", async () => {
    const vfs = createVFS(envFor(), {
      tenant: "concur-drop-read",
      versioning: "enabled",
    });
    // Seed 4 versions.
    for (let i = 1; i <= 4; i++) {
      await vfs.writeFile("/x.txt", `v${i}`);
    }
    const versions = await vfs.listVersions("/x.txt");
    expect(versions.length).toBe(4);
    // Pick the OLDEST version id as the drop target.
    const oldest = versions[versions.length - 1];

    // Race a read of the oldest against a dropVersions({ keepLast: 1 })
    // (which drops everything except head).
    const read = vfs
      .readFile("/x.txt", { version: oldest.id, encoding: "utf8" })
      .catch((e) => ({ err: e }));
    const drop = vfs.dropVersions("/x.txt", { keepLast: 1 });

    const [readRes, dropRes] = await Promise.all([read, drop]);

    // dropVersions returns its delta count regardless.
    expect(typeof dropRes.dropped).toBe("number");
    expect(typeof dropRes.kept).toBe("number");

    // Read either succeeded with the oldest bytes, or failed
    // cleanly with ENOENT — never a non-ENOENT throw.
    if (typeof readRes === "string") {
      expect(readRes).toBe("v1");
    } else {
      expect(readRes.err).toBeInstanceOf(ENOENT);
    }

    // Final state: head readable (`v4`), versionCount = 1.
    const head = await vfs.readFile("/x.txt", { encoding: "utf8" });
    expect(head).toBe("v4");
    const after = await vfs.listVersions("/x.txt");
    expect(after.length).toBe(1);
  });
});

describe("concurrent ops — bulk parallel writes finalize coherently (K5)", () => {
  it("K5 — 20 parallel writes to distinct paths all land + listFiles surfaces all 20", async () => {
    const vfs = createVFS(envFor(), { tenant: "concur-bulk-write" });
    const N = 20;
    const writes = Array.from({ length: N }, (_, i) =>
      vfs.writeFile(`/f${i}.txt`, `payload-${i}`)
    );
    const results = await Promise.allSettled(writes);
    for (const r of results) {
      expect(r.status).toBe("fulfilled");
    }
    // listFiles surfaces all 20.
    const list = await vfs.listFiles({ orderBy: "name" });
    expect(list.items.length).toBe(N);
    for (let i = 0; i < N; i++) {
      const back = await vfs.readFile(`/f${i}.txt`, { encoding: "utf8" });
      expect(back).toBe(`payload-${i}`);
    }
  });
});
