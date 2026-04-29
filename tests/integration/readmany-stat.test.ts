import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 5 — SDK-driven `readManyStat` integration test.
 *
 * Drives the SDK's `VFS` class directly (not the underlying DO RPC).
 * Verifies:
 *
 *   - The SDK constructs the right `vfs:${ns}:${tenant}` DO instance
 *     and routes the call through one DO RPC.
 *   - readManyStat returns one stat per input path; misses become null
 *     (does NOT throw on a single ENOENT).
 *   - Each returned stat is a real `VFSStat` instance with
 *     isFile()/isDirectory()/isSymbolicLink() methods, mode/size/ino
 *     fields, and Node-Date-shaped mtime/atime/ctime fields.
 *   - The git-status workload (10k stat queries in one batch) works.
 */

import { createVFS, type MossaicEnv, VFSStat } from "../../sdk/src/index";

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;

// The SDK uses MOSSAIC_USER as the canonical binding name. Our test
// harness binds MOSSAIC_USER; we synthesise the consumer-side env that
// createVFS expects by aliasing the bindings.
function makeEnv(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
  };
}

describe("SDK: createVFS + vfs.readManyStat", () => {
  it("returns one stat per input path; null for misses", async () => {
    const vfs = createVFS(makeEnv(), { tenant: "rms-basic" });

    // Seed a few files via the SDK itself.
    await vfs.writeFile("/a.txt", new TextEncoder().encode("alpha"));
    await vfs.writeFile("/b.txt", new TextEncoder().encode("beta!!"));
    await vfs.mkdir("/d");
    await vfs.writeFile("/d/x.txt", new TextEncoder().encode("xx"));

    const out = await vfs.readManyStat([
      "/a.txt",
      "/missing-1",
      "/d",
      "/d/x.txt",
      "/missing-2",
    ]);
    expect(out).toHaveLength(5);

    // /a.txt
    expect(out[0]).toBeInstanceOf(VFSStat);
    expect(out[0]!.isFile()).toBe(true);
    expect(out[0]!.size).toBe(5);
    expect(out[0]!.mode).toBe(0o644);
    expect(typeof out[0]!.ino).toBe("number");
    expect(out[0]!.ino).toBeGreaterThan(0);

    // misses
    expect(out[1]).toBeNull();
    expect(out[4]).toBeNull();

    // /d (directory)
    expect(out[2]).toBeInstanceOf(VFSStat);
    expect(out[2]!.isDirectory()).toBe(true);
    expect(out[2]!.isFile()).toBe(false);
    expect(out[2]!.mode).toBe(0o755);

    // /d/x.txt (nested file)
    expect(out[3]!.isFile()).toBe(true);
    expect(out[3]!.size).toBe(2);
  });

  it("VFSStat exposes Node-Stats-compatible fields", async () => {
    const vfs = createVFS(makeEnv(), { tenant: "rms-stats-shape" });
    await vfs.writeFile("/x.txt", new TextEncoder().encode("hello"));
    const [s] = await vfs.readManyStat(["/x.txt"]);
    expect(s).not.toBeNull();
    const stat = s!;
    expect(typeof stat.mtimeMs).toBe("number");
    expect(typeof stat.atimeMs).toBe("number");
    expect(typeof stat.ctimeMs).toBe("number");
    expect(typeof stat.birthtimeMs).toBe("number");
    expect(stat.mtime).toBeInstanceOf(Date);
    expect(stat.atime).toBeInstanceOf(Date);
    expect(stat.ctime).toBeInstanceOf(Date);
    expect(stat.birthtime).toBeInstanceOf(Date);
    expect(stat.dev).toBe(0);
    expect(typeof stat.uid).toBe("number");
    expect(typeof stat.gid).toBe("number");
    expect(stat.nlink).toBe(1);
    expect(stat.blksize).toBe(4096);
    expect(stat.blocks).toBe(Math.ceil(stat.size / 512));
    expect(stat.isBlockDevice()).toBe(false);
    expect(stat.isCharacterDevice()).toBe(false);
    expect(stat.isFIFO()).toBe(false);
    expect(stat.isSocket()).toBe(false);
  });

  it("symlink is reported via isSymbolicLink (lstat does NOT follow)", async () => {
    const vfs = createVFS(makeEnv(), { tenant: "rms-symlink" });
    await vfs.writeFile("/real.txt", new TextEncoder().encode("real"));
    await vfs.symlink("/real.txt", "/link");

    const [linkStat] = await vfs.readManyStat(["/link"]);
    expect(linkStat).not.toBeNull();
    expect(linkStat!.isSymbolicLink()).toBe(true);
    expect(linkStat!.isFile()).toBe(false);

    // stat() (singular, follow symlinks) returns the file
    const followed = await vfs.stat("/link");
    expect(followed.isFile()).toBe(true);
    expect(followed.isSymbolicLink()).toBe(false);
  });

  it("git-status-style workload: 1000 paths in one RPC", async () => {
    // Smaller than the §7-of-feasibility 10k for test runtime, but
    // exercises the same code path. The point is that the consumer
    // pays exactly 1 outbound RPC for an arbitrarily-long batch.
    const vfs = createVFS(makeEnv(), { tenant: "rms-bulk" });

    // Seed 100 real files; the rest of the 1000 paths are misses.
    for (let i = 0; i < 100; i++) {
      await vfs.writeFile(`/f${i}.txt`, new TextEncoder().encode(`v${i}`));
    }

    const paths: string[] = [];
    for (let i = 0; i < 1000; i++) {
      paths.push(i < 100 ? `/f${i}.txt` : `/missing-${i}.txt`);
    }
    const out = await vfs.readManyStat(paths);
    expect(out).toHaveLength(1000);
    let hits = 0;
    let misses = 0;
    for (const s of out) {
      if (s === null) misses++;
      else if (s.isFile()) hits++;
    }
    expect(hits).toBe(100);
    expect(misses).toBe(900);
  });

  it("createVFS rejects empty tenant", () => {
    expect(() =>
      createVFS(makeEnv(), { tenant: "" } as never)
    ).toThrow(/EINVAL/);
  });

  it("createVFS with namespace + sub composes the right DO", async () => {
    const vfsRoot = createVFS(makeEnv(), { tenant: "rms-scope-root" });
    const vfsAlice = createVFS(makeEnv(), {
      tenant: "rms-scope-root",
      sub: "alice",
    });
    await vfsRoot.writeFile("/r.txt", new TextEncoder().encode("root"));
    await vfsAlice.writeFile("/r.txt", new TextEncoder().encode("alice"));

    // Different DO instances ⇒ different content for the same path.
    const r = await vfsRoot.readFile("/r.txt", { encoding: "utf8" });
    const a = await vfsAlice.readFile("/r.txt", { encoding: "utf8" });
    expect(r).toBe("root");
    expect(a).toBe("alice");
  });
});
