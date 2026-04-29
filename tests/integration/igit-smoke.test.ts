import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";
import git from "isomorphic-git";

/**
 * Phase 6 — isomorphic-git end-to-end smoke.
 *
 * Drives a real git workflow against the SDK's VFS:
 *   git.init  → creates .git/ scaffolding via mkdir/writeFile
 *   writeFile → adds a working-tree file (consumer-side)
 *   git.add   → stages it via .git/index manipulation
 *   git.commit→ writes commit object + updates HEAD
 *   git.log   → reads back the commit
 *
 * This exercises a broad slice of the fs/promises surface:
 *   - mkdir (with and without recursive)
 *   - writeFile / readFile (incl. encoding overload)
 *   - readdir / stat / lstat
 *   - unlink / rename / exists
 * If isomorphic-git can complete this round-trip, the SDK's
 * fs/promises shape is correct enough for real-world git workloads.
 *
 * The test is intentionally minimal — Phase 6 is a smoke test, not
 * a full git suite. Verifies the wiring; deeper ops (clone, push,
 * merge) are out of scope.
 */

import { createVFS, type MossaicEnv } from "../../sdk/src/index";

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;

function makeEnv(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}

describe("isomorphic-git end-to-end against @mossaic/sdk VFS", () => {
  it("init → writeFile → add → commit → log round-trips a commit", async () => {
    const vfs = createVFS(makeEnv(), { tenant: "igit-roundtrip" });
    const dir = "/repo";
    // isomorphic-git's `fs` plugin needs `.promises` — VFS satisfies
    // that by self-reference (vfs.promises === vfs).
    const fs = vfs;

    // 1. Initialise an empty repo. isomorphic-git creates the
    //    .git/ scaffolding (objects/, refs/, HEAD, config) via
    //    mkdir + writeFile. Our mkdir supports `recursive: true`.
    await git.init({ fs, dir, defaultBranch: "main" });

    // Sanity: .git/HEAD must exist after init.
    expect(await vfs.exists(`${dir}/.git/HEAD`)).toBe(true);
    const head = await vfs.readFile(`${dir}/.git/HEAD`, { encoding: "utf8" });
    expect(head).toMatch(/^ref:/);

    // 2. Write a working-tree file.
    await vfs.writeFile(
      `${dir}/README.md`,
      new TextEncoder().encode("# hello mossaic\n")
    );

    // 3. Stage it. add() walks the path, calls stat, reads bytes,
    //    writes a blob object + updates the index. Failures here are
    //    the most common SDK shape mismatches.
    await git.add({ fs, dir, filepath: "README.md" });

    // 4. Commit. commit() writes a commit object referencing the tree.
    const oid = await git.commit({
      fs,
      dir,
      message: "first",
      author: { name: "Tester", email: "test@e.com" },
    });
    expect(typeof oid).toBe("string");
    expect(oid).toMatch(/^[a-f0-9]{40}$/);

    // 5. Read it back via git.log.
    const logs = await git.log({ fs, dir });
    expect(logs.length).toBe(1);
    expect(logs[0].oid).toBe(oid);
    expect(logs[0].commit.message).toBe("first\n");
    expect(logs[0].commit.author.name).toBe("Tester");
  });

  it("exercises additional fs ops: readdir, stat, lstat, rename, unlink", async () => {
    const vfs = createVFS(makeEnv(), { tenant: "igit-fs-ops" });

    // mkdir + nested mkdir
    await vfs.mkdir("/work", { recursive: true });
    await vfs.mkdir("/work/sub", { recursive: true });

    // writeFile + readFile round-trip
    const greeting = new TextEncoder().encode("hello");
    await vfs.writeFile("/work/a.txt", greeting);
    await vfs.writeFile("/work/b.txt", new TextEncoder().encode("world"));
    await vfs.writeFile("/work/sub/c.txt", new TextEncoder().encode("nested"));

    // readdir lists both files + the subfolder (sorted by VFS impl)
    const root = await vfs.readdir("/work");
    expect(root.sort()).toEqual(["a.txt", "b.txt", "sub"]);

    // stat returns Stats with isFile/isDirectory
    const fStat = await vfs.stat("/work/a.txt");
    expect(fStat.isFile()).toBe(true);
    expect(fStat.isDirectory()).toBe(false);
    expect(fStat.size).toBe(greeting.byteLength);

    const dStat = await vfs.stat("/work/sub");
    expect(dStat.isDirectory()).toBe(true);
    expect(dStat.isFile()).toBe(false);

    // lstat on a regular file == stat (no symlink)
    const lStat = await vfs.lstat("/work/a.txt");
    expect(lStat.isFile()).toBe(true);
    expect(lStat.size).toBe(fStat.size);

    // rename moves file
    await vfs.rename("/work/a.txt", "/work/sub/renamed.txt");
    expect(await vfs.exists("/work/a.txt")).toBe(false);
    expect(await vfs.exists("/work/sub/renamed.txt")).toBe(true);

    // unlink removes file
    await vfs.unlink("/work/b.txt");
    expect(await vfs.exists("/work/b.txt")).toBe(false);

    // readFile of removed → ENOENT
    let threw = false;
    try {
      await vfs.readFile("/work/b.txt");
    } catch (err) {
      threw = true;
      expect((err as { code?: string }).code).toBe("ENOENT");
    }
    expect(threw).toBe(true);
  });

  it("git.log on an empty repo behaves correctly (no commits)", async () => {
    const vfs = createVFS(makeEnv(), { tenant: "igit-empty" });
    const fs = vfs;
    const dir = "/repo";
    await git.init({ fs, dir, defaultBranch: "main" });
    // log on a repo with no commits throws NotFoundError. We just
    // ensure we get a clear, code-ful error rather than something
    // weird from the SDK layer.
    let caught: { code?: string } | null = null;
    try {
      await git.log({ fs, dir });
    } catch (err) {
      caught = err as { code?: string };
    }
    expect(caught).not.toBeNull();
    // isomorphic-git's NotFoundError carries `code` "NotFoundError".
    // We just check that some error was thrown — exact code name is
    // igit-internal, not our SDK contract.
    expect(typeof caught!).toBe("object");
  });
});
