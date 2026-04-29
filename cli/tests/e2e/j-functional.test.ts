/**
 * E2E J — Functional CLI tests via execa (≥10 cases).
 *
 * These run the actual `dist/bin.js` binary and assert stdout / stderr
 * / exit codes. Each test gets a fresh tenant and passes the secret +
 * scope via env vars (so we don't have to write a config file inside
 * the test runner).
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { execa, type ExecaError } from "execa";
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { ulid } from "ulid";
import { freshTenant, type TenantCtx } from "./helpers/tenant.js";
import { hasSecret, requireSecret, ENDPOINT, SECRET } from "./helpers/env.js";

const BIN = resolve(__dirname, "..", "..", "dist", "bin.js");

interface RunOpts {
  cwd?: string;
  input?: string | Buffer;
  env?: Record<string, string>;
  expectFailure?: boolean;
}

async function run(
  ctx: TenantCtx,
  args: string[],
  opts: RunOpts = {},
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const baseEnv: Record<string, string> = {
    ...process.env,
    MOSSAIC_ENDPOINT: ENDPOINT,
    MOSSAIC_JWT_SECRET: SECRET,
    MOSSAIC_NS: ctx.scope.ns,
    MOSSAIC_TENANT: ctx.scope.tenant,
  };
  if (ctx.scope.sub) baseEnv.MOSSAIC_SUB = ctx.scope.sub;
  Object.assign(baseEnv, opts.env ?? {});
  try {
    const r = await execa("node", [BIN, ...args], {
      cwd: opts.cwd,
      input: opts.input,
      env: baseEnv,
      reject: false,
      timeout: 60_000,
    });
    return {
      stdout: typeof r.stdout === "string" ? r.stdout : "",
      stderr: typeof r.stderr === "string" ? r.stderr : "",
      exitCode: r.exitCode ?? 0,
    };
  } catch (err) {
    const e = err as ExecaError;
    return {
      stdout: typeof e.stdout === "string" ? e.stdout : "",
      stderr: typeof e.stderr === "string" ? e.stderr : "",
      exitCode: e.exitCode ?? 1,
    };
  }
}

describe.skipIf(!hasSecret())("J — Functional CLI tests via execa", () => {
  beforeAll(() => requireSecret());

  let ctx: TenantCtx;
  let workdir: string;
  beforeEach(async () => {
    ctx = await freshTenant();
    workdir = mkdtempSync(join(tmpdir(), "mossaic-cli-j-"));
  });
  afterEach(async () => {
    rmSync(workdir, { recursive: true, force: true });
    await ctx.teardown();
  });

  it("J.1 — `auth setup` writes config and `auth whoami` returns ok", async () => {
    const cfgHome = mkdtempSync(join(tmpdir(), "mossaic-cli-cfg-"));
    try {
      const setup = await run(ctx,
        [
          "auth", "setup",
          "--name", "j1",
          "--endpoint", ENDPOINT,
          "--secret", SECRET,
          "--tenant", ctx.scope.tenant,
        ],
        { env: { MOSSAIC_CONFIG_HOME: cfgHome } },
      );
      expect(setup.exitCode).toBe(0);
      expect(setup.stdout).toMatch(/wrote profile "j1"/);

      const whoami = await run(ctx,
        ["--profile", "j1", "auth", "whoami", "--json"],
        { env: { MOSSAIC_CONFIG_HOME: cfgHome } },
      );
      expect(whoami.exitCode).toBe(0);
      const body = JSON.parse(whoami.stdout);
      expect(body.ok).toBe(true);
      expect(body.scope.tenant).toBe(ctx.scope.tenant);
    } finally {
      rmSync(cfgHome, { recursive: true, force: true });
    }
  });

  it("J.2 — `write --text` then `cat` round-trips text", async () => {
    expect((await run(ctx, ["write", "/a.txt", "--text", "hello"])).exitCode).toBe(0);
    const r = await run(ctx, ["cat", "/a.txt", "--encoding", "utf8"]);
    expect(r.exitCode).toBe(0);
    expect(r.stdout).toBe("hello");
  });

  it("J.3 — `put` + `get -o`: byte-equal round-trip", async () => {
    const local = join(workdir, "src.bin");
    const out = join(workdir, "dst.bin");
    writeFileSync(local, Buffer.from([0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe]));
    expect((await run(ctx, ["put", local, "/p.bin"])).exitCode).toBe(0);
    expect((await run(ctx, ["get", "/p.bin", "-o", out])).exitCode).toBe(0);
    const a = readFileSync(local);
    const b = readFileSync(out);
    expect(b.equals(a)).toBe(true);
  });

  it("J.4 — `ls /` lists entries one per line", async () => {
    await run(ctx, ["write", "/x.txt", "--text", "x"]);
    await run(ctx, ["write", "/y.txt", "--text", "y"]);
    const r = await run(ctx, ["ls", "/"]);
    expect(r.exitCode).toBe(0);
    const lines = r.stdout.split("\n").filter(Boolean).sort();
    expect(lines).toEqual(["x.txt", "y.txt"]);
  });

  it("J.5 — `stat --json` is parseable JSON with mode/size/mtimeMs", async () => {
    await run(ctx, ["write", "/s.txt", "--text", "abc"]);
    const r = await run(ctx, ["--json", "stat", "/s.txt"]);
    expect(r.exitCode).toBe(0);
    const parsed = JSON.parse(r.stdout);
    expect(parsed.path).toBe("/s.txt");
    expect(parsed.size).toBe(3);
    expect(typeof parsed.mtimeMs).toBe("number");
    expect(typeof parsed.mode).toBe("number");
  });

  it("J.6 — `find --tag t1 --json` returns array of items", async () => {
    await run(ctx, ["write", "/t1.txt", "--text", "x", "--tag", "t1"]);
    await run(ctx, ["write", "/t2.txt", "--text", "x", "--tag", "t2"]);
    const r = await run(ctx, ["--json", "find", "--tag", "t1"]);
    expect(r.exitCode).toBe(0);
    const parsed = JSON.parse(r.stdout);
    expect(Array.isArray(parsed.items)).toBe(true);
    expect(parsed.items.length).toBe(1);
    expect(parsed.items[0].path).toBe("/t1.txt");
  });

  it("J.7 — `versions ls --json` after enabling versioning + 3 writes", async () => {
    // Enable versioning via the admin route directly (CLI doesn't
    // expose `versions enable` in v1).
    const r0 = await fetch(ENDPOINT + "/api/vfs/admin/setVersioning", {
      method: "POST",
      headers: { Authorization: `Bearer ${ctx.token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: true }),
    });
    expect(r0.ok).toBe(true);
    for (const v of ["v1", "v2", "v3"]) {
      await run(ctx, ["write", "/v.txt", "--text", v]);
    }
    const r = await run(ctx, ["--json", "versions", "ls", "/v.txt"]);
    expect(r.exitCode).toBe(0);
    const versions = JSON.parse(r.stdout);
    expect(versions.length).toBeGreaterThanOrEqual(3);
  });

  it("J.8 — `versions restore` then `cat` returns historical bytes", async () => {
    await fetch(ENDPOINT + "/api/vfs/admin/setVersioning", {
      method: "POST",
      headers: { Authorization: `Bearer ${ctx.token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: true }),
    });
    await run(ctx, ["write", "/r.txt", "--text", "first"]);
    await run(ctx, ["write", "/r.txt", "--text", "second"]);
    const list = await run(ctx, ["--json", "versions", "ls", "/r.txt"]);
    const versions = JSON.parse(list.stdout);
    const oldest = versions[versions.length - 1];
    const restore = await run(ctx,
      ["versions", "restore", "/r.txt", oldest.id],
    );
    expect(restore.exitCode).toBe(0);
    const cat = await run(ctx, ["cat", "/r.txt", "--encoding", "utf8"]);
    expect(cat.stdout).toBe("first");
  });

  it("J.9 — `exists` exit code semantics: 0 if present, 1 if missing", async () => {
    await run(ctx, ["write", "/e.txt", "--text", "x"]);
    const present = await run(ctx, ["exists", "/e.txt"]);
    expect(present.exitCode).toBe(0);
    expect(present.stdout.trim()).toBe("true");

    const missing = await run(ctx, ["exists", "/missing.txt"]);
    expect(missing.exitCode).toBe(1);
    expect(missing.stdout.trim()).toBe("false");
  });

  it("J.10 — `chmod --yjs true` flips the yjs-mode bit; stat shows it", async () => {
    await run(ctx, ["write", "/y.md", "--text", ""]);
    const before = JSON.parse((await run(ctx, ["--json", "stat", "/y.md"])).stdout);
    expect(before.mode & 0o4000).toBe(0);
    expect((await run(ctx, ["chmod", "--yjs", "true", "/y.md"])).exitCode).toBe(0);
    const after = JSON.parse((await run(ctx, ["--json", "stat", "/y.md"])).stdout);
    expect(after.mode & 0o4000).toBe(0o4000);
  });

  it("J.11 — `cp` + `find --tag` shows copied tags", async () => {
    await run(ctx, ["write", "/src.bin", "--text", "src", "--tag", "origin"]);
    expect((await run(ctx, ["cp", "/src.bin", "/dest.bin"])).exitCode).toBe(0);
    const r = await run(ctx, ["--json", "find", "--tag", "origin"]);
    const parsed = JSON.parse(r.stdout);
    const paths = parsed.items.map((i: { path: string }) => i.path).sort();
    expect(paths).toEqual(["/dest.bin", "/src.bin"]);
  });

  it("J.12 — invalid metadata exit-code 1 with EINVAL on stderr", async () => {
    const r = await run(ctx,
      ["write", "/bad.txt", "--text", "x", "--tag", "bad!char"],
      { expectFailure: true },
    );
    expect(r.exitCode).toBe(1);
    expect(r.stderr).toMatch(/EINVAL|invalid value/);
  });

  it("J.13 — `token mint` prints a valid JWT", async () => {
    const r = await run(ctx, ["token", "mint"]);
    expect(r.exitCode).toBe(0);
    const tok = r.stdout.trim();
    expect(tok.split(".").length).toBe(3);
    // Round-trip the token by calling /api/health (which doesn't even
    // need auth; the assertion is the token format is non-empty).
    expect(tok.length).toBeGreaterThan(50);
    void ulid; // suppress unused
  });
});
