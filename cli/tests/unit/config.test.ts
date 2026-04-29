import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync, statSync, readFileSync, chmodSync, existsSync } from "node:fs";
import { tmpdir, platform } from "node:os";
import { join } from "node:path";
import {
  readConfig,
  writeConfig,
  resolveProfile,
  configPath,
  verifyConfigPermissions,
} from "../../src/config.js";

describe("config — file IO + permissions", () => {
  let tmp: string;
  let originalHome: string | undefined;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), "mossaic-cli-cfg-"));
    originalHome = process.env.MOSSAIC_CONFIG_HOME;
    process.env.MOSSAIC_CONFIG_HOME = tmp;
  });
  afterEach(() => {
    if (originalHome === undefined) delete process.env.MOSSAIC_CONFIG_HOME;
    else process.env.MOSSAIC_CONFIG_HOME = originalHome;
    rmSync(tmp, { recursive: true, force: true });
  });

  it("returns null when no config file exists", async () => {
    expect(await readConfig()).toBeNull();
  });

  it("writes config with mode 0600 and reads back", async () => {
    await writeConfig({
      active: "default",
      profiles: {
        default: {
          endpoint: "https://example.com",
          jwtSecret: "secret",
          scope: { ns: "default", tenant: "t1", sub: null },
        },
      },
    });
    const back = await readConfig();
    expect(back?.active).toBe("default");
    expect(back?.profiles.default.endpoint).toBe("https://example.com");
    if (platform() !== "win32") {
      const st = statSync(configPath());
      expect(st.mode & 0o777).toBe(0o600);
    }
  });

  it("verifyConfigPermissions returns false if 0644", async () => {
    if (platform() === "win32") return;
    await writeConfig({
      active: "default",
      profiles: {
        default: {
          endpoint: "https://example.com",
          jwtSecret: "secret",
          scope: { ns: "default", tenant: "t1", sub: null },
        },
      },
    });
    chmodSync(configPath(), 0o644);
    expect(await verifyConfigPermissions()).toBe(false);
    chmodSync(configPath(), 0o600);
    expect(await verifyConfigPermissions()).toBe(true);
  });

  it("rejects malformed config with a clear error", async () => {
    const { writeFileSync, mkdirSync } = await import("node:fs");
    mkdirSync(tmp, { recursive: true });
    writeFileSync(configPath(), "not json", { mode: 0o600 });
    await expect(readConfig()).rejects.toThrow(/not valid JSON/);
    writeFileSync(configPath(), '{"foo":"bar"}', { mode: 0o600 });
    await expect(readConfig()).rejects.toThrow(/malformed/);
  });

  it("env vars override stored profile", async () => {
    await writeConfig({
      active: "default",
      profiles: {
        default: {
          endpoint: "https://stored.example",
          jwtSecret: "stored-secret",
          scope: { ns: "stored-ns", tenant: "stored-t", sub: null },
        },
      },
    });
    const oldEp = process.env.MOSSAIC_ENDPOINT;
    const oldNs = process.env.MOSSAIC_NS;
    process.env.MOSSAIC_ENDPOINT = "https://override.example";
    process.env.MOSSAIC_NS = "override-ns";
    try {
      const p = await resolveProfile();
      expect(p?.endpoint).toBe("https://override.example");
      expect(p?.scope.ns).toBe("override-ns");
      expect(p?.scope.tenant).toBe("stored-t");
      expect(p?.jwtSecret).toBe("stored-secret");
    } finally {
      if (oldEp === undefined) delete process.env.MOSSAIC_ENDPOINT;
      else process.env.MOSSAIC_ENDPOINT = oldEp;
      if (oldNs === undefined) delete process.env.MOSSAIC_NS;
      else process.env.MOSSAIC_NS = oldNs;
    }
  });

  it("resolveProfile returns null when nothing is configured", async () => {
    const oldEp = process.env.MOSSAIC_ENDPOINT;
    const oldSec = process.env.MOSSAIC_JWT_SECRET;
    const oldT = process.env.MOSSAIC_TENANT;
    delete process.env.MOSSAIC_ENDPOINT;
    delete process.env.MOSSAIC_JWT_SECRET;
    delete process.env.MOSSAIC_TENANT;
    try {
      const p = await resolveProfile();
      expect(p).toBeNull();
    } finally {
      if (oldEp !== undefined) process.env.MOSSAIC_ENDPOINT = oldEp;
      if (oldSec !== undefined) process.env.MOSSAIC_JWT_SECRET = oldSec;
      if (oldT !== undefined) process.env.MOSSAIC_TENANT = oldT;
    }
  });

  it("config file is not present after fresh tmp dir", () => {
    expect(existsSync(configPath())).toBe(false);
  });

  it("readFileSync of written config has expected JSON shape", async () => {
    await writeConfig({
      active: "p1",
      profiles: {
        p1: {
          endpoint: "https://e",
          jwtSecret: "s",
          scope: { ns: "default", tenant: "t", sub: null },
        },
      },
    });
    const raw = readFileSync(configPath(), "utf8");
    const parsed = JSON.parse(raw);
    expect(parsed.active).toBe("p1");
    expect(parsed.profiles.p1.scope.tenant).toBe("t");
  });
});
