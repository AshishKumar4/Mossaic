/**
 * Persistent CLI configuration at `~/.mossaic/config.json`.
 *
 * Layout:
 * ```
 * {
 *   "active": "default",
 *   "profiles": {
 *     "default": {
 *       "endpoint": "https://mossaic-core.ashishkmr472.workers.dev",
 *       "jwtSecret": "<copy of the wrangler secret>",
 *       "scope": { "ns": "default", "tenant": "team-acme", "sub": null }
 *     }
 *   }
 * }
 * ```
 *
 * The file is written with mode `0600` (owner read/write only). The
 * directory is `0700`. Both are created lazily on first write.
 *
 * Loading respects four sources, in priority order (highest wins):
 *   1. Per-call CLI flags (`--endpoint`, `--secret`, `--ns`, `--tenant`,
 *      `--sub`, `--profile`). The flags layer is applied by `main.ts`
 *      before calling into commands.
 *   2. Environment vars (`MOSSAIC_ENDPOINT`, `MOSSAIC_JWT_SECRET`,
 *      `MOSSAIC_NS`, `MOSSAIC_TENANT`, `MOSSAIC_SUB`).
 *   3. The named profile (`--profile X` or `active`).
 *   4. Compile-time defaults (endpoint = mossaic-core, ns = "default").
 */

import { mkdir, readFile, writeFile, chmod, stat } from "node:fs/promises";
import { existsSync } from "node:fs";
import { homedir, platform } from "node:os";
import { dirname, join } from "node:path";

export interface Profile {
  endpoint: string;
  jwtSecret: string;
  scope: {
    ns: string;
    tenant: string;
    sub: string | null;
  };
}

export interface ConfigFile {
  active: string;
  profiles: Record<string, Profile>;
}

const DEFAULT_ENDPOINT = "https://mossaic-core.ashishkmr472.workers.dev";

export function configDir(): string {
  return process.env.MOSSAIC_CONFIG_HOME ?? join(homedir(), ".mossaic");
}

export function configPath(): string {
  return join(configDir(), "config.json");
}

export async function readConfig(): Promise<ConfigFile | null> {
  const path = configPath();
  if (!existsSync(path)) return null;
  const raw = await readFile(path, "utf8");
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error(`mossaic: config file ${path} is not valid JSON`);
  }
  // Light shape validation — the file is operator-owned but we still
  // want a clean error if the structure is wrong.
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    typeof (parsed as ConfigFile).active !== "string" ||
    typeof (parsed as ConfigFile).profiles !== "object"
  ) {
    throw new Error(
      `mossaic: config file ${path} is malformed (missing "active" or "profiles")`,
    );
  }
  return parsed as ConfigFile;
}

export async function writeConfig(cfg: ConfigFile): Promise<void> {
  const dir = configDir();
  if (!existsSync(dir)) {
    await mkdir(dir, { recursive: true, mode: 0o700 });
  }
  const path = configPath();
  // Write to a temp path then atomic rename so a crash mid-write
  // doesn't corrupt the existing file.
  const tmp = path + ".tmp";
  await writeFile(tmp, JSON.stringify(cfg, null, 2) + "\n", { mode: 0o600 });
  // chmod again to be safe — some platforms ignore mode in writeFile.
  if (platform() !== "win32") {
    await chmod(tmp, 0o600);
  }
  const { rename } = await import("node:fs/promises");
  await rename(tmp, path);
}

/**
 * Resolve the active profile, applying env-var overrides on top.
 * Returns `null` if neither config nor env provides enough to call
 * the API.
 */
export async function resolveProfile(opts?: {
  profile?: string;
  endpoint?: string;
  secret?: string;
  ns?: string;
  tenant?: string;
  sub?: string;
}): Promise<Profile | null> {
  const cfg = await readConfig();
  const profileName = opts?.profile ?? cfg?.active ?? "default";
  const stored = cfg?.profiles[profileName] ?? null;

  // Layer env vars on top of stored.
  const endpoint =
    opts?.endpoint ??
    process.env.MOSSAIC_ENDPOINT ??
    stored?.endpoint ??
    DEFAULT_ENDPOINT;
  const jwtSecret =
    opts?.secret ??
    process.env.MOSSAIC_JWT_SECRET ??
    stored?.jwtSecret ??
    "";
  const ns =
    opts?.ns ?? process.env.MOSSAIC_NS ?? stored?.scope.ns ?? "default";
  const tenant =
    opts?.tenant ?? process.env.MOSSAIC_TENANT ?? stored?.scope.tenant ?? "";
  const sub =
    opts?.sub ?? process.env.MOSSAIC_SUB ?? stored?.scope.sub ?? null;

  if (!endpoint || !jwtSecret || !tenant) return null;
  return {
    endpoint,
    jwtSecret,
    scope: { ns, tenant, sub },
  };
}

/** Verify the file mode is 0600 on POSIX. Returns true if absent (no leak). */
export async function verifyConfigPermissions(): Promise<boolean> {
  if (platform() === "win32") return true;
  const path = configPath();
  if (!existsSync(path)) return true;
  const st = await stat(path);
  // Only owner read/write allowed.
  return (st.mode & 0o077) === 0;
}
