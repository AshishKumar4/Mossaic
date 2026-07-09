import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { vfsUserDOName } from "@core/lib/utils";
import { INLINE_LIMIT } from "@shared/inline";
import {
  createVFS,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";

const PATHS = ["/alpha.bin", "/beta.bin", "/gamma.bin"] as const;
const PAYLOAD_SIZES = [0, 1, INLINE_LIMIT, INLINE_LIMIT + 1, 64 * 1024] as const;
const BASE_SEED = 0x5eedc0de;
const RUN_COUNT = 20;
const COMMAND_COUNT = 12;
const MULTI_CHUNK_SIZE = 1_048_576 + 1;

type TestPath = (typeof PATHS)[number];

interface ModelFile {
  bytes: Uint8Array;
  hash: string;
}

type Model = Map<TestPath, ModelFile>;

type Command =
  | { kind: "write" | "overwrite"; path: TestPath; bytes: Uint8Array }
  | { kind: "rename"; src: TestPath; dst: TestPath }
  | { kind: "unlink" | "read" | "exists" | "stat"; path: TestPath }
  | { kind: "list" };

interface Random {
  nextUint32(): number;
  int(limit: number): number;
}

interface StorageAccounting {
  storageUsed: number;
  fileCount: number;
  inlineBytesUsed: number;
  uploadingFiles: number;
  cleanupIntents: number;
}

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}

const E = env as unknown as TestEnv;

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD:
      E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

function randomFor(seed: number): Random {
  let state = seed >>> 0;
  const nextUint32 = (): number => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return (value ^ (value >>> 14)) >>> 0;
  };
  return {
    nextUint32,
    int(limit: number): number {
      return nextUint32() % limit;
    },
  };
}

function seedForRun(run: number): number {
  return (BASE_SEED + Math.imul(run, 0x9e3779b9)) >>> 0;
}

function formatSeed(seed: number): string {
  return `0x${seed.toString(16).padStart(8, "0")}`;
}

function pick<T>(values: readonly T[], random: Random): T {
  const value = values[random.int(values.length)];
  if (value === undefined) throw new Error("cannot pick from an empty set");
  return value;
}

function pickPath(random: Random): TestPath {
  return pick(PATHS, random);
}

function pickPathByPresence(
  model: Model,
  present: boolean,
  random: Random
): TestPath {
  return pick(
    PATHS.filter((path) => model.has(path) === present),
    random
  );
}

function pickDifferentPath(path: TestPath, random: Random): TestPath {
  return pick(
    PATHS.filter((candidate) => candidate !== path),
    random
  );
}

function payload(size: number, salt: number): Uint8Array {
  const bytes = new Uint8Array(size);
  let state = salt >>> 0;
  for (let index = 0; index < bytes.byteLength; index++) {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
    bytes[index] = state >>> 24;
  }
  return bytes;
}

function writeCommand(
  kind: "write" | "overwrite",
  path: TestPath,
  size: number,
  random: Random
): Command {
  return { kind, path, bytes: payload(size, random.nextUint32()) };
}

function generateCommand(
  step: number,
  run: number,
  initialPath: TestPath,
  model: Model,
  random: Random
): Command {
  switch (step) {
    case 0:
      return writeCommand("write", initialPath, PAYLOAD_SIZES[run % 5], random);
    case 1:
      return writeCommand(
        "overwrite",
        initialPath,
        PAYLOAD_SIZES[(run + 2) % 5],
        random
      );
    case 2: {
      if (run === 0) {
        const src = pickPathByPresence(model, false, random);
        return { kind: "rename", src, dst: pickDifferentPath(src, random) };
      }
      return { kind: "read", path: pickPathByPresence(model, true, random) };
    }
    case 3:
      return {
        kind: run === 1 ? "unlink" : "exists",
        path:
          run === 1
            ? pickPathByPresence(model, false, random)
            : pickPath(random),
      };
    case 4:
      return run === 2
        ? { kind: "read", path: pickPathByPresence(model, false, random) }
        : { kind: "list" };
    case 5:
      return writeCommand(
        "write",
        pickPathByPresence(model, false, random),
        PAYLOAD_SIZES[(run + 4) % 5],
        random
      );
    case 6: {
      const src = pickPathByPresence(model, true, random);
      const dst =
        run % 2 === 0
          ? pick(
              PATHS.filter((path) => path !== src && model.has(path)),
              random
            )
          : pickPathByPresence(model, false, random);
      return { kind: "rename", src, dst };
    }
    case 7:
      return { kind: "exists", path: pickPath(random) };
    case 8:
      return { kind: "stat", path: pickPathByPresence(model, true, random) };
    case 9:
      return { kind: "list" };
    case 10:
      return { kind: "unlink", path: pickPathByPresence(model, true, random) };
    default:
      return generateRandomCommand(model, random);
  }
}

function generateRandomCommand(model: Model, random: Random): Command {
  if (model.size === 0) {
    return writeCommand(
      "write",
      pickPath(random),
      pick(PAYLOAD_SIZES, random),
      random
    );
  }
  switch (random.int(7)) {
    case 0: {
      const path = pickPath(random);
      return writeCommand(
        model.has(path) ? "overwrite" : "write",
        path,
        pick(PAYLOAD_SIZES, random),
        random
      );
    }
    case 1: {
      const src = pickPathByPresence(model, true, random);
      return { kind: "rename", src, dst: pickDifferentPath(src, random) };
    }
    case 2:
      return { kind: "unlink", path: pickPathByPresence(model, true, random) };
    case 3:
      return { kind: "read", path: pickPathByPresence(model, true, random) };
    case 4:
      return { kind: "exists", path: pickPath(random) };
    case 5:
      return { kind: "stat", path: pickPathByPresence(model, true, random) };
    default:
      return { kind: "list" };
  }
}

async function sha256(bytes: Uint8Array): Promise<string> {
  const copy = Uint8Array.from(bytes);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer));
  let hex = "";
  for (const byte of digest) hex += byte.toString(16).padStart(2, "0");
  return hex;
}

function expectExactBytes(
  actual: Uint8Array,
  expected: Uint8Array,
  path: string
): void {
  expect(actual.byteLength, `${path} byte length`).toBe(expected.byteLength);
  for (let index = 0; index < expected.byteLength; index++) {
    if (actual[index] !== expected[index]) {
      throw new Error(
        `${path} byte mismatch at ${index}: expected ${expected[index]}, got ${actual[index]}`
      );
    }
  }
}

async function expectFileBytes(
  actual: Uint8Array,
  expected: ModelFile,
  path: string
): Promise<void> {
  expectExactBytes(actual, expected.bytes, path);
  expect(await sha256(actual), `${path} SHA-256`).toBe(expected.hash);
}

async function expectErrorCode(
  operation: () => Promise<unknown>,
  code: string
): Promise<void> {
  const error = await operation().then(
    () => null,
    (reason: unknown) => reason
  );
  expect(error).toMatchObject({ code });
}

async function executeCommand(
  vfs: ReturnType<typeof createVFS>,
  model: Model,
  command: Command
): Promise<void> {
  switch (command.kind) {
    case "write":
    case "overwrite": {
      const bytes = Uint8Array.from(command.bytes);
      const hash = await sha256(bytes);
      await vfs.writeFile(command.path, Uint8Array.from(bytes));
      model.set(command.path, { bytes, hash });
      return;
    }
    case "rename": {
      const source = model.get(command.src);
      if (!source) {
        await expectErrorCode(
          () => vfs.rename(command.src, command.dst),
          "ENOENT"
        );
        return;
      }
      await vfs.rename(command.src, command.dst);
      model.delete(command.src);
      model.set(command.dst, source);
      return;
    }
    case "unlink":
      if (!model.has(command.path)) {
        await expectErrorCode(() => vfs.unlink(command.path), "ENOENT");
        return;
      }
      await vfs.unlink(command.path);
      model.delete(command.path);
      return;
    case "read": {
      const expected = model.get(command.path);
      if (!expected) {
        await expectErrorCode(() => vfs.readFile(command.path), "ENOENT");
        return;
      }
      await expectFileBytes(await vfs.readFile(command.path), expected, command.path);
      return;
    }
    case "exists":
      expect(await vfs.exists(command.path)).toBe(model.has(command.path));
      return;
    case "stat": {
      const expected = model.get(command.path);
      if (!expected) {
        await expectErrorCode(() => vfs.stat(command.path), "ENOENT");
        return;
      }
      const stat = await vfs.stat(command.path);
      expect(stat.type).toBe("file");
      expect(stat.isFile()).toBe(true);
      expect(stat.size).toBe(expected.bytes.byteLength);
      return;
    }
    case "list":
      await Promise.all([
        vfs.readdir("/"),
        vfs.listFiles({ orderBy: "name", direction: "asc" }),
      ]);
  }
}

async function assertObservableState(
  vfs: ReturnType<typeof createVFS>,
  model: Model
): Promise<void> {
  const [existence, stats, contents, directory, listing] = await Promise.all([
    Promise.all(PATHS.map((path) => vfs.exists(path))),
    vfs.readManyStat([...PATHS]),
    vfs.readManyFile([...PATHS]),
    vfs.readdir("/"),
    vfs.listFiles({ orderBy: "name", direction: "asc", limit: 100 }),
  ]);
  const expectedPaths = [...model.keys()].sort();

  expect(existence).toEqual(PATHS.map((path) => model.has(path)));
  expect(directory).toEqual(expectedPaths.map((path) => path.slice(1)));
  expect(listing.items.map((item) => item.path)).toEqual(expectedPaths);
  expect(listing.cursor).toBeUndefined();

  for (let index = 0; index < PATHS.length; index++) {
    const path = PATHS[index];
    const expected = model.get(path);
    const stat = stats[index];
    const content = contents[index];
    if (!expected) {
      expect(stat, `${path} missing stat`).toBeNull();
      expect(content, `${path} missing content`).toBeNull();
      continue;
    }

    expect(stat, `${path} stat`).not.toBeNull();
    expect(stat?.type).toBe("file");
    expect(stat?.isFile()).toBe(true);
    expect(stat?.size).toBe(expected.bytes.byteLength);
    if (content === null) throw new Error(`${path} content is missing`);
    await expectFileBytes(content, expected, path);

    const listed = listing.items.find((item) => item.path === path);
    expect(listed, `${path} listFiles item`).toBeDefined();
    expect(listed?.stat?.type).toBe("file");
    expect(listed?.stat?.size).toBe(expected.bytes.byteLength);
  }
}

async function readStorageAccounting(tenant: string): Promise<StorageAccounting> {
  const stub = E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
  );
  return runInDurableObject(stub, async (_instance, state) => {
    const quota = state.storage.sql
      .exec(
        `SELECT storage_used, file_count,
                COALESCE(inline_bytes_used, 0) AS inline_bytes_used
           FROM quota WHERE user_id = ?`,
        tenant
      )
      .toArray()[0] as
      | {
          storage_used: number;
          file_count: number;
          inline_bytes_used: number;
        }
      | undefined;
    const uploadingFiles = (
      state.storage.sql
        .exec(
          "SELECT COUNT(*) AS count FROM files WHERE user_id = ? AND status = 'uploading'",
          tenant
        )
        .toArray()[0] as { count: number }
    ).count;
    const cleanupIntents = (
      state.storage.sql
        .exec("SELECT COUNT(*) AS count FROM chunk_cleanup_intents")
        .toArray()[0] as { count: number }
    ).count;
    return {
      storageUsed: quota?.storage_used ?? 0,
      fileCount: quota?.file_count ?? 0,
      inlineBytesUsed: quota?.inline_bytes_used ?? 0,
      uploadingFiles,
      cleanupIntents,
    };
  });
}

async function assertStorageAccounting(
  tenant: string,
  model: Model
): Promise<void> {
  const files = [...model.values()];
  expect(await readStorageAccounting(tenant)).toEqual({
    storageUsed: files.reduce(
      (total, file) => total + file.bytes.byteLength,
      0
    ),
    fileCount: model.size,
    inlineBytesUsed: files.reduce(
      (total, file) =>
        total +
        (file.bytes.byteLength <= INLINE_LIMIT ? file.bytes.byteLength : 0),
      0
    ),
    uploadingFiles: 0,
    cleanupIntents: 0,
  });
}

function isMutation(command: Command): boolean {
  return (
    command.kind === "write" ||
    command.kind === "overwrite" ||
    command.kind === "rename" ||
    command.kind === "unlink"
  );
}

async function readStoredChunkCount(
  tenant: string,
  fileName: string
): Promise<{ declared: number; routed: number }> {
  const stub = E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
  );
  return runInDurableObject(stub, async (_instance, state) => {
    const file = state.storage.sql
      .exec(
        `SELECT file_id, chunk_count FROM files
          WHERE user_id = ? AND file_name = ? AND status = 'complete'`,
        tenant,
        fileName
      )
      .toArray()[0] as { file_id: string; chunk_count: number };
    const routed = (
      state.storage.sql
        .exec(
          "SELECT COUNT(*) AS count FROM file_chunks WHERE file_id = ?",
          file.file_id
        )
        .toArray()[0] as { count: number }
    ).count;
    return { declared: file.chunk_count, routed };
  });
}

function commandDescription(command: Command): string {
  switch (command.kind) {
    case "write":
    case "overwrite":
      return `${command.kind}(${command.path}, ${command.bytes.byteLength} bytes)`;
    case "rename":
      return `rename(${command.src}, ${command.dst})`;
    case "unlink":
    case "read":
    case "exists":
    case "stat":
      return `${command.kind}(${command.path})`;
    case "list":
      return "list()";
  }
}

describe("VFS model-based state machine", () => {
  it(
    "matches an independent model across generated mutation and observation sequences",
    { timeout: 120_000 },
    async () => {
      const commandKinds = new Set<Command["kind"]>();
      const payloadSizes = new Set<number>();
      const missingErrors = new Set<"read" | "rename" | "unlink">();

      for (let run = 0; run < RUN_COUNT; run++) {
        const seed = seedForRun(run);
        const random = randomFor(seed);
        const tenant = `vfs-model-${formatSeed(seed).slice(2)}`;
        const vfs = createVFS(envFor(), { tenant });
        const model: Model = new Map();
        const initialPath = pickPath(random);
        const history: string[] = [];
        let step = -1;

        try {
          for (step = 0; step < COMMAND_COUNT; step++) {
            const command = generateCommand(
              step,
              run,
              initialPath,
              model,
              random
            );
            commandKinds.add(command.kind);
            if (command.kind === "write" || command.kind === "overwrite") {
              payloadSizes.add(command.bytes.byteLength);
            } else if (command.kind === "rename" && !model.has(command.src)) {
              missingErrors.add("rename");
            } else if (command.kind === "unlink" && !model.has(command.path)) {
              missingErrors.add("unlink");
            } else if (command.kind === "read" && !model.has(command.path)) {
              missingErrors.add("read");
            }
            history.push(commandDescription(command));
            await executeCommand(vfs, model, command);
            await assertObservableState(vfs, model);
            if (isMutation(command)) {
              await assertStorageAccounting(tenant, model);
            }
          }

          await assertStorageAccounting(tenant, model);
        } catch (error) {
          const sequence = history
            .map((command, index) => `  ${index}: ${command}`)
            .join("\n");
          throw new Error(
            `VFS state-machine failure: seed=${formatSeed(seed)}, run=${run}, step=${step}\n${sequence}`,
            { cause: error }
          );
        }
      }

      expect([...commandKinds].sort()).toEqual([
        "exists",
        "list",
        "overwrite",
        "read",
        "rename",
        "stat",
        "unlink",
        "write",
      ]);
      expect([...payloadSizes].sort((a, b) => a - b)).toEqual(PAYLOAD_SIZES);
      expect([...missingErrors].sort()).toEqual(["read", "rename", "unlink"]);
    }
  );

  it(
    "tracks every counter through a deterministic multi-chunk mutation sequence",
    { timeout: 30_000 },
    async () => {
      const tenant = "vfs-model-multi-chunk";
      const vfs = createVFS(envFor(), { tenant });
      const model: Model = new Map();
      const commands: Command[] = [
        {
          kind: "write",
          path: "/alpha.bin",
          bytes: payload(MULTI_CHUNK_SIZE, 0x101),
        },
        {
          kind: "write",
          path: "/beta.bin",
          bytes: payload(2_048, 0x202),
        },
        { kind: "rename", src: "/alpha.bin", dst: "/beta.bin" },
        {
          kind: "overwrite",
          path: "/beta.bin",
          bytes: payload(4_096, 0x303),
        },
        { kind: "unlink", path: "/beta.bin" },
      ];

      for (const [index, command] of commands.entries()) {
        await executeCommand(vfs, model, command);
        await assertObservableState(vfs, model);
        await assertStorageAccounting(tenant, model);
        if (index === 0) {
          expect(await readStoredChunkCount(tenant, "alpha.bin")).toEqual({
            declared: 2,
            routed: 2,
          });
        }
      }
    }
  );
});
