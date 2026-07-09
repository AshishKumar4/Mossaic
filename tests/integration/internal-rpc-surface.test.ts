import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import type { UserDO } from "@app/objects/user/user-do";
import { transactionSync } from "@core/objects/user/internal-storage";

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}

type UnknownRpcStub = Record<string, (...args: never[]) => Promise<unknown>>;

describe("UserDO RPC surface", () => {
  it("does not expose internal storage helpers on remote stubs", async () => {
    const testEnv = env as unknown as TestEnv;
    const stub = testEnv.MOSSAIC_USER.get(
      testEnv.MOSSAIC_USER.idFromName("internal-rpc-surface")
    ) as unknown as UnknownRpcStub;
    const internalMethods = [
      "lastSqlChanges",
      "runWithConcurrencyBlocked",
      "scheduleChunkCleanupSweep",
      "scheduleStaleUploadSweep",
      "stageChunkCleanupIntent",
      "transactionSync",
    ] as const;
    const internalCapabilities = ["state", "storage"] as const;

    for (const method of [...internalMethods, ...internalCapabilities]) {
      await expect(stub[method]!()).rejects.toThrow(
        /does not implement|not a function/i
      );
    }
  });

  it("rejects Promise-returning transaction callbacks", async () => {
    const testEnv = env as unknown as TestEnv;
    const stub = testEnv.MOSSAIC_USER.get(
      testEnv.MOSSAIC_USER.idFromName("internal-transaction-callback")
    );

    await runInDurableObject(stub, (instance) => {
      const unsafeTransaction = transactionSync as unknown as (
        durableObject: UserDO,
        closure: () => unknown
      ) => unknown;

      expect(() =>
        unsafeTransaction(instance, () => Promise.resolve())
      ).toThrow("transactionSync callback must be synchronous");
    });
  });
});
