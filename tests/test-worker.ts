/**
 * Test Worker entry — re-exports the production Hono app so that
 * `SELF.fetch` (vitest-pool-workers) drives the *real* request pipeline
 * end-to-end, including all /api/* routes.
 *
 * The ordinary suite binds the production DO classes. A separate fault
 * project binds the test-only subclasses exported below.
 *
 * Existing DO-direct tests (vfs-read, vfs-write, streaming, etc.) drive
 * the DOs via `env.MOSSAIC_USER.get(idFromName(...))` and never go through
 * SELF.fetch — those tests are unaffected by the entrypoint change.
 *
 * The Worker-boot smoke test (tests/integration/worker-smoke.test.ts)
 * uses `SELF.fetch("https://test/api/...")` to exercise the real route
 * handlers, providing an end-to-end regression gate.
 *
 * the worker entry now lives at `worker/app/index.ts` (the
 * App-mode bundle). DO re-exports point to the new layout. Class names
 * are unchanged so existing test bindings (`class_name: "UserDO"`)
 * continue to resolve.
 *
 * SearchDO moved from worker/core/objects/search/ to
 * worker/app/objects/search/ (App-only — backs the photo-library's
 * /api/search route, not part of the SDK contract).
 */
import { ShardDO } from "../worker/core/objects/shard/index";
import { UserDO } from "../worker/app/objects/user/index";
import type { EnvCore } from "../shared/types";
import type { VFSScope } from "../shared/vfs-types";

export { default } from "../worker/app/index";
export { SearchDO } from "../worker/app/objects/search/index";
export { ShardDO, UserDO };

export type DeleteChunksFailurePhase = "before" | "after";
export type DeleteManyChunksFailurePhase = "before" | "mid" | "after";
export type ClearMultipartStagingFailurePhase = "before" | "after";
export type PutChunkFailurePhase = "before" | "after";

export class FaultInjectingUserDO extends UserDO {
  private maintenanceAlarmFailuresRemaining = 0;

  constructor(ctx: DurableObjectState, env: EnvCore) {
    super(ctx, env);
    const storage = this.storage;
    this.storage = new Proxy(storage, {
      get: (target, property) => {
        if (property === "setAlarm") {
          return async (
            ...args: Parameters<DurableObjectStorage["setAlarm"]>
          ): Promise<void> => {
            if (this.maintenanceAlarmFailuresRemaining > 0) {
              this.maintenanceAlarmFailuresRemaining--;
              throw new Error("injected maintenance alarm failure");
            }
            await target.setAlarm(...args);
          };
        }
        const value = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
  }

  async testConfigureMaintenanceAlarmFailure(remaining: number): Promise<void> {
    await this.storage.deleteAlarm();
    this.maintenanceAlarmFailuresRemaining = remaining;
  }

  async testEvict(): Promise<void> {
    this.state.abort("injected UserDO eviction");
  }

  async testDropVersionRows(
    scope: VFSScope,
    userId: string,
    pathId: string,
    versionIds: string[]
  ): Promise<number> {
    const { dropVersionRows } = await import(
      "../worker/core/objects/user/vfs-versions"
    );
    return dropVersionRows(this, scope, userId, pathId, versionIds);
  }
}

export class FaultInjectingShardDO extends ShardDO {
  private putChunkFailure:
    | {
        phase: PutChunkFailurePhase;
        remaining: number | null;
      }
    | undefined;
  private putChunkBlock:
    | {
        entered: Promise<void>;
        markEntered: () => void;
        release: Promise<void>;
        unblock: () => void;
        claimed: boolean;
      }
    | undefined;
  private restoreChunkRefBlock:
    | {
        entered: Promise<void>;
        markEntered: () => void;
        release: Promise<void>;
        unblock: () => void;
      }
    | undefined;
  private multipartManifestBlock:
    | {
        uploadId: string;
        entered: Promise<void>;
        markEntered: () => void;
        release: Promise<void>;
        unblock: () => void;
      }
    | undefined;
  private deleteChunksFailure:
    | {
        fileId: string;
        phase: DeleteChunksFailurePhase;
        remaining: number | null;
      }
    | undefined;
  private deleteManyChunksFailure:
    | {
        phase: DeleteManyChunksFailurePhase;
        remaining: number | null;
      }
    | undefined;
  private clearMultipartStagingFailure:
    | {
        phase: ClearMultipartStagingFailurePhase;
        remaining: number | null;
      }
    | undefined;
  private deleteChunksBlock:
    | {
        fileId: string;
        entered: Promise<void>;
        markEntered: () => void;
        release: Promise<void>;
        unblock: () => void;
      }
    | undefined;
  private deleteChunksConcurrencyProbe:
    | {
        active: number;
        maxActive: number;
        target: number;
        reached: Promise<void>;
        markReached: () => void;
        release: Promise<void>;
        unblock: () => void;
      }
    | undefined;

  async testConfigurePutChunkFailure(
    phase: PutChunkFailurePhase,
    remaining: number | null
  ): Promise<void> {
    this.putChunkFailure = { phase, remaining };
  }

  async testClearPutChunkFailure(): Promise<void> {
    this.putChunkFailure = undefined;
  }

  async testConfigurePutChunkBlock(): Promise<void> {
    let markEntered = (): void => {};
    let unblock = (): void => {};
    const entered = new Promise<void>((resolve) => {
      markEntered = resolve;
    });
    const release = new Promise<void>((resolve) => {
      unblock = resolve;
    });
    this.putChunkBlock = {
      entered,
      markEntered,
      release,
      unblock,
      claimed: false,
    };
  }

  async testWaitForPutChunkBlocked(): Promise<void> {
    const block = this.putChunkBlock;
    if (!block) throw new Error("putChunk block is not configured");
    await block.entered;
  }

  async testReleasePutChunkBlock(): Promise<void> {
    const block = this.putChunkBlock;
    if (!block) throw new Error("putChunk block is not configured");
    block.unblock();
  }

  async testConfigureRestoreChunkRefBlock(): Promise<void> {
    let markEntered = (): void => {};
    let unblock = (): void => {};
    const entered = new Promise<void>((resolve) => {
      markEntered = resolve;
    });
    const release = new Promise<void>((resolve) => {
      unblock = resolve;
    });
    this.restoreChunkRefBlock = { entered, markEntered, release, unblock };
  }

  async testWaitForRestoreChunkRefBlocked(): Promise<void> {
    const block = this.restoreChunkRefBlock;
    if (!block) throw new Error("restoreChunkRef block is not configured");
    await block.entered;
  }

  async testReleaseRestoreChunkRefBlock(): Promise<void> {
    const block = this.restoreChunkRefBlock;
    if (!block) throw new Error("restoreChunkRef block is not configured");
    block.unblock();
  }

  async testConfigureMultipartManifestBlock(uploadId: string): Promise<void> {
    let markEntered = (): void => {};
    let unblock = (): void => {};
    const entered = new Promise<void>((resolve) => {
      markEntered = resolve;
    });
    const release = new Promise<void>((resolve) => {
      unblock = resolve;
    });
    this.multipartManifestBlock = {
      uploadId,
      entered,
      markEntered,
      release,
      unblock,
    };
  }

  async testWaitForMultipartManifestBlocked(): Promise<void> {
    const block = this.multipartManifestBlock;
    if (!block) throw new Error("multipart manifest block is not configured");
    await block.entered;
  }

  async testReleaseMultipartManifestBlock(): Promise<void> {
    const block = this.multipartManifestBlock;
    if (!block) throw new Error("multipart manifest block is not configured");
    block.unblock();
  }

  async testConfigureDeleteChunksFailure(
    fileId: string,
    phase: DeleteChunksFailurePhase,
    remaining: number | null
  ): Promise<void> {
    this.deleteChunksFailure = { fileId, phase, remaining };
  }

  async testConfigureAnyDeleteChunksFailure(
    phase: DeleteChunksFailurePhase,
    remaining: number | null
  ): Promise<void> {
    this.deleteChunksFailure = { fileId: "*", phase, remaining };
  }

  async testClearDeleteChunksFailure(): Promise<void> {
    this.deleteChunksFailure = undefined;
  }

  async testConfigureDeleteManyChunksFailure(
    phase: DeleteManyChunksFailurePhase,
    remaining: number | null
  ): Promise<void> {
    this.deleteManyChunksFailure = { phase, remaining };
  }

  async testClearDeleteManyChunksFailure(): Promise<void> {
    this.deleteManyChunksFailure = undefined;
  }

  async testConfigureClearMultipartStagingFailure(
    phase: ClearMultipartStagingFailurePhase,
    remaining: number | null
  ): Promise<void> {
    this.clearMultipartStagingFailure = { phase, remaining };
  }

  async testClearClearMultipartStagingFailure(): Promise<void> {
    this.clearMultipartStagingFailure = undefined;
  }

  async testConfigureDeleteChunksBlock(fileId: string): Promise<void> {
    let markEntered = (): void => {};
    let unblock = (): void => {};
    const entered = new Promise<void>((resolve) => {
      markEntered = resolve;
    });
    const release = new Promise<void>((resolve) => {
      unblock = resolve;
    });
    this.deleteChunksBlock = {
      fileId,
      entered,
      markEntered,
      release,
      unblock,
    };
  }

  async testWaitForDeleteChunksBlocked(): Promise<void> {
    const block = this.deleteChunksBlock;
    if (!block) throw new Error("deleteChunks block is not configured");
    await block.entered;
  }

  async testReleaseDeleteChunksBlock(): Promise<void> {
    const block = this.deleteChunksBlock;
    if (!block) throw new Error("deleteChunks block is not configured");
    block.unblock();
  }

  async testConfigureDeleteChunksConcurrencyProbe(target: number): Promise<void> {
    let markReached = (): void => {};
    let unblock = (): void => {};
    const reached = new Promise<void>((resolve) => {
      markReached = resolve;
    });
    const release = new Promise<void>((resolve) => {
      unblock = resolve;
    });
    this.deleteChunksConcurrencyProbe = {
      active: 0,
      maxActive: 0,
      target,
      reached,
      markReached,
      release,
      unblock,
    };
  }

  async testWaitForDeleteChunksConcurrency(): Promise<void> {
    const probe = this.deleteChunksConcurrencyProbe;
    if (!probe) throw new Error("deleteChunks concurrency probe is not configured");
    await probe.reached;
  }

  async testReadDeleteChunksMaxConcurrency(): Promise<number> {
    return this.deleteChunksConcurrencyProbe?.maxActive ?? 0;
  }

  async testReleaseDeleteChunksConcurrencyProbe(): Promise<void> {
    const probe = this.deleteChunksConcurrencyProbe;
    if (!probe) throw new Error("deleteChunks concurrency probe is not configured");
    probe.unblock();
  }

  override async putChunk(
    chunkHash: string,
    data: Uint8Array,
    fileId: string,
    chunkIndex: number,
    userId: string
  ): Promise<{ status: "created" | "deduplicated"; bytesStored: number }> {
    const block = this.putChunkBlock;
    if (block && !block.claimed) {
      block.claimed = true;
      block.markEntered();
      await block.release;
      if (this.putChunkBlock === block) {
        this.putChunkBlock = undefined;
      }
    }

    const failure = this.putChunkFailure;
    const shouldFail =
      failure !== undefined &&
      (failure.remaining === null || failure.remaining > 0);

    if (shouldFail && failure.phase === "before") {
      this.consumePutChunkFailure(failure);
      throw new Error(`injected putChunk failure before mutation: ${fileId}`);
    }

    const result = await super.putChunk(
      chunkHash,
      data,
      fileId,
      chunkIndex,
      userId
    );
    if (shouldFail && failure.phase === "after") {
      this.consumePutChunkFailure(failure);
      throw new Error(`injected putChunk response loss after mutation: ${fileId}`);
    }
    return result;
  }

  override async restoreChunkRef(
    chunkHash: string,
    newRefId: string,
    chunkIndex: number,
    userId: string
  ): Promise<{ status: "restored" | "already_referenced" }> {
    const block = this.restoreChunkRefBlock;
    if (block) {
      block.markEntered();
      await block.release;
      if (this.restoreChunkRefBlock === block) {
        this.restoreChunkRefBlock = undefined;
      }
    }
    return super.restoreChunkRef(chunkHash, newRefId, chunkIndex, userId);
  }

  override async getMultipartManifest(
    uploadId: string
  ): Promise<{ rows: Array<{ idx: number; hash: string; size: number }> }> {
    const block = this.multipartManifestBlock;
    if (block?.uploadId === uploadId) {
      block.markEntered();
      await block.release;
      if (this.multipartManifestBlock === block) {
        this.multipartManifestBlock = undefined;
      }
    }
    return super.getMultipartManifest(uploadId);
  }

  override async deleteChunks(fileId: string): Promise<{ marked: number }> {
    const probe = this.deleteChunksConcurrencyProbe;
    if (probe) {
      probe.active++;
      probe.maxActive = Math.max(probe.maxActive, probe.active);
      if (probe.active >= probe.target) probe.markReached();
      await probe.release;
      probe.active--;
    }

    const block = this.deleteChunksBlock;
    if (block?.fileId === fileId) {
      block.markEntered();
      await block.release;
      if (this.deleteChunksBlock === block) {
        this.deleteChunksBlock = undefined;
      }
    }

    const failure = this.deleteChunksFailure;
    const shouldFail =
      (failure?.fileId === fileId || failure?.fileId === "*") &&
      (failure.remaining === null || failure.remaining > 0);

    if (shouldFail && failure.phase === "before") {
      this.consumeDeleteChunksFailure(failure);
      throw new Error(`injected deleteChunks failure before mutation: ${fileId}`);
    }

    const result = await super.deleteChunks(fileId);
    if (shouldFail && failure.phase === "after") {
      this.consumeDeleteChunksFailure(failure);
      throw new Error(`injected deleteChunks response loss after mutation: ${fileId}`);
    }
    return result;
  }

  override async deleteManyChunks(
    fileIds: readonly string[]
  ): Promise<{ marked: number }> {
    const failure = this.deleteManyChunksFailure;
    const shouldFail =
      failure !== undefined &&
      (failure.remaining === null || failure.remaining > 0);
    if (shouldFail && failure.phase === "before") {
      this.consumeDeleteManyChunksFailure(failure);
      throw new Error("injected deleteManyChunks failure before mutation");
    }
    if (shouldFail && failure.phase === "mid") {
      const split = Math.max(1, Math.floor(fileIds.length / 2));
      await super.deleteManyChunks(fileIds.slice(0, split));
      this.consumeDeleteManyChunksFailure(failure);
      throw new Error("injected deleteManyChunks failure mid-batch");
    }

    const result = await super.deleteManyChunks(fileIds);
    if (shouldFail && failure.phase === "after") {
      this.consumeDeleteManyChunksFailure(failure);
      throw new Error("injected deleteManyChunks response loss after mutation");
    }
    return result;
  }

  override async clearMultipartStaging(
    uploadId: string
  ): Promise<{ dropped: number }> {
    const failure = this.clearMultipartStagingFailure;
    const shouldFail =
      failure !== undefined &&
      (failure.remaining === null || failure.remaining > 0);
    if (shouldFail && failure.phase === "before") {
      this.consumeClearMultipartStagingFailure(failure);
      throw new Error(
        `injected clearMultipartStaging failure before mutation: ${uploadId}`
      );
    }

    const result = await super.clearMultipartStaging(uploadId);
    if (shouldFail && failure.phase === "after") {
      this.consumeClearMultipartStagingFailure(failure);
      throw new Error(
        `injected clearMultipartStaging response loss after mutation: ${uploadId}`
      );
    }
    return result;
  }

  private consumeDeleteChunksFailure(
    failure: NonNullable<FaultInjectingShardDO["deleteChunksFailure"]>
  ): void {
    if (failure.remaining === null) return;
    failure.remaining--;
    if (failure.remaining === 0) {
      this.deleteChunksFailure = undefined;
    }
  }

  private consumeDeleteManyChunksFailure(
    failure: NonNullable<FaultInjectingShardDO["deleteManyChunksFailure"]>
  ): void {
    if (failure.remaining === null) return;
    failure.remaining--;
    if (failure.remaining === 0) {
      this.deleteManyChunksFailure = undefined;
    }
  }

  private consumeClearMultipartStagingFailure(
    failure: NonNullable<
      FaultInjectingShardDO["clearMultipartStagingFailure"]
    >
  ): void {
    if (failure.remaining === null) return;
    failure.remaining--;
    if (failure.remaining === 0) {
      this.clearMultipartStagingFailure = undefined;
    }
  }

  private consumePutChunkFailure(
    failure: NonNullable<FaultInjectingShardDO["putChunkFailure"]>
  ): void {
    if (failure.remaining === null) return;
    failure.remaining--;
    if (failure.remaining === 0) {
      this.putChunkFailure = undefined;
    }
  }
}
