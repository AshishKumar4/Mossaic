const WORKER_RPC_WRAPPER_PATH =
  "@cloudflare/vitest-pool-workers/dist/worker/lib/cloudflare/test-internal.mjs";
const VFS_ERROR_CODE =
  "(?:ENOENT|EEXIST|EISDIR|ENOTDIR|ENOTEMPTY|EFBIG|ELOOP|EBUSY|EINVAL|EAGAIN|EBADF|EACCES|ENOTSUP)";
const VFS_ERROR_CODE_ONLY = new RegExp(`^${VFS_ERROR_CODE}$`);
const VFS_ERROR_MESSAGE = new RegExp(`^(?:VFSError: )?${VFS_ERROR_CODE}:`);
const INTERNAL_RPC_SURFACE_ERROR =
  /^The RPC receiver(?:'s prototype)? does not implement "(?:lastSqlChanges|runWithConcurrencyBlocked|scheduleChunkCleanupSweep|scheduleStaleUploadSweep|stageChunkCleanupIntent|transactionSync|state|storage)"(?:\.|,)/;
const INJECTED_TEST_ERROR =
  /^injected (?:initial |chunk |putChunk |deleteChunks |deleteManyChunks |clearMultipartStaging |maintenance alarm |version |publication |session |multipart |stream |copy |UserDO eviction)/;
const SERIALIZED_RPC_LIMIT_ERROR =
  /^Serialized RPC arguments or return values are limited to 32MiB/;

export function isExpectedWorkerdRpcRejectionMirror(error: Error): boolean {
  if (error.message === "Network connection lost.") return false;
  if (
    Reflect.get(error, "durableObjectReset") === true &&
    INJECTED_TEST_ERROR.test(error.message)
  ) {
    return true;
  }

  const code = Reflect.get(error, "code");
  if (typeof code === "string" && VFS_ERROR_CODE_ONLY.test(code)) {
    return true;
  }
  if (code === "VFS_CONFIG_ERROR" || error.name === "AbortError") return true;
  if (SERIALIZED_RPC_LIMIT_ERROR.test(error.message)) return true;

  const stackIsRpcWrapper =
    error.stack?.includes(WORKER_RPC_WRAPPER_PATH) === true;
  const isRemote = Reflect.get(error, "remote") === true;
  if (!stackIsRpcWrapper && !isRemote) return false;

  return (
    VFS_ERROR_MESSAGE.test(error.message) ||
    INTERNAL_RPC_SURFACE_ERROR.test(error.message) ||
    INJECTED_TEST_ERROR.test(error.message) ||
    error.message === "Invalid credentials"
  );
}
