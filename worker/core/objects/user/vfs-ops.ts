// Phase 19 — barrel re-export. The implementation moved to
// `vfs/{helpers,reads,write-commit,mutations,metadata,streams}.ts`.
// External callers (user-do-core.ts, multipart-upload.ts,
// copy-file.ts) continue to import from this path; the barrel
// preserves the public surface byte-for-byte.
//
// Audit S1 #7: this file was a 3,541-line monolith mixing 8 concerns
// (stat-reads, file-reads, top-level write, mutations, metadata,
// tree ops, read-streams, write-streams). The split is a pure
// refactor — every function body moved verbatim; only import
// statements changed.

export {
  userIdFor,
  resolveOrThrow,
  statForResolved,
  resolveParent,
  poolSizeFor,
  folderExists,
  findLiveFile,
  type ResolvedHit,
} from "./vfs/helpers";

export {
  vfsStat,
  vfsLstat,
  vfsExists,
  vfsReadlink,
  vfsReaddir,
  vfsReadManyStat,
  vfsReadFile,
  vfsOpenManifest,
  vfsReadChunk,
} from "./vfs/reads";

export {
  hardDeleteFileRow,
  vfsWriteFile,
  commitRename,
  abortTempFile,
  type VFSWriteFileOpts,
} from "./vfs/write-commit";

export {
  vfsUnlink,
  vfsMkdir,
  vfsRmdir,
  vfsRename,
  vfsSymlink,
  vfsRemoveRecursive,
} from "./vfs/mutations";

export {
  vfsChmod,
  vfsPatchMetadata,
  vfsSetYjsMode,
  isYjsMode,
} from "./vfs/metadata";

export {
  vfsOpenReadStream,
  vfsPullReadStream,
  vfsCreateReadStream,
  vfsBeginWriteStream,
  vfsAppendWriteStream,
  vfsCommitWriteStream,
  vfsAbortWriteStream,
  vfsCreateWriteStream,
  type VFSReadHandle,
  type VFSWriteHandle,
} from "./vfs/streams";
