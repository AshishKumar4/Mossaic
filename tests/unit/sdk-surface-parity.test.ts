import { describe, it, expect } from "vitest";

/**
 * Surface parity between `@mossaic/sdk` (root) and `@mossaic/sdk/http`.
 *
 * Background: the two entry points were drifting — the root entry
 * exported `EBADF`, `ENOTSUP`, encryption types, and the `Placement`
 * abstraction; the `/http` entry did not. CLI consumers import from
 * `@mossaic/sdk/http` (Node-safe — no `cloudflare:workers` virtual)
 * and could not do `instanceof EBADF` even though the server throws
 * that code on encryption-mode mismatch.
 *
 * The two entries diverge ONLY in DO-class re-exports and the
 * `createVFS` factory (both Worker-runtime-only). Every other
 * value/type export is mirrored.
 */

import * as sdk from "../../sdk/src/index";
import * as sdkHttp from "../../sdk/src/http-only";

describe("SDK surface parity — `@mossaic/sdk` vs `@mossaic/sdk/http`", () => {
  it("error classes — every error in the root entry is also in /http", () => {
    const errorClassNames = [
      "VFSFsError",
      "ENOENT",
      "EEXIST",
      "EISDIR",
      "ENOTDIR",
      "EFBIG",
      "ELOOP",
      "EBUSY",
      "EINVAL",
      "EACCES",
      "EROFS",
      "ENOTEMPTY",
      "EAGAIN",
      "EBADF",
      "ENOTSUP",
      "MossaicUnavailableError",
    ] as const;
    for (const name of errorClassNames) {
      expect(name in sdk, `${name} missing from @mossaic/sdk`).toBe(true);
      expect(name in sdkHttp, `${name} missing from @mossaic/sdk/http`).toBe(
        true
      );
      // Must be the SAME class identity (not a copy) — instanceof
      // checks across entry points must work.
      expect(
        (sdk as unknown as Record<string, unknown>)[name],
        `${name} identity mismatch`
      ).toBe((sdkHttp as unknown as Record<string, unknown>)[name]);
    }
  });

  it("error helpers — isLikelyUnavailable is shared", () => {
    expect(sdk.isLikelyUnavailable).toBe(sdkHttp.isLikelyUnavailable);
  });

  it("placement abstraction — canonicalPlacement is shared", () => {
    expect(sdk.canonicalPlacement).toBe(sdkHttp.canonicalPlacement);
  });

  it("transfer engine — parallelUpload/parallelDownload are shared", () => {
    expect(sdk.parallelUpload).toBe(sdkHttp.parallelUpload);
    expect(sdk.parallelDownload).toBe(sdkHttp.parallelDownload);
    expect(sdk.parallelDownloadStream).toBe(sdkHttp.parallelDownloadStream);
    expect(sdk.beginUpload).toBe(sdkHttp.beginUpload);
    expect(sdk.putChunk).toBe(sdkHttp.putChunk);
    expect(sdk.finalizeUpload).toBe(sdkHttp.finalizeUpload);
    expect(sdk.abortUpload).toBe(sdkHttp.abortUpload);
    expect(sdk.statusUpload).toBe(sdkHttp.statusUpload);
    expect(sdk.deriveClientChunkSpec).toBe(sdkHttp.deriveClientChunkSpec);
    expect(sdk.THROUGHPUT_MATH).toBe(sdkHttp.THROUGHPUT_MATH);
  });

  it("constants — VFS_MODE_YJS_BIT, AIMDController, hashChunk match", () => {
    expect(sdk.VFS_MODE_YJS_BIT).toBe(sdkHttp.VFS_MODE_YJS_BIT);
    expect(sdk.AIMDController).toBe(sdkHttp.AIMDController);
    expect(sdk.hashChunk).toBe(sdkHttp.hashChunk);
    expect(sdk.computeFileHash).toBe(sdkHttp.computeFileHash);
    expect(sdk.computeChunkSpec).toBe(sdkHttp.computeChunkSpec);
  });

  it("HTTP client surface — createMossaicHttpClient + HttpVFS shared", () => {
    expect(sdk.createMossaicHttpClient).toBe(sdkHttp.createMossaicHttpClient);
    expect(sdk.HttpVFS).toBe(sdkHttp.HttpVFS);
  });

  it("token issuance helpers shared", () => {
    expect(sdk.issueVFSToken).toBe(sdkHttp.issueVFSToken);
    expect(sdk.verifyVFSToken).toBe(sdkHttp.verifyVFSToken);
  });

  it("EBADF and ENOTSUP can be instanceof-checked from /http", () => {
    // The root-entry test we don't need; the bug was that /http
    // didn't expose these at all. With them re-exported, an error
    // thrown anywhere in the SDK can be discriminated:
    const e = new sdkHttp.EBADF({ syscall: "open", path: "/x" });
    expect(e).toBeInstanceOf(sdkHttp.EBADF);
    expect(e).toBeInstanceOf(sdkHttp.VFSFsError);
    expect(e.code).toBe("EBADF");
    // Cross-entry instanceof must still work.
    expect(e).toBeInstanceOf(sdk.EBADF);

    const n = new sdkHttp.ENOTSUP({ syscall: "chmod", path: "/y" });
    expect(n).toBeInstanceOf(sdkHttp.ENOTSUP);
    expect(n.code).toBe("ENOTSUP");
    expect(n).toBeInstanceOf(sdk.ENOTSUP);
  });
});
