import { describe, expect, it } from "vitest";

import { isExpectedWorkerdRpcRejectionMirror } from "../../vitest-unhandled-errors";

const RPC_WRAPPER_FRAME =
  "at invoke (@cloudflare/vitest-pool-workers/dist/worker/lib/cloudflare/test-internal.mjs:374:47)";

function remoteError(message: string, stack = RPC_WRAPPER_FRAME): Error {
  const error = Object.assign(new Error(message), { remote: true });
  error.stack = `${error.name}: ${error.message}\n    ${stack}`;
  return error;
}

describe("Vitest unhandled error filtering", () => {
  it("ignores only documented Workers RPC mirror signatures", () => {
    expect(
      isExpectedWorkerdRpcRejectionMirror(
        remoteError("VFSError: ENOENT: expected rejection")
      )
    ).toBe(true);
    expect(
      isExpectedWorkerdRpcRejectionMirror(new Error("expected rejection"))
    ).toBe(false);
    expect(
      isExpectedWorkerdRpcRejectionMirror(
        remoteError(
          'The RPC receiver does not implement "transactionSync".',
          "at application (worker/index.ts:1:1)"
        )
      )
    ).toBe(true);
  });

  it("does not hide unexplained network or remote failures", () => {
    expect(
      isExpectedWorkerdRpcRejectionMirror(remoteError("Network connection lost."))
    ).toBe(false);
    expect(
      isExpectedWorkerdRpcRejectionMirror(
        Object.assign(new Error("unexpected remote failure"), { remote: true })
      )
    ).toBe(false);
    expect(
      isExpectedWorkerdRpcRejectionMirror(
        remoteError("unexpected remote failure")
      )
    ).toBe(false);
    expect(
      isExpectedWorkerdRpcRejectionMirror(
        Object.assign(remoteError("connection reset"), { code: "ECONNRESET" })
      )
    ).toBe(false);
  });
});
