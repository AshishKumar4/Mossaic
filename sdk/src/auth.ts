/**
 * Token issuance — thin re-export of the worker-side `signVFSToken`.
 *
 * Operator-side helper. The SDK doesn't bundle `jose` itself; instead
 * we re-export the worker-side function so a Worker holding the
 * secret signs tokens directly. Clients that don't have `JWT_SECRET`
 * (e.g. browsers) shouldn't call this.
 */

export {
  signVFSToken as issueVFSToken,
  verifyVFSToken,
  type VFSTokenPayload,
} from "../../worker/core/lib/auth";
