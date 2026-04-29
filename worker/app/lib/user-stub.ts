/**
 * Phase 17 — typed UserDO stub helper.
 *
 * App routes use this helper to obtain a `UserDO` stub from the
 * `MOSSAIC_USER` binding with the App's typed RPC surface
 * (`appCreateFile`, `appListFiles`, etc.) visible to TypeScript.
 *
 * Why a helper: `EnvApp.MOSSAIC_USER` is typed as the bare
 * `DurableObjectNamespace` (no generic) so the SDK's structural
 * `MossaicEnv` shape stays compatible with the consumer-side
 * binding. The cast here narrows it to the App's concrete `UserDO`
 * once at the route boundary, instead of every call site.
 */

import type { EnvApp } from "@shared/types";
import type { UserDO } from "../objects/user/user-do";
import { userDOName } from "@core/lib/utils";

/** Resolve a `UserDO` stub for a given userId, with typed RPC methods. */
export function userStub(env: EnvApp, userId: string): DurableObjectStub<UserDO> {
  const ns = env.MOSSAIC_USER as unknown as DurableObjectNamespace<UserDO>;
  return ns.get(ns.idFromName(userDOName(userId)));
}

/**
 * Resolve a `UserDO` stub by an arbitrary DO name (e.g. `auth:<email>`
 * for the signup/login routing). Public for the auth routes; other
 * code should prefer {@link userStub} keyed by userId.
 */
export function userStubByName(
  env: EnvApp,
  name: string
): DurableObjectStub<UserDO> {
  const ns = env.MOSSAIC_USER as unknown as DurableObjectNamespace<UserDO>;
  return ns.get(ns.idFromName(name));
}
