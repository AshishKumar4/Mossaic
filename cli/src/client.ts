/**
 * Client builder — produces an `HttpVFS` configured for the resolved
 * profile, plus a freshly-minted token.
 */

import { createMossaicHttpClient, type HttpVFS } from "@mossaic/sdk/http";
import type { Profile } from "./config.js";
import { mintToken } from "./jwt.js";

export interface BuiltClient {
  vfs: HttpVFS;
  token: string;
  endpoint: string;
  scope: { ns: string; tenant: string; sub: string | null };
}

export async function buildClient(profile: Profile): Promise<BuiltClient> {
  const token = await mintToken({
    secret: profile.jwtSecret,
    ns: profile.scope.ns,
    tenant: profile.scope.tenant,
    sub: profile.scope.sub ?? undefined,
  });
  const vfs = createMossaicHttpClient({
    url: profile.endpoint,
    apiKey: token,
  });
  return {
    vfs,
    token,
    endpoint: profile.endpoint,
    scope: { ...profile.scope },
  };
}
