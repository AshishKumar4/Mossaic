/**
 * Placement resolver.
 *
 * Maps a `VFSScope` to its placement strategy. **Server-authoritative**;
 * clients cannot influence this. Every server-side site that previously
 * called `vfsShardDOName(scope.ns, scope.tenant, scope.sub, idx)`
 * directly now calls `getPlacement(scope).shardDOName(scope, idx)` to
 * route through this lookup.
 *
 * **Design (revised from plan §1.3 Strategy A):** the resolver
 * unconditionally returns `canonicalPlacement` for every canonical
 * code path. The 27 `vfsShardDOName` sites in `worker/core/` are
 * canonical-by-construction — they were already using the canonical
 * template before the abstraction landed; the only purpose of routing through
 * `getPlacement(scope)` is to make the abstraction available for
 * advanced consumers who want to wire a custom placement at the SDK
 * boundary (e.g. tests, multi-tenant deployments with custom DO
 * naming).
 *
 * The legacy App at `mossaic.ashishkumarsingh.com` does NOT enter
 * any of these canonical sites. Its routes (`worker/app/routes/*.ts`)
 * call `legacyAppPlacement.{shardDOName,placeChunk,userDOName}`
 * **explicitly**. This is a clean two-namespace partition with no
 * overlap and no heuristic guessing.
 *
 * **Why no heuristic.** An earlier draft of this plan (§1.3) proposed
 * a structural heuristic of `ns === "default" && sub === undefined →
 * legacy`. That collides with the in-tree integration tests, which
 * use exactly that scope shape on the canonical surface. Since the
 * App routes never enter the canonical sites, there is no need to
 * disambiguate at the resolver — the dispatch is already
 * unambiguous at the call-site level.
 *
 * **Future extensibility.** A later phase may promote this to a
 * config-driven mechanism (`env.PLACEMENT_RULES`) for multi-app
 * deployments. The signature stays the same; only the body changes.
 *
 * **Score-template invariance:** both `canonicalPlacement` and
 * `legacyAppPlacement` use the same rendezvous score key
 * (`shard:${userId}:${idx}`) regardless of which one is selected.
 * Only the resulting *DO instance name* differs. See
 * `shared/placement.ts:placeChunk` and §1.4.
 */

import {
  canonicalPlacement,
  type Placement,
} from "../../../shared/placement";
import type { VFSScope } from "../../../shared/vfs-types";

/**
 * Map a VFSScope to its placement strategy.
 *
 * @param scope The VFS scope being served by the current request.
 *   (Unused in v1 — the resolver returns `canonicalPlacement`
 *   unconditionally because the canonical sites never serve App
 *   routes.) Kept in the signature so future config-driven
 *   dispatch can branch without changing call sites.
 * @returns The placement implementation that owns the chunk
 *   addressing for this scope.
 */
export function getPlacement(_scope: VFSScope): Placement {
  return canonicalPlacement;
}
