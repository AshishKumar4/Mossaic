/**
 * Phase 12: deep-merge for metadata patches.
 *
 * Pure function. Models the `patchMetadata` semantics:
 *   - The patch is recursively merged with the existing metadata.
 *   - A leaf value of `null` in the patch DELETES that key from the
 *     merged result (tombstone semantics — the only way to remove
 *     a key without replacing the entire blob).
 *   - Arrays are REPLACED, not merged. Concat-merge has no
 *     well-defined semantics across nested keys.
 *   - Primitives in the patch overwrite primitives in the base.
 *   - Objects in the patch are merged recursively.
 *
 * The function is total — given any `base` and `patch` of valid
 * shape, it returns a new object without mutating either input.
 *
 * Modeled in lean/Mossaic/Vfs/Metadata.lean as `mergeMeta`.
 */

type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [k: string]: Json };

export function deepMerge(
  base: Record<string, unknown> | null,
  patch: Record<string, unknown>
): Record<string, unknown> {
  // Treat null base as empty object — patch creates from scratch.
  const out: Record<string, unknown> = base
    ? structuredCloneCompat(base)
    : {};

  for (const key of Object.keys(patch)) {
    const pv = patch[key];
    if (pv === null) {
      // Tombstone: delete the key.
      delete out[key];
      continue;
    }
    if (Array.isArray(pv)) {
      // Replace, don't merge.
      out[key] = structuredCloneCompat(pv as Json[]);
      continue;
    }
    if (typeof pv === "object") {
      const bv = out[key];
      if (bv && typeof bv === "object" && !Array.isArray(bv)) {
        // Recurse.
        out[key] = deepMerge(
          bv as Record<string, unknown>,
          pv as Record<string, unknown>
        );
      } else {
        // Base had a non-object (or no value): replace with a fresh
        // clone of the patch sub-tree.
        out[key] = structuredCloneCompat(pv as Record<string, unknown>);
      }
      continue;
    }
    // Primitive: overwrite.
    out[key] = pv;
  }
  return out;
}

/**
 * Best-effort structured clone for JSON-shaped values. We avoid
 * relying on the `structuredClone` global so this module is
 * loadable in any JS runtime (workerd has it, but tests may run
 * elsewhere).
 */
function structuredCloneCompat<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}
