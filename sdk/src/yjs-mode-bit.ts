/**
 * Pure constant module — split out from `./yjs.ts` so the main SDK
 * bundle (`@mossaic/sdk`) can re-export `VFS_MODE_YJS_BIT` without
 * dragging in the optional `yjs` peer dep.
 *
 * The constant matches `VFS_MODE_YJS_BIT` in
 * `worker/objects/user/yjs.ts`. Equal to the POSIX setuid bit
 * (0o4000) — Mossaic does not enforce setuid semantics, so we
 * repurpose the slot.
 */
export const VFS_MODE_YJS_BIT = 0o4000;
