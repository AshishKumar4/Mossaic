/**
 * Test Worker entry — re-exports the production Hono app so that
 * `SELF.fetch` (vitest-pool-workers) drives the *real* request pipeline
 * end-to-end, including all /api/* routes.
 *
 * The DO classes (UserDO, ShardDO, SearchDO) are re-exported for
 * wrangler binding resolution; the test wrangler.test.jsonc declares
 * matching bindings.
 *
 * Existing DO-direct tests (vfs-read, vfs-write, streaming, etc.) drive
 * the DOs via `env.MOSSAIC_USER.get(idFromName(...))` and never go through
 * SELF.fetch — those tests are unaffected by the entrypoint change.
 *
 * The Worker-boot smoke test (tests/integration/worker-smoke.test.ts)
 * uses `SELF.fetch("https://test/api/...")` to exercise the real route
 * handlers, providing an end-to-end regression gate.
 *
 * the worker entry now lives at `worker/app/index.ts` (the
 * App-mode bundle). DO re-exports point to the new layout. Class names
 * are unchanged so existing test bindings (`class_name: "UserDO"`)
 * continue to resolve.
 *
 * SearchDO moved from worker/core/objects/search/ to
 * worker/app/objects/search/ (App-only — backs the photo-library's
 * /api/search route, not part of the SDK contract).
 */
export { default } from "../worker/app/index";
export { UserDO } from "../worker/app/objects/user/index";
export { ShardDO } from "../worker/core/objects/shard/index";
export { SearchDO } from "../worker/app/objects/search/index";
