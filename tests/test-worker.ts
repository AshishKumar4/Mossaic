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
 * the DOs via `env.USER_DO.get(idFromName(...))` and never go through
 * SELF.fetch — those tests are unaffected by the entrypoint change.
 *
 * The Worker-boot smoke test (tests/integration/worker-smoke.test.ts)
 * uses `SELF.fetch("https://test/api/...")` to exercise the real route
 * handlers, providing an end-to-end regression gate.
 */
export { default } from "../worker/index";
export { UserDO } from "../worker/objects/user/index";
export { ShardDO } from "../worker/objects/shard/index";
export { SearchDO } from "../worker/objects/search/index";
