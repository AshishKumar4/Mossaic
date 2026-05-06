/**
 * Minimal Worker entry for the vitest pool harness.
 *
 * The vitest-pool-workers harness uses this as the worker bundle target;
 * the actual DOs live in worker/objects. SELF.fetch is unused by tests —
 * tests drive the DOs directly via env.USER_DO.get(...) etc.
 */
export { UserDO } from "../worker/objects/user/index";
export { ShardDO } from "../worker/objects/shard/index";

export default {
  async fetch(): Promise<Response> {
    return new Response("test-worker");
  },
};
