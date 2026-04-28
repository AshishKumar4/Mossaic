import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 5 — Consumer fixture acceptance gate.
 *
 * Mounts the consumer fixture (tests/fixtures/consumer/src/index.ts —
 * which is what an actual customer's Worker looks like) and proves
 * the Phase 4/5 architectural promise:
 *
 *     "Each VFS method on the SDK = exactly 1 DO RPC subrequest from
 *      the consumer's perspective, regardless of internal chunk
 *      fan-out inside Mossaic's UserDO."
 *
 * The vitest pool harness only spins up one Worker, so we don't have
 * a separate consumer-Worker invocation to count subrequests on
 * directly. Instead we wrap the `MOSSAIC_USER` DurableObjectNamespace
 * binding in a counting proxy that increments a counter on every
 * `.get(...)` and on every typed-RPC method call against the returned
 * stub. After running the consumer fixture's request handler, the
 * counter MUST equal exactly the number of VFS calls the handler made.
 *
 * For `/read` (which calls `vfs.readFile` once), that's exactly 1
 * consumer-side outbound — the chunk fan-out happens entirely inside
 * the UserDO's invocation and is invisible to the consumer.
 */

import handler, {
  type Env as ConsumerEnv,
} from "../fixtures/consumer/src/index";

interface E {
  USER_DO: DurableObjectNamespace;
}
const E = env as unknown as E;

/**
 * Wrap a DurableObjectNamespace and the stubs it produces in a
 * counting proxy. Each call to `.get(...)` returns a stub whose RPC
 * methods bump `counter.outbound` once per call.
 *
 * `idFromName` is NOT counted (it's a synchronous local hash, not a
 * subrequest). What we count is what would, on a real consumer
 * Worker, register against the per-invocation subrequest budget: one
 * `.get(id).method(args)` call equals one DO RPC subrequest.
 */
function makeCountingNamespace(
  ns: DurableObjectNamespace,
  counter: { outbound: number; methods: string[] }
): DurableObjectNamespace {
  return new Proxy(ns, {
    get(target, prop) {
      if (prop === "get") {
        return (id: DurableObjectId) => {
          const stub = (target as unknown as DurableObjectNamespace).get(id);
          // The DO stub has host-side internal slots; methods called
          // with anything but the stub itself as `this` throw "Illegal
          // invocation". So we don't proxy-wrap the stub; instead we
          // return a plain JS object that, for each typed RPC method,
          // holds a closure that bumps the counter and calls the
          // underlying stub method via `(stub as any).method(...args)`
          // — i.e. a *real* method call on the *real* stub, never
          // `apply()` with a different receiver.
          const stubAny = stub as unknown as Record<
            string,
            (...args: unknown[]) => Promise<unknown>
          >;
          const wrapper: Record<string, unknown> = {};
          for (const key of [
            "vfsStat",
            "vfsLstat",
            "vfsExists",
            "vfsReadlink",
            "vfsReaddir",
            "vfsReadManyStat",
            "vfsReadFile",
            "vfsWriteFile",
            "vfsUnlink",
            "vfsMkdir",
            "vfsRmdir",
            "vfsRemoveRecursive",
            "vfsSymlink",
            "vfsChmod",
            "vfsRename",
            "vfsOpenManifest",
            "vfsReadChunk",
            "vfsCreateReadStream",
            "vfsCreateWriteStream",
            "vfsOpenReadStream",
            "vfsPullReadStream",
            "vfsBeginWriteStream",
            "vfsAppendWriteStream",
            "vfsCommitWriteStream",
            "vfsAbortWriteStream",
          ]) {
            wrapper[key] = (...args: unknown[]) => {
              counter.outbound++;
              counter.methods.push(key);
              // Direct method call on the real stub — this preserves
              // the host-side `this` binding that workerd requires.
              return stubAny[key](...args);
            };
          }
          return wrapper;
        };
      }
      // For all other properties (notably `idFromName`), return the
      // method bound to the underlying namespace so calls made as
      // `proxy.idFromName(name)` don't fail "Illegal invocation"
      // because the proxy is the receiver. Functions get bound to
      // the real `target`; non-function properties pass through.
      const real = Reflect.get(target, prop);
      if (typeof real === "function") {
        return (real as (...args: unknown[]) => unknown).bind(target);
      }
      return real;
    },
  });
}

function makeEnvWithCounter(
  tenant: string,
  counter: { outbound: number; methods: string[] }
): ConsumerEnv {
  return {
    MOSSAIC_USER: makeCountingNamespace(
      E.USER_DO,
      counter
    ) as unknown as ConsumerEnv["MOSSAIC_USER"],
    TEST_TENANT: tenant,
  };
}

describe("Consumer fixture: SDK as-Library DX + subrequest accounting", () => {
  it("vfs.writeFile (small payload) costs exactly 1 outbound DO RPC", async () => {
    const counter = { outbound: 0, methods: [] as string[] };
    const env = makeEnvWithCounter("cf-write", counter);
    const payload = new TextEncoder().encode("hello consumer fixture");
    const res = await handler.fetch(
      new Request("https://test/seed?path=/x.txt", {
        method: "POST",
        body: payload,
      }),
      env
    );
    expect(res.status).toBe(200);
    expect(counter.outbound).toBe(1);
    expect(counter.methods).toEqual(["vfsWriteFile"]);
  });

  it("vfs.readFile (multi-chunk payload) STILL costs exactly 1 outbound from the consumer", async () => {
    const counter = { outbound: 0, methods: [] as string[] };
    const tenant = "cf-read-multichunk";

    // Seed via the SDK directly (not through the consumer fixture) so
    // the seed cost doesn't pollute the readFile counter.
    const seedEnv = makeEnvWithCounter(tenant, {
      outbound: 0,
      methods: [],
    });
    const { createVFS } = await import("../../sdk/src/index");
    const seedVfs = createVFS(seedEnv as never, { tenant });
    // Use a streaming write so the file ends up multi-chunk.
    const handle = await seedVfs.createWriteStreamWithHandle("/big.bin");
    const cs = handle.handle.chunkSize;
    const writer = handle.stream.getWriter();
    for (let i = 0; i < 3; i++) {
      await writer.write(new Uint8Array(cs).fill(0x40 + i));
    }
    await writer.write(new Uint8Array(7).fill(0x43));
    await writer.close();

    // Now drive the consumer fixture's /read endpoint with a fresh
    // counter. The consumer makes ONE outbound (vfsReadFile); the
    // UserDO internally fans out to the ShardDOs (3+ chunks) but
    // those subrequests are billed against Mossaic's invocation
    // budget, not the consumer's — they should NOT appear in
    // counter.outbound.
    const env = makeEnvWithCounter(tenant, counter);
    const res = await handler.fetch(
      new Request("https://test/read?path=/big.bin"),
      env
    );
    expect(res.status).toBe(200);
    const back = new Uint8Array(await res.arrayBuffer());
    expect(back.byteLength).toBe(cs * 3 + 7);
    // The architectural promise.
    expect(counter.outbound).toBe(1);
    expect(counter.methods).toEqual(["vfsReadFile"]);
  });

  it("vfs.stat costs exactly 1 outbound DO RPC", async () => {
    const counter = { outbound: 0, methods: [] as string[] };
    const tenant = "cf-stat";
    // Seed first
    const sEnv = makeEnvWithCounter(tenant, { outbound: 0, methods: [] });
    await handler.fetch(
      new Request("https://test/seed?path=/s.txt", {
        method: "POST",
        body: new TextEncoder().encode("hi"),
      }),
      sEnv
    );

    // Hit /stat and count.
    const env = makeEnvWithCounter(tenant, counter);
    const res = await handler.fetch(
      new Request("https://test/stat?path=/s.txt"),
      env
    );
    expect(res.ok).toBe(true);
    const body = (await res.json()) as {
      size: number;
      isFile: boolean;
    };
    expect(body.size).toBe(2);
    expect(body.isFile).toBe(true);
    expect(counter.outbound).toBe(1);
    expect(counter.methods).toEqual(["vfsStat"]);
  });

  it("vfs.readdir costs exactly 1 outbound DO RPC", async () => {
    const counter = { outbound: 0, methods: [] as string[] };
    const tenant = "cf-readdir";
    const sEnv = makeEnvWithCounter(tenant, { outbound: 0, methods: [] });
    for (const name of ["a", "b", "c"]) {
      await handler.fetch(
        new Request(`https://test/seed?path=/${name}.txt`, {
          method: "POST",
          body: new TextEncoder().encode("x"),
        }),
        sEnv
      );
    }

    const env = makeEnvWithCounter(tenant, counter);
    const res = await handler.fetch(
      new Request("https://test/readdir?path=/"),
      env
    );
    const body = (await res.json()) as { entries: string[] };
    expect(body.entries.sort()).toEqual(["a.txt", "b.txt", "c.txt"]);
    expect(counter.outbound).toBe(1);
    expect(counter.methods).toEqual(["vfsReaddir"]);
  });

  it("404 on unknown path produces zero outbound (no SDK call made)", async () => {
    const counter = { outbound: 0, methods: [] as string[] };
    const env = makeEnvWithCounter("cf-404", counter);
    const res = await handler.fetch(
      new Request("https://test/nonexistent"),
      env
    );
    expect(res.status).toBe(404);
    expect(counter.outbound).toBe(0);
  });

  it("re-exports UserDO + ShardDO classes for wrangler discovery", async () => {
    const fixture = await import("../fixtures/consumer/src/index");
    // The fixture must re-export the DO classes for the wrangler
    // discovery pattern to work in a real deploy.
    //
    // Phase 11.1: SearchDO is no longer part of the SDK surface —
    // verify the fixture does NOT carry a `.SearchDO` export so any
    // future regression that re-introduces it (and silently inflates
    // every consumer's bundle with the CLIP/BGE vector code) trips
    // this assertion.
    expect(typeof fixture.UserDO).toBe("function");
    expect(typeof fixture.ShardDO).toBe("function");
    expect("SearchDO" in fixture).toBe(false);
  });
});
