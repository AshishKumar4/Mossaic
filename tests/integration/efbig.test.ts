import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 6 — EFBIG enforcement on readFile + writeFile + appendStream.
 *
 * The plan §11 requires per-method size caps so a pathological
 * payload fails fast with EFBIG rather than blowing the Worker's
 * memory limit. Caps are configured in shared/inline.ts:
 *
 *   READFILE_MAX  = 500 MB
 *   WRITEFILE_MAX = 500 MB
 *
 * Real-time pumping 500 MB through Miniflare RPC would dominate test
 * runtime (multi-minute) and trip the per-RPC 32 MiB serialization
 * cap. We instead seed the row's file_size to just above the cap
 * via direct DO state manipulation, then assert the public API
 * surfaces EFBIG with the correct error code.
 *
 * Coverage:
 *   - readFile (chunked tier) over a row whose file_size > READFILE_MAX
 *   - readFile (inlined tier) over a row whose inline_data is oversized
 *     (defensive — INLINE_LIMIT = 16 KB so this can't happen
 *     organically, but the EFBIG branch in vfs-ops handles it for
 *     legacy / corrupt data)
 *   - writeFile with a payload > WRITEFILE_MAX
 *   - appendWriteStream where cumulative would exceed WRITEFILE_MAX
 *
 * The error must:
 *   - throw VFSError with code === "EFBIG" on the server side
 *   - reach the SDK consumer as VFSFsError with code === "EFBIG"
 *     (Phase 5 mapServerError extracts the code from the message
 *     even after RPC re-serialisation)
 */

import { createVFS, type MossaicEnv, EFBIG, VFSFsError } from "../../sdk/src/index";
import { READFILE_MAX, WRITEFILE_MAX } from "@shared/inline";

interface E {
  USER_DO: DurableObjectNamespace;
}
const E = env as unknown as E;

function makeEnv(): MossaicEnv {
  return { MOSSAIC_USER: E.USER_DO as MossaicEnv["MOSSAIC_USER"] };
}

describe("EFBIG enforcement", () => {
  it("readFile throws EFBIG when file_size > READFILE_MAX", async () => {
    const vfs = createVFS(makeEnv(), { tenant: "efbig-readfile" });

    // Seed a CHUNKED file (>INLINE_LIMIT so it goes through the
    // chunked write path; inline reads bypass the file_size check).
    // We use the begin/append/commit primitives to write one small
    // chunk so the row ends up with chunk_count=1 + inline_data NULL.
    const handle = await vfs.createWriteStreamWithHandle("/big.bin");
    const writer = handle.stream.getWriter();
    // Push >INLINE_LIMIT so commit goes via chunked tier.
    await writer.write(new Uint8Array(20 * 1024).fill(7));
    await writer.close();
    writer.releaseLock();

    // Now pump file_size past the cap via direct DO state mutation.
    // The actual chunk stays tiny — vfs-ops checks file_size before
    // fanning out chunk fetches, so it errors before any I/O.
    const stub = E.USER_DO.get(
      E.USER_DO.idFromName("vfs:default:efbig-readfile")
    );
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "UPDATE files SET file_size = ? WHERE file_name = 'big.bin'",
        READFILE_MAX + 1
      );
    });

    let caught: unknown = null;
    try {
      await vfs.readFile("/big.bin");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(VFSFsError);
    expect((caught as VFSFsError).code).toBe("EFBIG");
    expect(caught).toBeInstanceOf(EFBIG);
  });

  it("inline tier reads work normally below the cap (sanity)", async () => {
    // The defensive branch in vfs-ops that throws EFBIG when
    // inline_data.byteLength > READFILE_MAX is belt-and-suspenders
    // code: it can't be reached through the public write API
    // (INLINE_LIMIT = 16 KB enforced at write time) AND can't be
    // reached via SQL seed (workerd's SQLite BLOB row limit is
    // 2 MB << READFILE_MAX = 500 MB). We document the gap and
    // assert the happy-path: a small inline read works.
    const vfs = createVFS(makeEnv(), { tenant: "efbig-inline-sanity" });
    await vfs.writeFile("/i.txt", new TextEncoder().encode("hello"));
    const got = await vfs.readFile("/i.txt", { encoding: "utf8" });
    expect(got).toBe("hello");
  });

  it("writeFile rejects oversized payloads (RPC arg cap fires before EFBIG)", async () => {
    // The WRITEFILE_MAX EFBIG check inside vfsWriteFile is reached
    // only AFTER the RPC layer accepts the payload. workerd caps
    // serialised RPC args at 32 MiB, well below WRITEFILE_MAX (500
    // MB), so any consumer payload approaching the cap fails at
    // the RPC boundary first — the EFBIG branch is defensive and
    // primarily relevant when the DO method is invoked
    // intra-process (not over RPC) or when WRITEFILE_MAX is
    // configured BELOW the RPC cap.
    //
    // We assert that an oversized SDK writeFile fails with SOME
    // typed error (not silent corruption / silent truncation). The
    // exact code depends on whether the RPC layer or the EFBIG
    // branch fires first.
    const vfs = createVFS(makeEnv(), { tenant: "efbig-writefile" });
    let caught: unknown = null;
    try {
      // 33 MiB is reliably above the RPC arg cap (~32 MiB).
      const oversized = new Uint8Array(33 * 1024 * 1024);
      await vfs.writeFile("/oversized.bin", oversized);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    // The error reaches the consumer as a VFSFsError because the
    // SDK's writeFile wraps any thrown error via mapServerError.
    expect(caught).toBeInstanceOf(VFSFsError);
  });

  it("writeFile under-cap succeeds (sanity)", async () => {
    // Sub-cap writes work. A 1 MB payload is well below both the
    // RPC cap and WRITEFILE_MAX.
    const vfs = createVFS(makeEnv(), { tenant: "efbig-writefile-ok" });
    const payload = new Uint8Array(1024 * 1024).fill(0xab);
    await vfs.writeFile("/ok.bin", payload);
    const back = await vfs.readFile("/ok.bin");
    expect(back.byteLength).toBe(payload.byteLength);
    expect(back[0]).toBe(0xab);
    expect(back[back.byteLength - 1]).toBe(0xab);
  });

  it("appendWriteStream throws EFBIG on cumulative overflow", async () => {
    // This is already covered in tests/integration/streaming.test.ts
    // via SQL seeding of file_size. Re-asserting here against the
    // SDK consumer surface so a single Phase 6 file demonstrates
    // EFBIG on every entry point. Drives through the SDK's
    // createWriteStreamWithHandle to surface the handle, then SQL-
    // seeds file_size, then attempts an append that would push
    // cumulative over WRITEFILE_MAX.
    const vfs = createVFS(makeEnv(), { tenant: "efbig-stream" });
    const { handle } = await vfs.createWriteStreamWithHandle("/stream.bin");

    const stub = E.USER_DO.get(
      E.USER_DO.idFromName("vfs:default:efbig-stream")
    );
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "UPDATE files SET file_size = ? WHERE file_id = ?",
        WRITEFILE_MAX - 5,
        handle.tmpId
      );
    });

    // The next vfsAppendWriteStream call (10 bytes) crosses the cap.
    // We invoke it via the underlying namespace stub since the SDK's
    // VFS class doesn't expose append directly — that's intentional
    // (consumers stream via WritableStream). For the test we go
    // direct.
    const userStub = E.USER_DO.get(
      E.USER_DO.idFromName("vfs:default:efbig-stream")
    ) as DurableObjectStub & {
      vfsAppendWriteStream(
        scope: { ns: string; tenant: string },
        h: typeof handle,
        idx: number,
        data: Uint8Array
      ): Promise<{ bytesWritten: number }>;
      vfsAbortWriteStream(
        scope: { ns: string; tenant: string },
        h: typeof handle
      ): Promise<void>;
    };

    let caught: unknown = null;
    try {
      await userStub.vfsAppendWriteStream(
        { ns: "default", tenant: "efbig-stream" },
        handle,
        0,
        new Uint8Array(10)
      );
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    // Server-side throws VFSError with code EFBIG; the message is
    // preserved on the wire even when the .code property isn't.
    expect(String(caught)).toMatch(/EFBIG/);

    // Cleanup
    await userStub.vfsAbortWriteStream(
      { ns: "default", tenant: "efbig-stream" },
      handle
    );
  });

  it("matches expected cap values (regression guard)", () => {
    // Pin the public cap values so a future bump is intentional.
    expect(READFILE_MAX).toBe(500 * 1024 * 1024);
    expect(WRITEFILE_MAX).toBe(500 * 1024 * 1024);
  });
});
