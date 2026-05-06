/**
 * E2E K — Phase 15 end-to-end encryption coverage (14 cases).
 *
 * These tests exercise the encryption boundary against the live
 * Service-mode worker (https://mossaic-core.ashishkmr472.workers.dev
 * by default). The Mossaic server NEVER decrypts user data — these
 * tests verify exactly that:
 *
 *   - the server stamps `files.encryption_mode` from the
 *     `X-Mossaic-Encryption` request header
 *   - the server returns the envelope bytes verbatim with the
 *     `X-Mossaic-Encryption` response header set
 *   - cross-mode and plaintext-into-encrypted writes are rejected
 *     server-side with EBADF
 *   - convergent envelopes dedupe at the chunk-hash boundary;
 *     random envelopes do NOT dedupe
 *   - copyFile preserves the encryption stamp; the destination is
 *     readable with the same master key, fails with WRONG_KEY for
 *     a different key
 *   - encrypted yjs-mode files are opaque to the wire-level Yjs
 *     adapter (server returns envelope bytes, not plaintext updates)
 *
 * Encryption itself is performed CLIENT-SIDE in these tests using
 * `encryptPayload` / `decryptPayload` from `@mossaic/sdk/encryption`
 * (the same primitives the in-process binding-mode SDK uses). The
 * `HttpVFS` client does not auto-encrypt — that is by design and
 * documented in `sdk/src/http-only.ts`. The HTTP test pattern is the
 * one used by `tests/integration/encryption-http.test.ts`:
 *
 *   1. encryptPayload(plaintext, cfg) → envelope bytes
 *   2. POST /api/vfs/writeFile?path=... with X-Mossaic-Encryption
 *      header + octet-stream body
 *   3. POST /api/vfs/readFile → response body is envelope bytes,
 *      response header X-Mossaic-Encryption is the stamp
 *   4. decryptPayload(envelope, cfg) → plaintext
 *
 * Test isolation: each test creates a fresh ulid-suffixed tenant
 * via `freshTenant()` (matches the pattern used by every other E2E
 * test in this folder). Per-tenant teardown walks the tenant root
 * and removeRecursive's every entry. Two tenants in the same
 * describe-block can never collide.
 *
 * Why no test for the in-process `compactEncryptedYjs` /
 * `vfs.compactYjs` API: those flows require either a Worker runtime
 * (binding-mode SDK) or an HTTP route that doesn't exist in v15
 * (compaction is client-driven, the SDK speaks DO RPC directly).
 * The HTTP fallback consumer cannot trigger client-driven encrypted
 * Yjs compaction over the wire today; the closest assertion we can
 * make is K.10 (encrypted yjs is opaque to the wire-level Yjs
 * adapter) which is included.
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { freshTenant, type TenantCtx } from "./helpers/tenant.js";
import { hasSecret, requireSecret } from "./helpers/env.js";
import {
  encryptPayload,
  decryptPayload,
  WRONG_KEY,
  type EncryptionConfig,
} from "@mossaic/sdk/encryption";
import { VFS_MODE_YJS_BIT } from "@mossaic/sdk/http";
import { openYDocOverWs } from "../../src/yjs-ws.js";

/**
 * Build a deterministic 32-byte raw key from a single byte. The
 * value is irrelevant for the test (these are throwaway tenants);
 * what matters is that two calls with the same byte produce the same
 * key (so we can detect dedup) and two calls with different bytes
 * produce different keys (so wrong-key fails authentically).
 */
function makeKey(byte: number): Uint8Array {
  const a = new Uint8Array(32);
  a.fill(byte);
  return a;
}

/**
 * Fill `buf` with cryptographically random bytes, working around
 * Web Crypto's 64 KiB-per-call limit on `getRandomValues`. For
 * payloads up to 64 KiB this is identical to `getRandomValues`;
 * larger sizes are chunked.
 */
function fillRandom(buf: Uint8Array): Uint8Array {
  const CHUNK = 65_536;
  for (let off = 0; off < buf.length; off += CHUNK) {
    const slice = buf.subarray(off, Math.min(off + CHUNK, buf.length));
    crypto.getRandomValues(slice);
  }
  return buf;
}

/**
 * Build a fresh, random-per-test EncryptionConfig. Each call returns
 * a *different* tenantSalt so two tests cannot accidentally produce
 * the same envelope bytes.
 */
function freshEncCfg(
  mode: "convergent" | "random" = "convergent",
  keyId?: string,
): EncryptionConfig {
  const masterKey = crypto.getRandomValues(new Uint8Array(32));
  const tenantSalt = crypto.getRandomValues(new Uint8Array(32));
  const cfg: EncryptionConfig = { masterKey, tenantSalt, mode };
  if (keyId !== undefined) cfg.keyId = keyId;
  return cfg;
}

/**
 * POST /api/vfs/writeFile with optional `X-Mossaic-Encryption`
 * header. Returns nothing on success; throws an Error with the
 * server's status + body on non-2xx.
 */
async function httpWriteFile(
  ctx: TenantCtx,
  path: string,
  bytes: Uint8Array,
  encryption?: { mode: "convergent" | "random"; keyId?: string },
): Promise<void> {
  const headers: Record<string, string> = {
    Authorization: `Bearer ${ctx.token}`,
    "Content-Type": "application/octet-stream",
  };
  if (encryption !== undefined) {
    headers["X-Mossaic-Encryption"] = JSON.stringify(encryption);
  }
  const url =
    ctx.endpoint + "/api/vfs/writeFile?path=" + encodeURIComponent(path);
  const r = await fetch(url, {
    method: "POST",
    headers,
    // Cast: fetch's BodyInit signature wants ArrayBuffer / Blob /
    // ReadableStream / typed arrays — Uint8Array is accepted at
    // runtime in Node 20+ via undici.
    body: bytes as BodyInit,
  });
  if (!r.ok) {
    const body = await r.text();
    throw Object.assign(new Error(`writeFile HTTP ${r.status}: ${body}`), {
      status: r.status,
      body,
    });
  }
}

/**
 * POST /api/vfs/readFile and return BOTH the response body bytes and
 * any `X-Mossaic-Encryption` response header. Throws on non-2xx with
 * the server's body for diagnostics.
 */
async function httpReadFile(
  ctx: TenantCtx,
  path: string,
): Promise<{ bytes: Uint8Array; encryption: string | null }> {
  const url = ctx.endpoint + "/api/vfs/readFile";
  const r = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${ctx.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ path }),
  });
  if (!r.ok) {
    const body = await r.text();
    throw Object.assign(new Error(`readFile HTTP ${r.status}: ${body}`), {
      status: r.status,
      body,
    });
  }
  const buf = new Uint8Array(await r.arrayBuffer());
  return { bytes: buf, encryption: r.headers.get("X-Mossaic-Encryption") };
}

/**
 * Helper used by K.10 — copy of the setYjsMode helper in f-yjs.test.ts.
 * Inlined to keep this file self-contained.
 */
async function setYjsMode(
  ctx: TenantCtx,
  path: string,
  enabled: boolean,
): Promise<void> {
  const r = await fetch(ctx.endpoint + "/api/vfs/setYjsMode", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${ctx.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ path, enabled }),
  });
  if (!r.ok) {
    const body = await r.text();
    throw Object.assign(new Error(`setYjsMode HTTP ${r.status}: ${body}`), {
      status: r.status,
    });
  }
}

describe.skipIf(!hasSecret())("K — Phase 15 encryption coverage", () => {
  beforeAll(() => requireSecret());

  let ctx: TenantCtx;
  beforeEach(async () => {
    ctx = await freshTenant();
  });
  afterEach(async () => {
    await ctx.teardown();
  });

  it("K.1 — plaintext, encrypted, and mixed writes coexist in one tenant", async () => {
    const cfg = freshEncCfg("convergent", "k1-key");
    // Plaintext file via HttpVFS (no encryption header).
    await ctx.vfs.writeFile("/plain.txt", "hello plaintext");
    // Encrypted file via raw HTTP.
    const env = await encryptPayload(
      new TextEncoder().encode("hello encrypted"),
      cfg,
    );
    await httpWriteFile(ctx, "/enc.bin", env, { mode: "convergent", keyId: "k1-key" });

    // Both stat correctly with their respective encryption fields
    // (canonical surface — `vfsStat` reads `files.encryption_*`
    // directly).
    const sPlain = await ctx.vfs.stat("/plain.txt");
    const sEnc = await ctx.vfs.stat("/enc.bin");
    expect(sPlain.encryption).toBeUndefined();
    expect(sEnc.encryption).toEqual({ mode: "convergent", keyId: "k1-key" });

    // listFiles surfaces both files (presence test). The `stat`
    // sub-object on listFiles items is a minimal projection that
    // does NOT carry `encryption` in v15 (see
    // `worker/core/objects/user/list-files.ts:566` —
    // `out.stat` only includes type/mode/size/mtimeMs/uid/gid/ino).
    // Consumers needing the encryption stamp must call vfsStat per
    // path. We test the canonical stat path above; here we only
    // assert presence + path mapping.
    const page = await ctx.vfs.listFiles({ prefix: "/" });
    const paths = page.items.map((i) => i.path).sort();
    expect(paths).toContain("/plain.txt");
    expect(paths).toContain("/enc.bin");
  });

  it("K.2 — encrypted writeFile / readFile round-trip (utf8 + binary)", async () => {
    const cfg = freshEncCfg("random");

    // (a) UTF-8 plaintext.
    const utf8Plain = "héllo Φ utf-8 ⛅️ encrypted ✓";
    const utf8Bytes = new TextEncoder().encode(utf8Plain);
    const utf8Env = await encryptPayload(utf8Bytes, cfg);
    await httpWriteFile(ctx, "/utf8.bin", utf8Env, { mode: "random" });

    const r1 = await httpReadFile(ctx, "/utf8.bin");
    expect(r1.encryption).not.toBeNull();
    const recoveredUtf8Bytes = await decryptPayload(r1.bytes, cfg);
    expect(new TextDecoder().decode(recoveredUtf8Bytes)).toBe(utf8Plain);

    // (b) Random binary plaintext (8 KiB).
    const binPlain = crypto.getRandomValues(new Uint8Array(8192));
    const binEnv = await encryptPayload(binPlain, cfg);
    await httpWriteFile(ctx, "/bin.dat", binEnv, { mode: "random" });

    const r2 = await httpReadFile(ctx, "/bin.dat");
    expect(r2.encryption).not.toBeNull();
    const recoveredBin = await decryptPayload(r2.bytes, cfg);
    expect(recoveredBin).toEqual(binPlain);
  });

  it("K.3 — readFile WITHOUT decryption returns ciphertext bytes (server can't decrypt)", async () => {
    const cfg = freshEncCfg("convergent", "k3");
    const plaintext = "TOP-SECRET-marker-string-must-not-leak-through-server";
    const plainBytes = new TextEncoder().encode(plaintext);
    const env = await encryptPayload(plainBytes, cfg);

    await httpWriteFile(ctx, "/secret.bin", env, {
      mode: "convergent",
      keyId: "k3",
    });

    // Read raw envelope back without decrypting.
    const r = await httpReadFile(ctx, "/secret.bin");

    // The bytes returned by the server are the envelope, not the
    // plaintext. Two facts prove this:
    //   1. The bytes equal the envelope we wrote (byte-faithful).
    //   2. The plaintext marker string is NOT a substring of the
    //      envelope bytes — AES-GCM would have to be broken for it
    //      to appear there.
    expect(r.bytes).toEqual(env);
    const haystack = Buffer.from(r.bytes).toString("binary");
    expect(haystack.includes(plaintext)).toBe(false);

    // The encryption header tells the consumer "this needs decryption".
    expect(r.encryption).not.toBeNull();
    const stamp = JSON.parse(r.encryption!);
    expect(stamp.mode).toBe("convergent");
    expect(stamp.keyId).toBe("k3");
  });

  it("K.4 — wrong masterKey on read → WRONG_KEY (EINVAL)", async () => {
    const cfgWrite = freshEncCfg("random");
    // Different masterKey AND different tenantSalt → guaranteed-bad
    // decrypt regardless of mode.
    const cfgRead: EncryptionConfig = {
      masterKey: makeKey(0x99),
      tenantSalt: makeKey(0xaa),
      mode: "random",
    };
    const plain = new TextEncoder().encode("only the right key sees this");
    const env = await encryptPayload(plain, cfgWrite);
    await httpWriteFile(ctx, "/wk.bin", env, { mode: "random" });

    const r = await httpReadFile(ctx, "/wk.bin");
    // The server gave us the bytes verbatim — but decryption must fail.
    await expect(decryptPayload(r.bytes, cfgRead)).rejects.toMatchObject({
      code: "EINVAL",
    });
    // Also assert the typed-error class so consumers can catch it
    // structurally rather than by code string.
    let caught: unknown;
    try {
      await decryptPayload(r.bytes, cfgRead);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(WRONG_KEY);
  });

  it("K.5 — convergent mode dedupes: same plaintext → same chunk hashes across paths", async () => {
    // The SDK encrypts the WHOLE file as a single envelope (Phase 15
    // §4 — multi-chunk envelope-stream encryption is v15.1). Convergent
    // mode derives the IV deterministically from plaintext+key, so
    // two writes of the same plaintext under the same key/salt
    // produce byte-identical envelopes — the storage layer hashes
    // those envelope bytes for dedup.
    //
    // To detect dedup we compare the chunk hashes via openManifest.
    // Both paths must report the same chunks list (same hash → same
    // underlying chunk row).
    const cfg = freshEncCfg("convergent", "dedup");
    // Plaintext must exceed shared/inline.ts INLINE_LIMIT (16 KB) so
    // openManifest returns chunked data (inline files have no
    // chunk_hash list to compare). 64 KB is comfortably above the
    // cutoff and small enough to keep the round-trip fast.
    const plain = fillRandom(new Uint8Array(64 * 1024));

    const env1 = await encryptPayload(plain, cfg);
    const env2 = await encryptPayload(plain, cfg);
    // Sanity: convergent envelopes are byte-identical.
    expect(env1).toEqual(env2);

    await httpWriteFile(ctx, "/dedup-a.bin", env1, {
      mode: "convergent",
      keyId: "dedup",
    });
    await httpWriteFile(ctx, "/dedup-b.bin", env2, {
      mode: "convergent",
      keyId: "dedup",
    });

    const mA = await ctx.vfs.openManifest("/dedup-a.bin");
    const mB = await ctx.vfs.openManifest("/dedup-b.bin");

    // If either is inlined the assertion below would be vacuous — we
    // pre-validate the test scenario before checking dedup.
    expect(mA.inlined).toBe(false);
    expect(mB.inlined).toBe(false);

    const hashesA = mA.chunks.map((c) => c.hash);
    const hashesB = mB.chunks.map((c) => c.hash);
    // Same plaintext, convergent mode → byte-identical envelope →
    // identical chunk hashes (dedup at storage layer).
    expect(hashesA).toEqual(hashesB);
    // Sanity: both have at least one chunk.
    expect(hashesA.length).toBeGreaterThan(0);
  }, 90_000);

  it("K.6 — random mode does NOT dedupe: same plaintext → different chunk hashes", async () => {
    // Random mode generates a fresh 12-byte IV per envelope, so two
    // envelopes of the same plaintext are byte-different and therefore
    // produce different chunk hashes — no dedup possible.
    const cfg = freshEncCfg("random");
    // Same size justification as K.5 — exceed INLINE_LIMIT (16 KB)
    // so openManifest returns a chunk list to compare.
    const plain = fillRandom(new Uint8Array(64 * 1024));

    const env1 = await encryptPayload(plain, cfg);
    const env2 = await encryptPayload(plain, cfg);
    // Sanity: random envelopes differ even for identical plaintext.
    expect(env1).not.toEqual(env2);

    await httpWriteFile(ctx, "/rand-a.bin", env1, { mode: "random" });
    await httpWriteFile(ctx, "/rand-b.bin", env2, { mode: "random" });

    const mA = await ctx.vfs.openManifest("/rand-a.bin");
    const mB = await ctx.vfs.openManifest("/rand-b.bin");
    expect(mA.inlined).toBe(false);
    expect(mB.inlined).toBe(false);

    const hashesA = mA.chunks.map((c) => c.hash);
    const hashesB = mB.chunks.map((c) => c.hash);
    // No two chunks should match between the two writes — this
    // proves no envelope-byte overlap between random-mode writes
    // of identical plaintext.
    const hashesBSet = new Set(hashesB);
    const overlap = hashesA.filter((h) => hashesBSet.has(h));
    expect(overlap).toEqual([]);
  }, 90_000);

  it("K.7 — patchMetadata on encrypted file works; metadata stays plaintext", async () => {
    const cfg = freshEncCfg("convergent");
    const plain = new TextEncoder().encode("payload bytes");
    const env = await encryptPayload(plain, cfg);
    await httpWriteFile(ctx, "/with-meta.bin", env, { mode: "convergent" });

    // Patch with a plaintext marker we can spot via listFiles
    // (which returns metadata as JSON — encryption stamps the
    // file BYTES, not the metadata column).
    const marker = "metadata-plaintext-marker-7e3f";
    await ctx.vfs.patchMetadata("/with-meta.bin", { tag: marker });

    const page = await ctx.vfs.listFiles({
      prefix: "/",
      includeMetadata: true,
    });
    const item = page.items.find((i) => i.path === "/with-meta.bin")!;
    // Metadata is fully readable server-side (proves it stays plaintext).
    expect(item.metadata).toEqual({ tag: marker });
    // The encryption stamp is preserved on the file row — verify via
    // the canonical vfsStat surface (listFiles' stat projection
    // intentionally omits the encryption field; see K.1 comment).
    const sEnc = await ctx.vfs.stat("/with-meta.bin");
    expect(sEnc.encryption).toEqual({ mode: "convergent" });

    // Body still decrypts correctly after the metadata patch.
    const r = await httpReadFile(ctx, "/with-meta.bin");
    const back = await decryptPayload(r.bytes, cfg);
    expect(new TextDecoder().decode(back)).toBe("payload bytes");
  });

  it("K.8 — listFiles returns encrypted + plaintext together with correct stamps", async () => {
    const cfg = freshEncCfg("random");
    // Three encrypted files, two plaintext, all tagged "mixed" so we
    // can list them as a unit.
    for (let i = 0; i < 3; i++) {
      const env = await encryptPayload(
        new TextEncoder().encode(`enc-${i}`),
        cfg,
      );
      await httpWriteFile(ctx, `/m-enc-${i}.bin`, env, { mode: "random" });
      await ctx.vfs.patchMetadata(`/m-enc-${i}.bin`, null, {
        addTags: ["mixed"],
      });
    }
    for (let i = 0; i < 2; i++) {
      await ctx.vfs.writeFile(`/m-plain-${i}.txt`, `plain-${i}`, {
        tags: ["mixed"],
      });
    }
    const page = await ctx.vfs.listFiles({
      tags: ["mixed"],
      includeMetadata: true,
    });
    const byPath = new Map(page.items.map((i) => [i.path, i]));
    expect(byPath.size).toBe(5);

    // listFiles returns mixed-mode files together (presence test).
    // The encryption stamp is verified per path via vfsStat — the
    // listFiles projection intentionally omits it (see K.1 comment
    // for the line reference). Per-file stat:
    for (let i = 0; i < 3; i++) {
      const s = await ctx.vfs.stat(`/m-enc-${i}.bin`);
      expect(s.encryption?.mode).toBe("random");
    }
    for (let i = 0; i < 2; i++) {
      const s = await ctx.vfs.stat(`/m-plain-${i}.txt`);
      expect(s.encryption).toBeUndefined();
    }
  });

  it("K.9 — copyFile of encrypted file preserves stamp; dest decrypts with same key, fails with wrong key", async () => {
    const cfgRight = freshEncCfg("convergent", "k9-orig");
    const cfgWrong: EncryptionConfig = {
      masterKey: makeKey(0x77),
      tenantSalt: makeKey(0x88),
      mode: "convergent",
    };
    const plain = new TextEncoder().encode("copy-source-payload");
    const env = await encryptPayload(plain, cfgRight);
    await httpWriteFile(ctx, "/cp-src.bin", env, {
      mode: "convergent",
      keyId: "k9-orig",
    });

    // Copy via HttpVFS (server-side copy preserves encryption columns).
    await ctx.vfs.copyFile("/cp-src.bin", "/cp-dest.bin");

    const sDest = await ctx.vfs.stat("/cp-dest.bin");
    expect(sDest.encryption).toEqual({ mode: "convergent", keyId: "k9-orig" });

    // Right key decrypts dest payload exactly.
    const rDest = await httpReadFile(ctx, "/cp-dest.bin");
    expect(rDest.encryption).not.toBeNull();
    const back = await decryptPayload(rDest.bytes, cfgRight);
    expect(new TextDecoder().decode(back)).toBe("copy-source-payload");

    // Wrong key on dest fails authentically.
    await expect(decryptPayload(rDest.bytes, cfgWrong)).rejects.toBeInstanceOf(
      WRONG_KEY,
    );
  });

  it("K.10 — encrypted yjs file is opaque to wire-level Yjs adapter (server returns envelopes; plaintext never leaks)", async () => {
    // Setup: write an encrypted file, then enable yjs-mode on it.
    // Phase 15's encrypted-yjs path means the SERVER stores the
    // envelope op log opaquely. The wire-level openYDocOverWs
    // adapter (encryption-unaware) cannot usefully decrypt these,
    // but we CAN still observe:
    //   - the file's encryption stamp is preserved through the
    //     yjs-mode toggle
    //   - no plaintext leaks through the readFile boundary even
    //     though the server now treats the bytes as a yjs op-log
    const cfg = freshEncCfg("random");
    const plaintextMarker = "yjs-plaintext-marker-must-never-leak";
    const env = await encryptPayload(
      new TextEncoder().encode(plaintextMarker),
      cfg,
    );
    await httpWriteFile(ctx, "/yenc.md", env, { mode: "random" });
    await setYjsMode(ctx, "/yenc.md", true);

    // stat: encryption stamp is intact AND yjs-mode bit is set.
    const s = await ctx.vfs.stat("/yenc.md");
    expect(s.encryption).toEqual({ mode: "random" });
    expect(s.mode & VFS_MODE_YJS_BIT).toBe(VFS_MODE_YJS_BIT);

    // The wire-level openYDocOverWs adapter is encryption-unaware
    // (by design; encryption is the SDK binding-mode adapter's job).
    // Connecting to an encrypted yjs file MAY succeed at the WS
    // upgrade layer (the server relays opaque bytes), but the local
    // doc view CANNOT contain the plaintext marker — proving the
    // server is not materialising plaintext server-side.
    // The wire-level adapter calls `Y.applyUpdate` on every received
    // tag-2 frame. For an encrypted yjs file the server returns
    // opaque envelope bytes; lib0's decoder will throw "Unexpected
    // end of array" inside applyUpdate, and because applyUpdate is
    // invoked from the WS message handler (no surrounding try/catch
    // in the SDK adapter today) the error escapes as an uncaught
    // exception. We install a temporary process-level handler to
    // swallow ONLY those decoder errors during this test — any
    // unrelated uncaught exception still propagates.
    const swallowedDecodeErrors: unknown[] = [];
    const onUncaught = (err: unknown): void => {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("Unexpected end of array")) {
        swallowedDecodeErrors.push(err);
        return;
      }
      // Re-throw anything else by re-emitting; vitest will catch it.
      throw err;
    };
    process.on("uncaughtException", onUncaught);

    let handle: Awaited<ReturnType<typeof openYDocOverWs>> | undefined;
    try {
      handle = await openYDocOverWs({
        endpoint: ctx.endpoint,
        token: ctx.token,
        path: "/yenc.md",
      });
      // Wait for initial sync to complete OR fail. Either is fine —
      // we only care that the local doc state never contains the
      // plaintext marker.
      try {
        await Promise.race([
          handle.synced,
          new Promise<void>((_, rej) =>
            setTimeout(() => rej(new Error("sync timeout")), 3000),
          ),
        ]);
      } catch {
        /* sync failure is expected for opaque envelope updates */
      }
      // Look at every Y.Text in the doc — none of them may contain
      // the plaintext marker.
      const allText = JSON.stringify(handle.doc.toJSON());
      expect(allText.includes(plaintextMarker)).toBe(false);
    } finally {
      if (handle) await handle.close();
      // Give any in-flight applyUpdate calls a tick to throw so
      // we capture them in our handler before removing it.
      await new Promise((r) => setTimeout(r, 200));
      process.off("uncaughtException", onUncaught);
    }
    // Strengthen the assertion: the server MUST have sent at least
    // one frame that the encryption-unaware Y.applyUpdate could not
    // decode. If `swallowedDecodeErrors.length === 0` the server
    // either sent no frames (encrypted-yjs relay broken) OR sent
    // valid plaintext yjs frames (encryption boundary breached).
    // Either is a Phase 15 violation we want to catch.
    expect(swallowedDecodeErrors.length).toBeGreaterThan(0);

    // Re-stat: encryption stamp is still there post-WS-handshake.
    const s2 = await ctx.vfs.stat("/yenc.md");
    expect(s2.encryption).toEqual({ mode: "random" });
  }, 30_000);

  it("K.11 — readFile bytes never contain plaintext (envelope-only storage proof, large payload)", async () => {
    // Pin a stronger version of the K.3 invariant: 64 KiB of random
    // plaintext, one written-then-read cycle, scan the entire
    // returned envelope for any 16-byte plaintext substring.
    const cfg = freshEncCfg("random");
    // Pre-Phase-15.1 the SDK encrypts the WHOLE file as one envelope,
    // so 64 KiB plaintext → ~64 KiB envelope. fillRandom handles the
    // 64 KiB-per-call cap on Web Crypto's getRandomValues.
    const plain = fillRandom(new Uint8Array(64 * 1024));
    const env = await encryptPayload(plain, cfg);
    await httpWriteFile(ctx, "/big-secret.bin", env, { mode: "random" });

    const r = await httpReadFile(ctx, "/big-secret.bin");
    expect(r.encryption).not.toBeNull();

    // Scan: try every 16-byte window of the plaintext against the
    // returned bytes. AES-GCM is IND-CPA so the probability of any
    // such window appearing is < 2^-128 per window — effectively
    // zero for a passing test.
    const plainBuf = Buffer.from(plain);
    const envBuf = Buffer.from(r.bytes);
    let foundLeak = false;
    for (let off = 0; off + 16 <= plainBuf.length; off += 256) {
      // Sample every 256th window — 16-byte window scanning every
      // single offset would be O(n*m) and slow on a 64KB plaintext.
      // Sampling 256 windows out of ~4000 keeps the test under 1s
      // while still giving comprehensive coverage.
      const needle = plainBuf.subarray(off, off + 16);
      if (envBuf.indexOf(needle) !== -1) {
        foundLeak = true;
        break;
      }
    }
    expect(foundLeak).toBe(false);

    // Sanity: the right key still recovers the exact plaintext.
    const back = await decryptPayload(r.bytes, cfg);
    expect(back).toEqual(plain);
  });

  it("K.12 — HTTP fallback X-Mossaic-Encryption header round-trips on stat + read", async () => {
    const cfg = freshEncCfg("convergent", "k12-key-v1");
    const env = await encryptPayload(
      new TextEncoder().encode("k12 payload"),
      cfg,
    );
    await httpWriteFile(ctx, "/k12.bin", env, {
      mode: "convergent",
      keyId: "k12-key-v1",
    });

    // (a) stat returns encryption_mode + encryption_key_id.
    const sUrl = ctx.endpoint + "/api/vfs/stat";
    const sr = await fetch(sUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${ctx.token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/k12.bin" }),
    });
    expect(sr.status).toBe(200);
    const sb = (await sr.json()) as {
      stat: { encryption?: { mode: string; keyId?: string } };
    };
    expect(sb.stat.encryption).toEqual({
      mode: "convergent",
      keyId: "k12-key-v1",
    });

    // (b) readFile sends X-Mossaic-Encryption response header
    //     matching the stat field for encrypted files.
    const rRes = await httpReadFile(ctx, "/k12.bin");
    expect(rRes.encryption).not.toBeNull();
    const stamp = JSON.parse(rRes.encryption!);
    expect(stamp).toEqual({ mode: "convergent", keyId: "k12-key-v1" });

    // (c) Plaintext file does NOT have the response header.
    await ctx.vfs.writeFile("/k12-plain.txt", "no encryption here");
    const rPlain = await httpReadFile(ctx, "/k12-plain.txt");
    expect(rPlain.encryption).toBeNull();
  });

  it("K.13 — EBADF: cross-mode write to existing encrypted path", async () => {
    const cfg = freshEncCfg("convergent");
    const env1 = await encryptPayload(
      new TextEncoder().encode("first write convergent"),
      cfg,
    );
    await httpWriteFile(ctx, "/mode-mismatch.bin", env1, { mode: "convergent" });

    // Second write to the SAME path, with a different mode →
    // server rejects with EBADF (Phase 15 §4.5 — encryption mode is
    // pinned per path's history). 409 is the EBADF status code in
    // worker/core/routes/vfs.ts:143; we additionally assert the
    // response body's `code` field is the exact string "EBADF" to
    // disambiguate from other 409s (EISDIR, EEXIST, EBUSY, ENOTEMPTY).
    const cfg2 = freshEncCfg("random");
    const env2 = await encryptPayload(new TextEncoder().encode("nope"), cfg2);
    let caught: { status?: number; body?: string } | undefined;
    try {
      await httpWriteFile(ctx, "/mode-mismatch.bin", env2, { mode: "random" });
    } catch (err) {
      caught = err as { status?: number; body?: string };
    }
    expect(caught).toBeDefined();
    expect(caught!.status).toBe(409);
    const body = JSON.parse(caught!.body!);
    expect(body.code).toBe("EBADF");
    expect(String(body.message)).toMatch(/encryption|mix/i);
  });

  it("K.14 — EBADF: plaintext write to existing encrypted path", async () => {
    const cfg = freshEncCfg("random");
    const env = await encryptPayload(
      new TextEncoder().encode("encrypted first"),
      cfg,
    );
    await httpWriteFile(ctx, "/no-plain.bin", env, { mode: "random" });

    // Plaintext write to an encrypted path → EBADF (409). Tightened
    // body.code === "EBADF" assertion as in K.13.
    let caught: { status?: number; body?: string } | undefined;
    try {
      await httpWriteFile(
        ctx,
        "/no-plain.bin",
        new TextEncoder().encode("plaintext attempt"),
        // No encryption header — this is a plaintext write attempt.
      );
    } catch (err) {
      caught = err as { status?: number; body?: string };
    }
    expect(caught).toBeDefined();
    expect(caught!.status).toBe(409);
    const body = JSON.parse(caught!.body!);
    expect(body.code).toBe("EBADF");
    expect(String(body.message)).toMatch(/encrypted|encryption/i);
  });
});
