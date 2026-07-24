import { describe, expect, it } from "vitest";

import {
	createMossaicHttpClient,
	beginUpload,
	DEFAULT_COMPLETION_REQUEST_BUDGET,
	EFBIG,
	MULTIPART_OPERATION_RPC_BUDGET,
	MULTIPART_OPERATION_RETRY_BUDGET,
	parallelUpload,
	type MultipartUploadHandle,
} from "../../sdk/src/index";
import {
	MULTIPART_PAGED_CONTROL_CAPABILITY,
	MULTIPART_PROTOCOL_VERSION,
} from "../../shared/multipart";

describe("HTTP SDK multipart finalization", () => {
	it("collects more than 4096 landed chunks while preserving an explicit resume page", async () => {
		const requests: URL[] = [];
		const totalChunks = 4_097;
		const fetcher: typeof fetch = async (input) => {
			const url = new URL(
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			);
			requests.push(url);
			if (url.pathname.endsWith("/begin")) {
				return Response.json({
					uploadId: "upload-id",
					chunkSize: 1,
					totalChunks,
					poolSize: 65,
					sessionToken: "session-token",
					putEndpoint: "/api/vfs/multipart/upload-id",
					expiresAtMs: 1_000,
					landed: Array.from({ length: 256 }, (_, index) => index),
					continuation: "256",
					protocolVersion: MULTIPART_PROTOCOL_VERSION,
				});
			}
			const start = Number(url.searchParams.get("continuation"));
			const end = Math.min(start + 256, totalChunks);
			return Response.json({
				landed: Array.from({ length: end - start }, (_, offset) => offset + start),
				total: totalChunks,
				bytesUploaded: end - start,
				expiresAtMs: 1_000,
				...(end < totalChunks ? { continuation: String(end) } : {}),
			});
		};
		const client = createMossaicHttpClient({
			url: "https://mossaic.test",
			apiKey: "test-token",
			fetcher,
		});

		const page = await client.multipartBeginPage({
			path: "/large.bin",
			size: totalChunks,
			resumeFrom: "upload-id",
		});
		expect(page.landed).toHaveLength(256);
		expect(requests).toHaveLength(1);

		requests.length = 0;
		await expect(
			client.multipartBegin({
				path: "/large.bin",
				size: totalChunks,
				resumeFrom: "upload-id",
			}),
		).rejects.toMatchObject({ code: "EFBIG" });
		expect(requests).toHaveLength(DEFAULT_COMPLETION_REQUEST_BUDGET);

		requests.length = 0;
		const all = await beginUpload(client, "/large.bin", {
			size: totalChunks,
			resumeFrom: "upload-id",
		});
		expect(all.landed).toEqual(
			Array.from({ length: totalChunks }, (_, index) => index),
		);
		expect(requests).toHaveLength(17);

		requests.length = 0;
		const handle: MultipartUploadHandle = {
			uploadId: "upload-id",
			path: "/large.bin",
			chunkSize: 1,
			expectedChunks: totalChunks,
			poolSize: 65,
			sessionToken: "session-token",
			expiresAtMs: 1_000,
			size: totalChunks,
		};
		await expect(client.resumeMultipartUpload(handle)).rejects.toMatchObject({
			code: "EFBIG",
		});
		expect(requests).toHaveLength(0);

		requests.length = 0;
		const resumedPage = await client.resumeMultipartUploadPage(handle);
		expect(resumedPage.landed).toHaveLength(256);
		expect(resumedPage.continuation).toBe("256");
		expect(requests).toHaveLength(1);
	});

	it("announces paged finalize support when beginning an upload", async () => {
		let requestBody: Record<string, unknown> | undefined;
		const fetcher: typeof fetch = async (_input, init) => {
			requestBody = JSON.parse(String(init?.body)) as Record<string, unknown>;
			return Response.json({
				uploadId: "upload-id",
				chunkSize: 1,
				totalChunks: 300,
				poolSize: 32,
				sessionToken: "session-token",
				putEndpoint: "/api/vfs/multipart/upload-id",
				expiresAtMs: Date.now() + 60_000,
				landed: [],
				protocolVersion: MULTIPART_PROTOCOL_VERSION,
			});
		};
		const client = createMossaicHttpClient({
			url: "https://mossaic.test",
			apiKey: "test-token",
			fetcher,
		});

		await client.beginMultipartUpload("/large.bin", {
			size: 300,
			chunkSize: 1,
		});

		expect(requestBody).toMatchObject({
			path: "/large.bin",
			size: 300,
			capabilities: [MULTIPART_PAGED_CONTROL_CAPABILITY],
			protocolVersion: MULTIPART_PROTOCOL_VERSION,
		});
	});

	it("parallelUpload never re-PUTs a landed chunk beyond the sixteenth status page", async () => {
		const totalChunks = 4_097;
		let statusRequests = 0;
		let putRequests = 0;
		const fetcher: typeof fetch = async (input, init) => {
			const url = new URL(
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			);
			if (url.pathname.endsWith("/begin")) {
				return Response.json({
					uploadId: "resumed-upload",
					chunkSize: 1,
					totalChunks,
					poolSize: 65,
					sessionToken: "session-token",
					putEndpoint: "/api/vfs/multipart/resumed-upload",
					expiresAtMs: Date.now() + 60_000,
					landed: Array.from({ length: 256 }, (_, index) => index),
					continuation: "256",
					protocolVersion: MULTIPART_PROTOCOL_VERSION,
				});
			}
			if (url.pathname.endsWith("/status")) {
				statusRequests++;
				const start = Number(url.searchParams.get("continuation"));
				const end = Math.min(start + 256, totalChunks);
				return Response.json({
					landed: Array.from(
						{ length: end - start },
						(_, offset) => start + offset,
					),
					total: totalChunks,
					bytesUploaded: end - start,
					expiresAtMs: Date.now() + 60_000,
					...(end < totalChunks ? { continuation: String(end) } : {}),
				});
			}
			if (url.pathname.endsWith("/hash-page")) {
				const body = JSON.parse(String(init?.body)) as {
					startIndex: number;
					hashes: string[];
				};
				return Response.json({
					staged: body.startIndex + body.hashes.length,
					total: totalChunks,
				});
			}
			if (url.pathname.endsWith("/finalize-step")) {
				return Response.json({
					done: true,
					result: {
						fileId: "resumed-path-id",
						versionId: "",
						size: totalChunks,
						chunkCount: totalChunks,
						fileHash: "f".repeat(64),
						path: "/resumed.bin",
						mimeType: "application/octet-stream",
						isEncrypted: false,
					},
				});
			}
			putRequests++;
			throw new Error(`unexpected chunk PUT to ${url.pathname}`);
		};
		const client = createMossaicHttpClient({
			url: "https://mossaic.test",
			apiKey: "test-token",
			fetcher,
		});

		await expect(
			parallelUpload(
				client,
				"/resumed.bin",
				new Uint8Array(totalChunks),
				{ chunkSize: 1, resumeUploadId: "resumed-upload" },
			),
		).resolves.toMatchObject({ fileId: "resumed-path-id", size: totalChunks });
		expect(statusRequests).toBe(16);
		expect(putRequests).toBe(0);
	}, 20_000);

	it("falls back to legacy endpoints when an old server selects no capabilities", async () => {
		const paths: string[] = [];
		const fetcher: typeof fetch = async (input) => {
			const url = new URL(
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			);
			paths.push(url.pathname);
			if (url.pathname.endsWith("/begin")) {
				return Response.json({
					uploadId: "legacy-upload",
					chunkSize: 1,
					totalChunks: 1,
					poolSize: 32,
					sessionToken: "legacy-token",
					putEndpoint: "/api/vfs/multipart/legacy-upload",
					expiresAtMs: Date.now() + 60_000,
					landed: [],
				});
			}
			return Response.json({
				fileId: "legacy-path-id",
				versionId: "",
				size: 1,
				chunkCount: 1,
				fileHash: "f".repeat(64),
				path: "/legacy.bin",
				mimeType: "application/octet-stream",
				isEncrypted: false,
			});
		};
		const client = createMossaicHttpClient({
			url: "https://mossaic.test",
			apiKey: "test-token",
			fetcher,
		});

		const handle = await client.beginMultipartUpload("/legacy.bin", {
			size: 1,
			chunkSize: 1,
		});
		expect(handle.capabilities).toBeUndefined();
		expect(handle.protocolVersion).toBeUndefined();
		await expect(
			client.finalizeMultipartUpload(handle, ["a".repeat(64)]),
		).resolves.toMatchObject({ pathId: "legacy-path-id" });
		expect(paths).toEqual([
			"/api/vfs/multipart/begin",
			"/api/vfs/multipart/finalize",
		]);
	});

	it("stages hashes in pages of at most 256 and sends hash-free finalize steps", async () => {
		const requests: Array<{ pathname: string; body: Record<string, unknown> }> = [];
		let finalizeSteps = 0;
		const fetcher: typeof fetch = async (input, init) => {
			const url = new URL(
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			);
			const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
			requests.push({ pathname: url.pathname, body });
			if (url.pathname.endsWith("/hash-page")) {
				const hashes = body.hashes as string[];
				return Response.json({
					staged: (body.startIndex as number) + hashes.length,
					total: 600,
				});
			}
			finalizeSteps++;
			if (finalizeSteps === 1) {
				return Response.json({
					done: false,
					phase: "preparing",
					cursor: 255,
					total: 300,
				});
			}
			return Response.json({
				done: true,
				result: {
					fileId: "path-id",
					versionId: "",
					size: 600,
					chunkCount: 600,
					fileHash: "f".repeat(64),
					path: "/paged.bin",
					mimeType: "application/octet-stream",
					isEncrypted: false,
				},
			});
		};
		const client = createMossaicHttpClient({
			url: "https://mossaic.test",
			apiKey: "test-token",
			fetcher,
		});
		const handle: MultipartUploadHandle = {
			uploadId: "upload-id",
			path: "/paged.bin",
			chunkSize: 1,
			expectedChunks: 600,
			poolSize: 32,
			sessionToken: "session-token",
			expiresAtMs: Date.now() + 60_000,
			protocolVersion: MULTIPART_PROTOCOL_VERSION,
		};
		const hashes = Array.from({ length: 600 }, (_, index) =>
			index.toString(16).padStart(64, "0"),
		);

		await expect(
			client.finalizeMultipartUpload(handle, hashes),
		).resolves.toMatchObject({ pathId: "path-id", fileHash: "f".repeat(64) });
		expect(
			requests
				.filter((request) => request.pathname.endsWith("/hash-page"))
				.map((request) => (request.body.hashes as string[]).length),
		).toEqual([256, 256, 88]);
		expect(requests.slice(-2)).toEqual([
			{
				pathname: "/api/vfs/multipart/finalize-step",
				body: { uploadId: "upload-id" },
			},
			{
				pathname: "/api/vfs/multipart/finalize-step",
				body: { uploadId: "upload-id" },
			},
		]);
		expect(
			requests.some((request) => request.pathname.endsWith("/finalize")),
		).toBe(false);
	});

	it("uses legacy finalization when the server does not negotiate paging", async () => {
		const requests: Array<{ pathname: string; body: Record<string, unknown> }> = [];
		const fetcher: typeof fetch = async (input, init) => {
			const url = new URL(
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			);
			const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
			requests.push({ pathname: url.pathname, body });
			return Response.json({
				fileId: "legacy-path-id",
				versionId: "",
				size: 1,
				chunkCount: 1,
				fileHash: "f".repeat(64),
				path: "/legacy.bin",
				mimeType: "application/octet-stream",
				isEncrypted: false,
			});
		};
		const client = createMossaicHttpClient({
			url: "https://mossaic.test",
			apiKey: "test-token",
			fetcher,
		});
		const handle: MultipartUploadHandle = {
			uploadId: "legacy-upload-id",
			path: "/legacy.bin",
			chunkSize: 1,
			expectedChunks: 1,
			poolSize: 32,
			sessionToken: "session-token",
			expiresAtMs: Date.now() + 60_000,
		};
		const hashes = ["a".repeat(64)];

		await expect(
			client.finalizeMultipartUpload(handle, hashes),
		).resolves.toMatchObject({ pathId: "legacy-path-id" });
		expect(requests).toEqual([
			{
				pathname: "/api/vfs/multipart/finalize",
				body: { uploadId: "legacy-upload-id", chunkHashList: hashes },
			},
		]);
	});

	it("applies the shared retry budget to finalize and abort steps", async () => {
		let hashAttempts = 0;
		let abortAttempts = 0;
		const fetcher: typeof fetch = async (input, init) => {
			const url = new URL(
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			);
			if (url.pathname.endsWith("/hash-page")) {
				hashAttempts++;
				if (hashAttempts <= MULTIPART_OPERATION_RETRY_BUDGET) {
					throw new TypeError("fetch failed");
				}
				const body = JSON.parse(String(init?.body)) as {
					startIndex: number;
					hashes: string[];
				};
				return Response.json({
					staged: body.startIndex + body.hashes.length,
					total: 1,
				});
			}
			if (url.pathname.endsWith("/finalize-step")) {
				return Response.json({
					done: true,
					result: {
						fileId: "retry-path-id",
						versionId: "",
						size: 1,
						chunkCount: 1,
						fileHash: "f".repeat(64),
						path: "/retry.bin",
						mimeType: "application/octet-stream",
						isEncrypted: false,
					},
				});
			}
			abortAttempts++;
			if (abortAttempts <= MULTIPART_OPERATION_RETRY_BUDGET) {
				throw new TypeError("fetch failed");
			}
			return Response.json({ done: true });
		};
		const client = createMossaicHttpClient({
			url: "https://mossaic.test",
			apiKey: "test-token",
			fetcher,
		});
		const handle: MultipartUploadHandle = {
			uploadId: "retry-upload",
			path: "/retry.bin",
			chunkSize: 1,
			expectedChunks: 1,
			poolSize: 1,
			sessionToken: "session-token",
			expiresAtMs: Date.now() + 60_000,
			capabilities: [MULTIPART_PAGED_CONTROL_CAPABILITY],
		};

		await expect(
			client.startFinalizeMultipartUpload(handle, ["a".repeat(64)]),
		).resolves.toMatchObject({ pathId: "retry-path-id" });
		await expect(client.startAbortMultipartUpload(handle)).resolves.toEqual({
			aborted: true,
		});
		expect(hashAttempts).toBe(MULTIPART_OPERATION_RETRY_BUDGET + 1);
		expect(abortAttempts).toBe(MULTIPART_OPERATION_RETRY_BUDGET + 1);
	});

	it("keeps completion shapes stable and exposes bounded finalize and abort steps", async () => {
		const paths: string[] = [];
		const hashesLength = (MULTIPART_OPERATION_RPC_BUDGET + 1) * 256;
		let abortSteps = 0;
		const fetcher: typeof fetch = async (input, init) => {
			const url = new URL(
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			);
			paths.push(url.pathname);
			if (url.pathname.endsWith("/hash-page")) {
				const body = JSON.parse(String(init?.body)) as {
					startIndex: number;
					hashes: string[];
				};
				return Response.json({
					staged: body.startIndex + body.hashes.length,
					total: (MULTIPART_OPERATION_RPC_BUDGET + 1) * 256,
				});
			}
			if (url.pathname.endsWith("/finalize-step")) {
				return Response.json({
					done: true,
					result: {
						fileId: "bounded-path-id",
						versionId: "",
						size: hashesLength,
						chunkCount: hashesLength,
						fileHash: "f".repeat(64),
						path: "/bounded.bin",
						mimeType: "application/octet-stream",
						isEncrypted: false,
					},
				});
			}
			abortSteps++;
			if (abortSteps === MULTIPART_OPERATION_RPC_BUDGET + 1) {
				return Response.json({ done: true });
			}
			return Response.json({
				done: false,
				phase: "fencing",
				cursor: 1,
				total: 100,
			});
		};
		const client = createMossaicHttpClient({
			url: "https://mossaic.test",
			apiKey: "test-token",
			fetcher,
		});
		const handle: MultipartUploadHandle = {
			uploadId: "bounded-upload",
			path: "/bounded.bin",
			chunkSize: 1,
			expectedChunks: hashesLength,
			poolSize: 100,
			sessionToken: "session-token",
			expiresAtMs: Date.now() + 60_000,
			capabilities: [MULTIPART_PAGED_CONTROL_CAPABILITY],
		};
		const hashes = Array.from(
			{ length: hashesLength },
			() => "a".repeat(64),
		);

		await expect(
			client.finalizeMultipartUpload(handle, hashes),
		).rejects.toMatchObject({ code: "EFBIG" });
		expect(paths).toHaveLength(0);

		paths.length = 0;
		const pending = await client.startFinalizeMultipartUpload(handle, hashes);
		expect(pending).toEqual({
			operation: {
				kind: "multipart-finalize",
				uploadId: handle.uploadId,
				nextHashIndex: MULTIPART_OPERATION_RPC_BUDGET * 256,
			},
		});
		expect(paths).toHaveLength(MULTIPART_OPERATION_RPC_BUDGET);
		if (!("operation" in pending)) throw new Error("expected pending finalize");
		paths.length = 0;
		await expect(
			client.stepFinalizeMultipartUpload(handle, hashes, pending.operation),
		).resolves.toMatchObject({ pathId: "bounded-path-id" });
		expect(paths).toHaveLength(2);

		paths.length = 0;
		abortSteps = 0;
		const abortPending = await client.startAbortMultipartUpload(handle);
		expect(abortPending).toEqual({
			operation: { kind: "multipart-abort", uploadId: handle.uploadId },
		});
		expect(paths).toHaveLength(MULTIPART_OPERATION_RPC_BUDGET);
		if (!("operation" in abortPending)) throw new Error("expected pending abort");
		await expect(
			client.stepAbortMultipartUpload(handle, abortPending.operation),
		).resolves.toEqual({ aborted: true });

		paths.length = 0;
		abortSteps = 0;
		await expect(client.abortMultipartUpload(handle)).rejects.toMatchObject({
			code: "EFBIG",
		});
		expect(paths).toHaveLength(0);
	});

	it("collects complete status with monotonic progress and cancellation", async () => {
		let requests = 0;
		const controller = new AbortController();
		const progress: number[] = [];
		const fetcher: typeof fetch = async () => {
			requests++;
			return Response.json({
				landed: [requests - 1],
				total: 18,
				bytesUploaded: 1,
				expiresAtMs: Date.now() + 60_000,
				...(requests < 18 ? { continuation: `page-${requests + 1}` } : {}),
			});
		};
		const client = createMossaicHttpClient({
			url: "https://mossaic.test",
			apiKey: "test-token",
			fetcher,
		});
		const handle: MultipartUploadHandle = {
			uploadId: "status-upload",
			path: "/status.bin",
			chunkSize: 1,
			expectedChunks: 100,
			poolSize: 100,
			sessionToken: "session-token",
			expiresAtMs: Date.now() + 60_000,
			capabilities: [MULTIPART_PAGED_CONTROL_CAPABILITY],
		};

		await expect(
			client.getMultipartUploadStatus(handle, {
				onProgress: (event) => progress.push(event.requestsUsed),
			}),
		).rejects.toMatchObject({ code: "EFBIG" });
		expect(requests).toBe(DEFAULT_COMPLETION_REQUEST_BUDGET);
		expect(progress).toEqual(
			Array.from(
				{ length: DEFAULT_COMPLETION_REQUEST_BUDGET },
				(_, index) => index + 1,
			),
		);

		requests = 0;
		await expect(
			client.getMultipartUploadStatus(handle, {
				signal: controller.signal,
				onProgress: () =>
					controller.abort(new DOMException("cancelled", "AbortError")),
			}),
		).rejects.toMatchObject({ name: "AbortError" });
		expect(requests).toBe(1);
	});

	it("preflights completion boundaries before any HTTP mutation", async () => {
		const requests: string[] = [];
		const fetcher: typeof fetch = async (input, init) => {
			const url = new URL(
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			);
			requests.push(url.pathname);
			if (url.pathname.endsWith("/hash-page")) {
				const body = JSON.parse(String(init?.body)) as {
					startIndex: number;
					hashes: string[];
				};
				return Response.json({
					staged: body.startIndex + body.hashes.length,
					total: 1_024,
				});
			}
			if (url.pathname.endsWith("/finalize-step")) {
				return Response.json({
					done: true,
					result: {
						fileId: "path-id",
						versionId: "",
						size: 1_024,
						chunkCount: 1_024,
						fileHash: "f".repeat(64),
						path: "/boundary.bin",
						mimeType: "application/octet-stream",
						isEncrypted: false,
					},
				});
			}
			if (url.pathname.endsWith("/abort-step")) {
				return Response.json({ done: true });
			}
			if (url.pathname.endsWith("/status")) {
				const start = Number(url.searchParams.get("continuation") ?? 0);
				const end = Math.min(start + 256, 4_095);
				return Response.json({
					landed: Array.from(
						{ length: end - start },
						(_, offset) => start + offset,
					),
					total: 4_095,
					bytesUploaded: end - start,
					expiresAtMs: Date.now() + 60_000,
					...(end < 4_095 ? { continuation: String(end) } : {}),
				});
			}
			throw new Error(`unexpected request to ${url.pathname}`);
		};
		const client = createMossaicHttpClient({
			url: "https://mossaic.test",
			apiKey: "test-token",
			fetcher,
		});
		const handle = (expectedChunks: number): MultipartUploadHandle => ({
			uploadId: `upload-${expectedChunks}`,
			path: "/boundary.bin",
			chunkSize: 1,
			expectedChunks,
			poolSize: 1,
			sessionToken: "session-token",
			expiresAtMs: Date.now() + 60_000,
			capabilities: [MULTIPART_PAGED_CONTROL_CAPABILITY],
		});

		const hashesAtCap = Array.from({ length: 1_024 }, () => "a".repeat(64));
		await expect(
			client.finalizeMultipartUpload(handle(1_024), hashesAtCap),
		).resolves.toMatchObject({ pathId: "path-id" });
		expect(requests).toHaveLength(5);

		requests.length = 0;
		const hashesAboveCap = [...hashesAtCap, "b".repeat(64)];
		const finalizeError = await client
			.finalizeMultipartUpload(handle(1_025), hashesAboveCap)
			.catch((error: Error) => error);
		expect(finalizeError).toBeInstanceOf(EFBIG);
		expect(finalizeError).toMatchObject({ code: "EFBIG" });
		expect(String(finalizeError)).toContain("startFinalizeMultipartUpload()");
		expect(requests).toHaveLength(0);

		await expect(client.abortMultipartUpload(handle(3_328))).resolves.toEqual({
			aborted: true,
		});
		expect(requests).toEqual(["/api/vfs/multipart/abort-step"]);

		requests.length = 0;
		await expect(
			client.abortMultipartUpload(handle(3_329)),
		).rejects.toMatchObject({ code: "EFBIG" });
		expect(requests).toHaveLength(0);

		await expect(
			client.getMultipartUploadStatus(handle(4_095)),
		).resolves.toMatchObject({ total: 4_095 });
		expect(requests).toHaveLength(DEFAULT_COMPLETION_REQUEST_BUDGET);

		requests.length = 0;
		await expect(
			client.getMultipartUploadStatus(handle(4_096)),
		).rejects.toMatchObject({ code: "EFBIG" });
		await expect(
			client.resumeMultipartUpload(handle(4_096)),
		).rejects.toMatchObject({ code: "EFBIG" });
		const controller = new AbortController();
		controller.abort(new DOMException("cancelled", "AbortError"));
		await expect(
			client.finalizeMultipartUpload(handle(1_025), hashesAboveCap, {
				signal: controller.signal,
			}),
		).rejects.toMatchObject({ name: "AbortError" });
		expect(requests).toHaveLength(0);
	});
});
