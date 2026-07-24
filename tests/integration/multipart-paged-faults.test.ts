import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import type { UserDO } from "@app/objects/user/user-do";
import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";
import { sweepExpiredMultipartSessions } from "@core/objects/user/multipart-upload";
import { hashChunk } from "@shared/crypto";
import { MULTIPART_PLACEMENT_VERSION } from "@shared/multipart";
import { placeChunk, placeMultipartChunk } from "@shared/placement";

interface FaultUserDO extends UserDO {
	testEvict(): Promise<void>;
	testConfigureFinalizeStepResponseLoss(remaining: number): Promise<void>;
	testConfigureFinalizeResponseLoss(remaining: number): Promise<void>;
	testConfigureMultipartStatusResponseLoss(remaining: number): Promise<void>;
}

interface FaultShardDO extends ShardDO {
	testConfigureFenceMultipartFailure(
		uploadId: string,
		remaining: number,
	): Promise<void>;
}

interface TestEnv {
	MOSSAIC_USER: DurableObjectNamespace<FaultUserDO>;
	MOSSAIC_SHARD: DurableObjectNamespace<FaultShardDO>;
}

const E = env as unknown as TestEnv;
const hash = (value: number): string => value.toString(16).padStart(64, "0");

function userStub(tenant: string): DurableObjectStub<FaultUserDO> {
	return E.MOSSAIC_USER.get(
		E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant)),
	);
}

describe("paged multipart fault recovery", () => {
	it("backs off repeated fence failures without poisoning and resets progress on recovery", async () => {
		const tenant = "multipart-fence-retry-recovery";
		const scope = { ns: "default", tenant } as const;
		const user = userStub(tenant);
		const begin = await user.vfsBeginMultipart(scope, "/fence-retry.bin", {
			size: 0,
			protocolVersion: 2,
		});
		await runInDurableObject(user, (_instance, state) => {
			state.storage.sql.exec(
				`UPDATE upload_sessions
				    SET pool_size = 1, expires_at = 0
				  WHERE upload_id = ?`,
				begin.uploadId,
			);
		});
		const shard = E.MOSSAIC_SHARD.get(
			E.MOSSAIC_SHARD.idFromName(
				vfsShardDOName("default", tenant, undefined, 0),
			),
		);
		await shard.testConfigureFenceMultipartFailure(begin.uploadId, 5);

		for (let attempt = 1; attempt <= 5; attempt++) {
			await runInDurableObject(user, (_instance, state) => {
				state.storage.sql.exec(
					"UPDATE upload_sessions SET abort_retry_at = 0 WHERE upload_id = ?",
					begin.uploadId,
				);
			});
			await runInDurableObject(user, (instance) =>
				sweepExpiredMultipartSessions(instance, () => scope),
			);
			await expect(
				runInDurableObject(user, (_instance, state) =>
					state.storage.sql
						.exec(
							`SELECT status, attempts, abort_retry_at
							   FROM upload_sessions WHERE upload_id = ?`,
							begin.uploadId,
						)
						.toArray()[0],
				),
			).resolves.toMatchObject({
				status: "aborting",
				attempts: attempt,
				abort_retry_at: expect.any(Number),
			});
		}

		await runInDurableObject(user, (_instance, state) => {
			state.storage.sql.exec(
				"UPDATE upload_sessions SET abort_retry_at = 0 WHERE upload_id = ?",
				begin.uploadId,
			);
		});
		await runInDurableObject(user, (instance) =>
			sweepExpiredMultipartSessions(instance, () => scope),
		);
		await expect(
			runInDurableObject(user, (_instance, state) =>
				state.storage.sql
					.exec(
						"SELECT status, attempts, abort_retry_at FROM upload_sessions WHERE upload_id = ?",
						begin.uploadId,
					)
					.toArray()[0],
			),
		).resolves.toEqual({ status: "aborted", attempts: 0, abort_retry_at: 0 });
	});

	it("replays the same continuation after a status-page response is lost", async () => {
		const tenant = "paged-status-response-loss";
		const scope = { ns: "default", tenant } as const;
		const user = userStub(tenant);
		const begin = await user.vfsBeginMultipart(scope, "/status-loss.bin", {
			size: 300,
			chunkSize: 1,
			protocolVersion: 2,
		});
		await runInDurableObject(user, (instance) => {
			instance.sql.exec(
				"UPDATE upload_sessions SET pool_size = 1 WHERE upload_id = ?",
				begin.uploadId,
			);
		});
		const shard = E.MOSSAIC_SHARD.get(
			E.MOSSAIC_SHARD.idFromName(
				vfsShardDOName("default", tenant, undefined, 0),
			),
		);
		await shard.getMultipartLanded(begin.uploadId);
		await runInDurableObject(shard, (_instance, state) => {
			for (let index = 0; index < 300; index++) {
				state.storage.sql.exec(
					`INSERT INTO upload_chunks
					   (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
					 VALUES (?, ?, ?, 1, ?, ?)`,
					begin.uploadId,
					index,
					hash(index + 1),
					tenant,
					Date.now(),
				);
			}
		});

		const first = await user.vfsGetMultipartStatus(scope, begin.uploadId);
		expect(first.landed).toHaveLength(256);
		expect(first.continuation).toEqual(expect.any(String));
		await user.testConfigureMultipartStatusResponseLoss(1);
		await expect(
			user.vfsGetMultipartStatus(scope, begin.uploadId, first.continuation),
		).rejects.toThrow(/status response loss/);
		await expect(
			user.vfsGetMultipartStatus(scope, begin.uploadId, first.continuation),
		).resolves.toMatchObject({
			landed: Array.from({ length: 44 }, (_, offset) => 256 + offset),
			bytesUploaded: 44,
		});
	});

	it("resumes a 300-chunk finalize after UserDO eviction and replays its terminal result", async () => {
		const tenant = "paged-finalize-eviction";
		const scope = { ns: "default", tenant } as const;
		let user = userStub(tenant);
		const begin = await user.vfsBeginMultipart(scope, "/evicted.bin", {
			size: 300,
			chunkSize: 1,
			protocolVersion: 2,
		});
		const hashes = Array.from({ length: 300 }, (_, index) => hash(index + 1));
		await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, hashes.slice(0, 256));
		await user.vfsStageMultipartHashes(scope, begin.uploadId, 256, hashes.slice(256));
		await runInDurableObject(user, (_instance, state) => {
			state.storage.sql.exec(
				"UPDATE upload_sessions SET pool_size = 1 WHERE upload_id = ?",
				begin.uploadId,
			);
		});
		const shard = E.MOSSAIC_SHARD.get(
			E.MOSSAIC_SHARD.idFromName(
				vfsShardDOName("default", tenant, undefined, 0),
			),
		);
		await shard.getMultipartManifest(begin.uploadId);
		await runInDurableObject(shard, (_instance, state) => {
			for (let index = 0; index < hashes.length; index++) {
				state.storage.sql.exec(
					`INSERT INTO upload_chunks
					   (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
					 VALUES (?, ?, ?, 1, ?, ?)`,
					begin.uploadId,
					index,
					hashes[index],
					tenant,
					Date.now(),
				);
			}
		});

		await user.vfsFinalizeMultipartStep(scope, begin.uploadId);
		await expect(
			user.vfsFinalizeMultipartStep(scope, begin.uploadId),
		).resolves.toMatchObject({ done: false, cursor: 256 });
		await expect(user.testEvict()).rejects.toThrow(/injected UserDO eviction/);
		await expect(
			user.vfsFinalizeMultipartStep(scope, begin.uploadId),
		).rejects.toThrow(/injected UserDO eviction/);

		user = userStub(tenant);
		await expect(
			user.vfsFinalizeMultipartStep(scope, begin.uploadId),
		).resolves.toMatchObject({ done: false, phase: "publishing", cursor: 300 });
		await expect(
			user.vfsFinalizeMultipartStep(scope, begin.uploadId),
		).resolves.toEqual({
			done: false,
			phase: "cleaning",
			cursor: 0,
			total: 300,
		});
		await expect(
			user.vfsFinalizeMultipartStep(scope, begin.uploadId),
		).resolves.toEqual({
			done: false,
			phase: "cleaning",
			cursor: 0,
			total: 1,
		});
		await expect(
			user.vfsFinalizeMultipartStep(scope, begin.uploadId),
		).resolves.toEqual({
			done: false,
			phase: "cleaning",
			cursor: 256,
			total: 300,
		});
		const completed = await user.vfsFinalizeMultipartStep(scope, begin.uploadId);
		expect(completed).toEqual({
			done: true,
			fresh: true,
			result: expect.objectContaining({
				fileId: begin.uploadId,
				size: 300,
				fileHash: await hashChunk(
					new TextEncoder().encode(hashes.join("")),
				),
			}),
		});
		await expect(
			user.vfsFinalizeMultipartStep(scope, begin.uploadId),
		).resolves.toEqual({ ...completed, fresh: false });
	});

	it("returns the persisted legacy finalize result after response loss", async () => {
		const tenant = "legacy-finalize-response-loss";
		const scope = { ns: "default", tenant } as const;
		const user = userStub(tenant);
		const begin = await user.vfsBeginMultipart(scope, "/legacy-loss.bin", {
			size: 1,
			chunkSize: 1,
		});
		const bytes = new Uint8Array([71]);
		const chunkHash = await hashChunk(bytes);
		const shardIndex = placeChunk(
			tenant,
			begin.uploadId,
			0,
			begin.poolSize,
		);
		const shard = E.MOSSAIC_SHARD.get(
			E.MOSSAIC_SHARD.idFromName(
				vfsShardDOName("default", tenant, undefined, shardIndex),
			),
		);
		await shard.putChunkMultipart(
			chunkHash,
			bytes,
			begin.uploadId,
			0,
			tenant,
			begin.sessionToken,
		);
		await user.testConfigureFinalizeResponseLoss(1);

		await expect(
			user.vfsFinalizeMultipart(scope, begin.uploadId, [chunkHash]),
		).rejects.toThrow(/legacy finalize response loss after mutation/);
		const persisted = await runInDurableObject(user, (_instance, state) => {
			return state.storage.sql
				.exec(
					"SELECT status, finalize_result FROM upload_sessions WHERE upload_id = ?",
					begin.uploadId,
				)
				.toArray()[0] as { status: string; finalize_result: string };
		});
		expect(persisted.status).toBe("finalized");
		expect(persisted.finalize_result).toEqual(expect.any(String));

		await expect(
			user.vfsFinalizeMultipart(scope, begin.uploadId, [chunkHash]),
		).resolves.toEqual(JSON.parse(persisted.finalize_result));
	});

	it("returns a 513-chunk overwrite result immediately after publication and replays response loss", async () => {
		const tenant = "legacy-overwrite-513-response-loss";
		const scope = { ns: "default", tenant } as const;
		const user = userStub(tenant);
		await user.vfsWriteFile(scope, "/legacy-overwrite.bin", new Uint8Array([1]));
		const oldFileId = await runInDurableObject(user, (_instance, state) => {
			const sql = state.storage.sql;
			const file = sql
				.exec(
					"SELECT file_id FROM files WHERE user_id = ? AND file_name = 'legacy-overwrite.bin'",
					tenant,
				)
				.toArray()[0] as { file_id: string };
			sql.exec(
				`UPDATE files SET inline_data = NULL, file_size = 513, chunk_count = 513
				  WHERE file_id = ?`,
				file.file_id,
			);
			for (let index = 0; index < 513; index++) {
				sql.exec(
					`INSERT INTO file_chunks
					   (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
					 VALUES (?, ?, ?, 1, ?)`,
					file.file_id,
					index,
					hash(60_000 + index),
					index % 4,
				);
			}
			return file.file_id;
		});
		const begin = await user.vfsBeginMultipart(scope, "/legacy-overwrite.bin", {
			size: 1,
			chunkSize: 1,
		});
		const bytes = new Uint8Array([72]);
		const chunkHash = await hashChunk(bytes);
		const shardIndex = placeChunk(
			tenant,
			begin.uploadId,
			0,
			begin.poolSize,
		);
		await E.MOSSAIC_SHARD.get(
			E.MOSSAIC_SHARD.idFromName(
				vfsShardDOName("default", tenant, undefined, shardIndex),
			),
		).putChunkMultipart(
			chunkHash,
			bytes,
			begin.uploadId,
			0,
			tenant,
			begin.sessionToken,
		);
		await user.testConfigureFinalizeResponseLoss(1);

		await expect(
			user.vfsFinalizeMultipart(scope, begin.uploadId, [chunkHash]),
		).rejects.toThrow(/legacy finalize response loss after mutation/);
		const persisted = await runInDurableObject(user, (_instance, state) => ({
			session: state.storage.sql
				.exec(
					"SELECT status, finalize_phase, finalize_result FROM upload_sessions WHERE upload_id = ?",
					begin.uploadId,
				)
				.toArray()[0] as {
				status: string;
				finalize_phase: string;
				finalize_result: string;
			},
			oldChunks: (
				state.storage.sql
					.exec(
						"SELECT COUNT(*) AS n FROM file_chunks WHERE file_id = ?",
						oldFileId,
					)
					.toArray()[0] as { n: number }
			).n,
		}));
		expect(persisted).toMatchObject({
			session: {
				status: "finalized",
				finalize_phase: "cleaning_upload_intents",
				finalize_result: expect.any(String),
			},
			oldChunks: 513,
		});
		await expect(
			user.vfsFinalizeMultipart(scope, begin.uploadId, [chunkHash]),
		).resolves.toEqual(JSON.parse(persisted.session.finalize_result));
		await expect(user.vfsStat(scope, "/legacy-overwrite.bin")).resolves.toMatchObject({
			size: 1,
		});
	});

	it("replays overwrite preparation and post-publication cleanup after lost responses", async () => {
		const tenant = "paged-overwrite-response-loss";
		const scope = { ns: "default", tenant } as const;
		const user = userStub(tenant);
		await user.vfsWriteFile(scope, "/overwrite.bin", new Uint8Array([1]));
		const oldFileId = await runInDurableObject(user, (_instance, state) => {
			const sql = state.storage.sql;
			const file = sql
				.exec("SELECT file_id FROM files WHERE user_id = ? AND file_name = 'overwrite.bin'", tenant)
				.toArray()[0] as { file_id: string };
			sql.exec(
				"UPDATE files SET inline_data = NULL, file_size = 300, chunk_count = 300 WHERE file_id = ?",
				file.file_id,
			);
			for (let index = 0; index < 300; index++) {
				sql.exec(
					`INSERT INTO file_chunks
					   (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
					 VALUES (?, ?, ?, 1, ?)`,
					file.file_id,
					index,
					hash(40_000 + index),
					index % 4,
				);
			}
			return file.file_id;
		});
		const begin = await user.vfsBeginMultipart(scope, "/overwrite.bin", {
			size: 1,
			chunkSize: 1,
			protocolVersion: 2,
		});
		const chunkHash = hash(50_000);
		await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, [chunkHash]);
		const shardIndex = placeMultipartChunk(
			tenant,
			begin.uploadId,
			0,
			begin.poolSize,
			MULTIPART_PLACEMENT_VERSION,
		);
		const shard = E.MOSSAIC_SHARD.get(
			E.MOSSAIC_SHARD.idFromName(
				vfsShardDOName("default", tenant, undefined, shardIndex),
			),
		);
		await shard.getMultipartManifest(begin.uploadId);
		await runInDurableObject(shard, (_instance, state) => {
			state.storage.sql.exec(
				`INSERT INTO upload_chunks
				   (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
				 VALUES (?, 0, ?, 1, ?, ?)`,
				begin.uploadId,
				chunkHash,
				tenant,
				Date.now(),
			);
		});

		await user.vfsFinalizeMultipartStep(scope, begin.uploadId);
		await expect(
			user.vfsFinalizeMultipartStep(scope, begin.uploadId),
		).resolves.toMatchObject({ phase: "preparing" });
		await user.testConfigureFinalizeStepResponseLoss(1);
		await expect(
			user.vfsFinalizeMultipartStep(scope, begin.uploadId),
		).rejects.toThrow(/finalize-step response loss after mutation/);
		await expect(
			runInDurableObject(user, (_instance, state) =>
				state.storage.sql
					.exec(
						"SELECT finalize_phase, finalize_old_manifest_cursor FROM upload_sessions WHERE upload_id = ?",
						begin.uploadId,
					)
					.toArray()[0],
			),
		).resolves.toEqual({
			finalize_phase: "preparing",
			finalize_old_manifest_cursor: 255,
		});
		await expect(
			user.vfsFinalizeMultipartStep(scope, begin.uploadId),
		).resolves.toMatchObject({ phase: "publishing", cursor: 299 });

		await user.testConfigureFinalizeStepResponseLoss(1);
		await expect(
			user.vfsFinalizeMultipartStep(scope, begin.uploadId),
		).rejects.toThrow(/finalize-step response loss after mutation/);
		await expect(
			runInDurableObject(user, (_instance, state) =>
				state.storage.sql
					.exec(
						"SELECT status, finalize_phase FROM upload_sessions WHERE upload_id = ?",
						begin.uploadId,
					)
					.toArray()[0],
			),
		).resolves.toEqual({
			status: "finalized",
			finalize_phase: "cleaning_upload_intents",
		});

		await runInDurableObject(user, (instance) => instance.alarm());
		await runInDurableObject(user, (instance) => instance.alarm());
		await runInDurableObject(user, (instance) => instance.alarm());
		await expect(
			runInDurableObject(user, (_instance, state) => {
				const sql = state.storage.sql;
				return {
					phase: (
						sql
							.exec(
								"SELECT finalize_phase FROM upload_sessions WHERE upload_id = ?",
								begin.uploadId,
							)
							.toArray()[0] as { finalize_phase: string }
					).finalize_phase,
					oldChunks: (
						sql
							.exec(
								"SELECT COUNT(*) AS n FROM file_chunks WHERE file_id = ?",
								oldFileId,
							)
							.toArray()[0] as { n: number }
					).n,
				};
			}),
		).resolves.toEqual({ phase: "cleaning_old_manifest", oldChunks: 44 });

		for (let alarm = 0; alarm < 3; alarm++) {
			await runInDurableObject(user, (instance) => instance.alarm());
		}
		await expect(
			runInDurableObject(user, (_instance, state) => {
				const sql = state.storage.sql;
				return {
					phase: (
						sql
							.exec(
								"SELECT finalize_phase FROM upload_sessions WHERE upload_id = ?",
								begin.uploadId,
							)
							.toArray()[0] as { finalize_phase: string }
					).finalize_phase,
					oldChunks: (
						sql
							.exec("SELECT COUNT(*) AS n FROM file_chunks WHERE file_id = ?", oldFileId)
							.toArray()[0] as { n: number }
					).n,
					routes: (
						sql
							.exec(
								"SELECT COUNT(*) AS n FROM upload_cleanup_routes WHERE upload_id = ?",
								begin.uploadId,
							)
							.toArray()[0] as { n: number }
					).n,
				};
			}),
		).resolves.toEqual({ phase: "done", oldChunks: 0, routes: 0 });
	});
});
