import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { createVFS, type MossaicEnv } from "../../sdk/src/index";
import type { UserDO } from "@app/objects/user/user-do";
import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";
import {
	MULTIPART_PROTOCOL_VERSION,
	MULTIPART_STATUS_ENTRY_PAGE_SIZE,
} from "@shared/multipart";

interface TestEnv {
	MOSSAIC_USER: DurableObjectNamespace<UserDO>;
	MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}

const E = env as unknown as TestEnv;

function userStub(tenant: string): DurableObjectStub<UserDO> {
	return E.MOSSAIC_USER.get(
		E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant)),
	);
}

function shardStub(tenant: string, shardIndex: number): DurableObjectStub<ShardDO> {
	return E.MOSSAIC_SHARD.get(
		E.MOSSAIC_SHARD.idFromName(
			vfsShardDOName("default", tenant, undefined, shardIndex),
		),
	);
}

async function setPoolSize(
	user: DurableObjectStub<UserDO>,
	uploadId: string,
	poolSize: number,
): Promise<void> {
	await runInDurableObject(user, (instance) => {
		instance.sql.exec(
			"UPDATE upload_sessions SET pool_size = ? WHERE upload_id = ?",
			poolSize,
			uploadId,
		);
	});
}

async function seedSingleShardRows(
	shard: DurableObjectStub<ShardDO>,
	uploadId: string,
	tenant: string,
	count: number,
): Promise<void> {
	await shard.getMultipartLanded(uploadId);
	await runInDurableObject(shard, (instance) => {
		for (let index = 0; index < count; index++) {
			instance.sql.exec(
				`INSERT INTO upload_chunks
				   (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
				 VALUES (?, ?, ?, 1, ?, ?)`,
				uploadId,
				index,
				index.toString(16).padStart(64, "0"),
				tenant,
				Date.now(),
			);
		}
	});
}

describe("multipart status paging", () => {
	it("pages more than 256 landed rows without an unbounded shard read", async () => {
		const tenant = "multipart-status-dense";
		const user = userStub(tenant);
		const scope = { ns: "default", tenant } as const;
		const begin = await user.vfsBeginMultipart(scope, "/dense.bin", {
			size: 300,
			chunkSize: 1,
			protocolVersion: MULTIPART_PROTOCOL_VERSION,
		});
		await setPoolSize(user, begin.uploadId, 1);
		const shard = shardStub(tenant, 0);
		await seedSingleShardRows(shard, begin.uploadId, tenant, 300);

		const first = await user.vfsGetMultipartStatus(scope, begin.uploadId);
		expect(first.landed).toHaveLength(MULTIPART_STATUS_ENTRY_PAGE_SIZE);
		expect(first.bytesUploaded).toBe(MULTIPART_STATUS_ENTRY_PAGE_SIZE);
		expect(first.continuation).toEqual(expect.any(String));
		const second = await user.vfsGetMultipartStatus(
			scope,
			begin.uploadId,
			first.continuation,
		);
		expect(second).toMatchObject({
			landed: Array.from({ length: 44 }, (_, offset) => 256 + offset),
			bytesUploaded: 44,
		});
		expect(second.continuation).toBeUndefined();

		const manifestFirst = await shard.getMultipartManifest(
			begin.uploadId,
			-1,
			MULTIPART_STATUS_ENTRY_PAGE_SIZE,
		);
		const manifestSecond = await shard.getMultipartManifest(
			begin.uploadId,
			manifestFirst.rows.at(-1)?.idx ?? -1,
			MULTIPART_STATUS_ENTRY_PAGE_SIZE,
		);
		expect(manifestFirst.rows).toHaveLength(256);
		expect(manifestSecond.rows).toHaveLength(44);
		await expect(
			shard.getMultipartLanded(begin.uploadId, -1, 257),
		).rejects.toThrow(/limit 1\.\.256/);
	});

	it("limits one empty-pool scan to 64 shards and validates cursor scope", async () => {
		const tenant = "multipart-status-wide";
		const user = userStub(tenant);
		const scope = { ns: "default", tenant } as const;
		const begin = await user.vfsBeginMultipart(scope, "/wide.bin", {
			size: 0,
			chunkSize: 1,
			protocolVersion: MULTIPART_PROTOCOL_VERSION,
		});
		await setPoolSize(user, begin.uploadId, 65);

		const first = await user.vfsGetMultipartStatus(scope, begin.uploadId);
		expect(first).toMatchObject({ landed: [], bytesUploaded: 0 });
		expect(first.continuation).toEqual(expect.any(String));
		const second = await user.vfsGetMultipartStatus(
			scope,
			begin.uploadId,
			first.continuation,
		);
		expect(second.continuation).toBeUndefined();

		const other = await user.vfsBeginMultipart(scope, "/other.bin", {
			size: 0,
			chunkSize: 1,
			protocolVersion: MULTIPART_PROTOCOL_VERSION,
		});
		await expect(
			user.vfsGetMultipartStatus(scope, other.uploadId, first.continuation),
		).rejects.toThrow(/invalid continuation for this upload and tenant/);
		await expect(
			user.vfsGetMultipartStatus(scope, begin.uploadId, begin.sessionToken),
		).rejects.toThrow(/invalid continuation/);
		const continuation = first.continuation ?? "";
		const tampered = `${continuation.slice(0, -1)}${continuation.endsWith("a") ? "b" : "a"}`;
		await expect(
			user.vfsGetMultipartStatus(scope, begin.uploadId, tampered),
		).rejects.toThrow(/invalid continuation/);

		const otherTenant = "multipart-status-other-tenant";
		await expect(
			userStub(otherTenant).vfsGetMultipartStatus(
				{ ns: "default", tenant: otherTenant },
				begin.uploadId,
				first.continuation,
			),
		).rejects.toThrow(/invalid continuation for this upload and tenant/);
	});

	it("binding SDK exposes one page and collects small multi-page scans", async () => {
		const tenant = "multipart-status-binding-sdk";
		const sdkEnv: MossaicEnv = {
			MOSSAIC_USER: E.MOSSAIC_USER,
			MOSSAIC_SHARD: E.MOSSAIC_SHARD,
		};
		const vfs = createVFS(sdkEnv, { tenant });
		const handle = await vfs.beginMultipartUpload("/sdk.bin", {
			size: 300,
			chunkSize: 1,
		});
		const user = userStub(tenant);
		await setPoolSize(user, handle.uploadId, 1);
		await seedSingleShardRows(shardStub(tenant, 0), handle.uploadId, tenant, 300);

		const page = await vfs.getMultipartUploadStatusPage(handle);
		expect(page.landed).toHaveLength(256);
		expect(page.continuation).toEqual(expect.any(String));
		await expect(vfs.getMultipartUploadStatus(handle)).resolves.toMatchObject({
			landed: Array.from({ length: 300 }, (_, index) => index),
			bytesUploaded: 300,
		});
	});

	it("does not serialize 32 MiB of staged hashes into status or resume", async () => {
		const tenant = "multipart-status-32mib";
		const user = userStub(tenant);
		const scope = { ns: "default", tenant } as const;
		const begin = await user.vfsBeginMultipart(scope, "/large-hash.bin", {
			size: 1_024,
			chunkSize: 1,
			protocolVersion: MULTIPART_PROTOCOL_VERSION,
		});
		await setPoolSize(user, begin.uploadId, 1);
		const shard = shardStub(tenant, 0);
		await shard.getMultipartLanded(begin.uploadId);
		await runInDurableObject(shard, (instance) => {
			const largeHash = "a".repeat(32 * 1024);
			for (let index = 0; index < 1_024; index++) {
				instance.sql.exec(
					`INSERT INTO upload_chunks
					   (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
					 VALUES (?, ?, ?, 1, ?, ?)`,
					begin.uploadId,
					index,
					largeHash,
					tenant,
					Date.now(),
				);
			}
		});

		await expect(
			user.vfsGetMultipartStatus(scope, begin.uploadId),
		).resolves.toMatchObject({
			landed: Array.from({ length: 256 }, (_, index) => index),
			bytesUploaded: 256,
		});
		await expect(
			user.vfsBeginMultipart(scope, "/large-hash.bin", {
				size: 1_024,
				chunkSize: 1,
				protocolVersion: MULTIPART_PROTOCOL_VERSION,
				resumeFrom: begin.uploadId,
			}),
		).resolves.toMatchObject({
			landed: Array.from({ length: 256 }, (_, index) => index),
		});
	}, 30_000);
});
