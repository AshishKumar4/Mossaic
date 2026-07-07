import { describe, expect, it } from "vitest";
import { z, ZodError } from "zod/v4";

import { PatchMetadataIfHeadRequestSchema } from "../../shared/patch-metadata-if-head";
import { JsonObjectSchema, JsonValueSchema } from "../../shared/schemas/json";
import { parseRequestBody, parseResponse } from "../../shared/schemas/parse";
import { VFSError } from "../../shared/vfs-types";

describe("JsonValueSchema", () => {
	it("accepts nested JSON values", () => {
		const value = {
			name: "photo",
			count: 3,
			published: false,
			deletedAt: null,
			tags: ["raw", "edited"],
			camera: { make: "mossaic", iso: 100 },
		};

		expect(JsonValueSchema.parse(value)).toEqual(value);
		expect(JsonObjectSchema.parse(value)).toEqual(value);
	});

	it("rejects values that cannot be represented as JSON", () => {
		expect(() => JsonValueSchema.parse(Number.NaN)).toThrowError();
		expect(() => JsonValueSchema.parse(Number.POSITIVE_INFINITY)).toThrowError();
		expect(() => JsonValueSchema.parse({ bad: undefined })).toThrowError(/bad/);
		expect(() => JsonValueSchema.parse({ bad: () => undefined })).toThrowError(/bad/);
	});
});

describe("schema parse helpers", () => {
	it("converts request parse failures into VFSError EINVAL", () => {
		expect(() =>
			parseRequestBody(
				PatchMetadataIfHeadRequestSchema,
				{ pathId: "", expectedHeadVersionId: null, patch: null },
				"patchMetadataIfHead",
			),
		).toThrow(VFSError);

		try {
			parseRequestBody(
				PatchMetadataIfHeadRequestSchema,
				{ pathId: "", expectedHeadVersionId: null, patch: null },
				"patchMetadataIfHead",
			);
			expect.unreachable("parseRequestBody should throw");
		} catch (err) {
			expect(err).toBeInstanceOf(VFSError);
			expect((err as { code?: unknown }).code).toBe("EINVAL");
			expect(String((err as Error).message)).toContain(
				"patchMetadataIfHead: invalid request body",
			);
		}
	});

	it("parses SDK responses with the provided schema", () => {
		const schema = z.object({ ok: z.boolean() });

		expect(parseResponse(schema, { ok: true }, "foo")).toEqual({ ok: true });

		try {
			parseResponse(schema, { ok: "yes" }, "foo");
			expect.unreachable("parseResponse should throw");
		} catch (err) {
			expect(err).toBeInstanceOf(TypeError);
			expect(String((err as Error).message)).toContain(
				"foo: invalid response body",
			);
			expect((err as Error & { cause?: unknown }).cause).toBeInstanceOf(ZodError);
		}
	});
});

describe("PatchMetadataIfHeadRequestSchema", () => {
	it("requires the patch to be a JSON object when present", () => {
		expect(
			PatchMetadataIfHeadRequestSchema.parse({
				pathId: "p-1",
				expectedHeadVersionId: null,
				patch: { title: "One", nested: { rating: 5 } },
			}),
		).toEqual({
			pathId: "p-1",
			expectedHeadVersionId: null,
			patch: { title: "One", nested: { rating: 5 } },
		});

		expect(() =>
			PatchMetadataIfHeadRequestSchema.parse({
				pathId: "p-1",
				expectedHeadVersionId: null,
				patch: { title: undefined },
			}),
		).toThrowError(/title/);
	});
});
