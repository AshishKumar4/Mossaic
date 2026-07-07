import { z } from "zod/v4";

import { JsonObjectSchema } from "./schemas/json";
import { parseResponse } from "./schemas/parse";

export const PatchMetadataIfHeadPatchSchema = JsonObjectSchema;

export const PatchMetadataIfHeadOptsSchema = z.object({
	addTags: z.array(z.string()).optional(),
	removeTags: z.array(z.string()).optional(),
});

export const PatchMetadataIfHeadRequestSchema = z.object({
	pathId: z.string().min(1),
	expectedHeadVersionId: z.string().nullable(),
	patch: PatchMetadataIfHeadPatchSchema.nullable(),
	opts: PatchMetadataIfHeadOptsSchema.optional(),
});

export type PatchMetadataIfHeadRequest = z.infer<
	typeof PatchMetadataIfHeadRequestSchema
>;

export const PatchMetadataIfHeadResultSchema = z.object({
	patched: z.boolean(),
	headVersionId: z.string().nullable(),
});

export type PatchMetadataIfHeadResult = z.infer<
	typeof PatchMetadataIfHeadResultSchema
>;

export function parsePatchMetadataIfHeadResult(
	raw: unknown,
): PatchMetadataIfHeadResult {
	return parseResponse(PatchMetadataIfHeadResultSchema, raw, "patchMetadataIfHead");
}
