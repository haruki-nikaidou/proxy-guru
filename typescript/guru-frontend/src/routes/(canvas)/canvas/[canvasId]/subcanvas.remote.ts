/**
 * Subcanvases: canvases drawn inside another one. A subcanvas only organises
 * the drawing — edges cross canvas boundaries freely — so creating, renaming or
 * deleting one changes the tree, not the traffic.
 */
import * as v from 'valibot';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { coordSchema, nameSchema } from '#lib/server/topology/schemas.js';
import { command } from '$app/server';
import { refreshTreeViews } from './refresh.js';

const descriptionSchema = v.optional(
	v.pipe(v.string(), v.trim(), v.maxLength(1000, 'canvas_description_too_long')),
	''
);

export const createSubcanvas = command(
	v.object({ canvasId: idSchema, name: nameSchema, x: coordSchema, y: coordSchema }),
	async ({ canvasId, name, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		const created = await callGrpc(() =>
			orchestrationClient().createCanvas(
				{
					name,
					description: '',
					parentId: canvasId,
					position: { x: BigInt(x), y: BigInt(y) }
				},
				{ metadata }
			)
		);
		await refreshTreeViews(canvasId);
		return { ok: true as const, subcanvasId: created.canvas?.id ?? '' };
	}
);

/** `UpdateCanvas` replaces both texts; an unset position leaves it where it is. */
export const updateSubcanvas = command(
	v.object({
		canvasId: idSchema,
		subcanvasId: idSchema,
		name: nameSchema,
		description: descriptionSchema
	}),
	async ({ canvasId, subcanvasId, name, description }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().updateCanvas(
				{ canvasId: subcanvasId, name, description, position: undefined },
				{ metadata }
			)
		);
		await refreshTreeViews(canvasId);
		return { ok: true as const };
	}
);

/**
 * Deletes the subcanvas with everything inside it. The control plane refuses
 * while a pod outside still leads into it, or runs on a server inside it.
 */
export const deleteSubcanvas = command(
	v.object({ canvasId: idSchema, subcanvasId: idSchema }),
	async ({ canvasId, subcanvasId }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().deleteCanvas({ canvasId: subcanvasId }, { metadata })
		);
		await refreshTreeViews(canvasId);
		return { ok: true as const };
	}
);
