/**
 * The two nodes that cross a canvas boundary: an import, which embeds another
 * canvas, and an export, which is one port of this canvas as seen from the
 * parent. Both reshape the canvas on the other side of the boundary, so every
 * write here refreshes more than the canvas being edited.
 */
import * as v from 'valibot';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { fromExportAs, fromPortKind } from '#lib/server/topology/enums.js';
import {
	coordSchema,
	exportAsSchema,
	idSchema,
	nameSchema,
	portKindSchema
} from '#lib/server/topology/schemas.js';
import { command } from '$app/server';
import { refreshAcrossBoundary, refreshSubcanvasViews } from './refresh.js';

/**
 * A brand-new canvas plus the import node that embeds it. The two steps are not
 * atomic in the control plane, so a failed import takes the canvas it just
 * created back out rather than leaving a stray root behind.
 */
export const createSubcanvas = command(
	v.object({ canvasId: idSchema, name: nameSchema, x: coordSchema, y: coordSchema }),
	async ({ canvasId, name, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		const created = await callGrpc(() =>
			orchestrationClient().createCanvas({ name, description: '' }, { metadata })
		);
		const targetCanvasId = created.canvas?.id ?? '';
		try {
			await callGrpc(() =>
				orchestrationClient().createNode(
					{
						canvasId,
						name,
						comment: '',
						spec: { canvasImport: { canvasId: targetCanvasId } },
						position: { x, y },
						itemCount: 0
					},
					{ metadata }
				)
			);
		} catch (err) {
			await orchestrationClient()
				.deleteCanvas({ canvasId: targetCanvasId }, { metadata })
				.catch(() => undefined);
			throw err;
		}
		await refreshSubcanvasViews(canvasId, targetCanvasId);
		return { ok: true as const, subcanvasId: targetCanvasId };
	}
);

/** Embeds an existing canvas. Its target is immutable once the node exists. */
export const importCanvas = command(
	v.object({
		canvasId: idSchema,
		targetCanvasId: idSchema,
		name: nameSchema,
		x: coordSchema,
		y: coordSchema
	}),
	async ({ canvasId, targetCanvasId, name, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().createNode(
				{
					canvasId,
					name,
					comment: '',
					spec: { canvasImport: { canvasId: targetCanvasId } },
					position: { x, y },
					itemCount: 0
				},
				{ metadata }
			)
		);
		await refreshSubcanvasViews(canvasId, targetCanvasId);
		return { ok: true as const };
	}
);

/**
 * One boundary port of this canvas. Creating it reshapes the importer's ports
 * in the same transaction, so the parent gains a matching port at once.
 */
export const createExportNode = command(
	v.object({
		canvasId: idSchema,
		name: nameSchema,
		portKind: portKindSchema,
		exportAs: exportAsSchema,
		x: coordSchema,
		y: coordSchema
	}),
	async ({ canvasId, name, portKind, exportAs, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().createNode(
				{
					canvasId,
					name,
					comment: '',
					spec: {
						canvasExport: { kind: fromPortKind(portKind), direction: fromExportAs(exportAs) }
					},
					position: { x, y },
					itemCount: 0
				},
				{ metadata }
			)
		);
		await refreshAcrossBoundary(canvasId, metadata);
		return { ok: true as const };
	}
);

/**
 * Re-kinding an export reshapes the mirrored port on the importer, which drops
 * whatever edge the parent had attached to it.
 */
export const replaceExportSpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		portKind: portKindSchema,
		exportAs: exportAsSchema
	}),
	async ({ canvasId, nodeId, portKind, exportAs }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().replaceNodeSpec(
				{
					nodeId,
					spec: {
						canvasExport: { kind: fromPortKind(portKind), direction: fromExportAs(exportAs) }
					},
					itemCount: 0
				},
				{ metadata }
			)
		);
		await refreshAcrossBoundary(canvasId, metadata);
		return { ok: true as const };
	}
);
