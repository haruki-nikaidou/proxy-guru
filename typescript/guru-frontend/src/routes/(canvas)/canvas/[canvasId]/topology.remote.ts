/**
 * Reading a canvas. The writes live beside this file — `./servers.remote.js`,
 * `./nodes.remote.js`, `./subcanvas.remote.js`, `./edges.remote.js` — and every
 * one of them refreshes `getCanvasGraph`, which is why it lives here alone with
 * the tree queries the dialogs ask.
 *
 * The control plane is authoritative on permissions: no role check happens here.
 */
import type { CanvasTreeNode as ProtoCanvasTreeNode } from 'app-protobuf/orchestration/orchestration';
import * as v from 'valibot';
import type { CanvasOption } from '#lib/dto/canvas.js';
import type { CanvasGraph } from '#lib/dto/topology.js';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { toCanvasGraph } from '#lib/server/topology/canvas.js';
import { query } from '$app/server';

export const getCanvasGraph = query(
	v.object({ canvasId: idSchema }),
	async ({ canvasId }): Promise<CanvasGraph> => {
		const metadata = sessionMetadata(requireSessionId());

		const [detail, validation] = await callGrpc(() =>
			Promise.all([
				orchestrationClient().getCanvas({ canvasId }, { metadata }),
				orchestrationClient().validateCanvas({ canvasId }, { metadata })
			])
		);

		// An import node's ports are keyed by the record id of the export node
		// they mirror, which is unreadable on screen: the names live on the target
		// canvas, so each distinct target is read once for them.
		const exportNames = new Map<string, Map<string, string>>();
		await Promise.all(
			[
				...new Set(
					detail.nodes.flatMap(node =>
						node.spec?.canvasImport ? [node.spec.canvasImport.canvasId] : []
					)
				)
			].map(async targetId => {
				try {
					const target = await orchestrationClient().getCanvas(
						{ canvasId: targetId },
						{ metadata }
					);
					const names = new Map<string, string>();
					for (const node of target.nodes) {
						if (node.spec?.canvasExport) names.set(node.id, node.name);
					}
					exportNames.set(targetId, names);
				} catch {
					// A target that vanished is `CANVAS_IMPORT_UNRESOLVED` in the
					// problems panel; its ports simply keep their raw keys.
				}
			})
		);

		return toCanvasGraph(canvasId, detail, validation, exportNames);
	}
);

/** Every canvas id of the tree `node` roots. */
function treeCanvasIds(node: ProtoCanvasTreeNode | undefined, into: Set<string>) {
	if (!node) return;
	if (node.canvas) into.add(node.canvas.id);
	for (const child of node.children) treeCanvasIds(child, into);
}

/**
 * What this canvas may embed: root canvases outside its own tree. A canvas of
 * the same tree would be a self, ancestor or duplicate import, all of which the
 * control plane refuses; a canvas that is already imported elsewhere is not a
 * root, so it never appears here.
 */
export const listImportableCanvases = query(
	v.object({ canvasId: idSchema }),
	async ({ canvasId }): Promise<CanvasOption[]> => {
		const metadata = sessionMetadata(requireSessionId());
		const [roots, tree] = await callGrpc(() =>
			Promise.all([
				orchestrationClient().listCanvases({ includeSubcanvases: false }, { metadata }),
				orchestrationClient().getCanvasTree({ canvasId }, { metadata })
			])
		);
		const own = new Set<string>();
		treeCanvasIds(tree.root, own);
		return roots.canvases
			.filter(canvas => !own.has(canvas.id))
			.map(canvas => ({ id: canvas.id, name: canvas.name, description: canvas.description }))
			.sort((a, b) => a.name.localeCompare(b.name));
	}
);

/**
 * Which canvas of this tree owns `nodeId`. Topology problems are reported for
 * the flattened tree, so a problem opened on one canvas can name a node that
 * lives in another; this is how the editor finds where to go. Empty when the
 * node is nowhere in the tree.
 */
export const locateNodeCanvas = query(
	v.object({ canvasId: idSchema, nodeId: idSchema }),
	async ({ canvasId, nodeId }): Promise<string> => {
		const metadata = sessionMetadata(requireSessionId());
		const tree = await callGrpc(() =>
			orchestrationClient().getCanvasTree({ canvasId }, { metadata })
		);
		const ids = new Set<string>();
		treeCanvasIds(tree.root, ids);
		ids.delete(canvasId); // already on screen: the caller looked there first
		const found = await Promise.all(
			[...ids].map(async id => {
				const detail = await orchestrationClient().getCanvas({ canvasId: id }, { metadata });
				return detail.nodes.some(node => node.id === nodeId) ? id : '';
			})
		);
		return found.find(id => id !== '') ?? '';
	}
);
