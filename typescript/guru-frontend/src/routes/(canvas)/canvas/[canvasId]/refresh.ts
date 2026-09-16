/**
 * What goes stale beyond the canvas being edited. Deliberately not a
 * `*.remote.ts` file: such a module may only export remote functions, and these
 * are plain helpers the command files share.
 */
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import type { sessionMetadata } from '#lib/server/session.js';
import {
	getCanvasTrail,
	listCanvases,
	listCanvasOptions
} from '../../../(home)/canvases.remote.js';
import { getCanvasGraph, listImportableCanvases } from './topology.remote.js';

/**
 * Attaching or detaching a subcanvas moves a canvas between the root listing
 * and its parent's tree, so the shell's listings go stale with the graph — for
 * the canvas that was edited and for the canvas that changed hands.
 */
export const refreshSubcanvasViews = (canvasId: string, targetCanvasId: string) =>
	Promise.all([
		getCanvasGraph({ canvasId }).refresh(),
		getCanvasTrail({ canvasId }).refresh(),
		getCanvasTrail({ canvasId: targetCanvasId }).refresh(),
		listImportableCanvases({ canvasId }).refresh(),
		listImportableCanvases({ canvasId: targetCanvasId }).refresh(),
		listCanvases({ includeSubcanvases: false }).refresh(),
		listCanvases({ includeSubcanvases: true }).refresh(),
		listCanvasOptions().refresh()
	]);

/**
 * An export edit reshapes — or relabels — the mirrored port on the importing
 * node, so the parent's graph is as stale as this one. A root canvas has no
 * parent and refreshes only itself.
 */
export async function refreshAcrossBoundary(
	canvasId: string,
	metadata: ReturnType<typeof sessionMetadata>
) {
	const detail = await callGrpc(() => orchestrationClient().getCanvas({ canvasId }, { metadata }));
	const parent = detail.ancestors.at(-1);
	await Promise.all([
		getCanvasGraph({ canvasId }).refresh(),
		...(parent ? [getCanvasGraph({ canvasId: parent.id }).refresh()] : [])
	]);
}
