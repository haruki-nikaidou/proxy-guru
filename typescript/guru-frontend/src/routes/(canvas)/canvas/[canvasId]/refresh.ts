/**
 * What goes stale beyond the graph being edited. Deliberately not a
 * `*.remote.ts` file: such a module may only export remote functions, and this
 * is a plain helper the command files share.
 */
import {
	getCanvasTrail,
	listCanvases,
	listCanvasOptions
} from '../../../(home)/canvases.remote.js';
import { getCanvasGraph } from './graph.remote.js';

/**
 * A subcanvas coming or going changes the tree, so the shell's listings and the
 * breadcrumb go stale with the graph.
 */
export const refreshTreeViews = (canvasId: string) =>
	Promise.all([
		getCanvasGraph({ canvasId }).refresh(),
		getCanvasTrail({ canvasId }).refresh(),
		listCanvases({ includeSubcanvases: false }).refresh(),
		listCanvases({ includeSubcanvases: true }).refresh(),
		listCanvasOptions().refresh()
	]);
