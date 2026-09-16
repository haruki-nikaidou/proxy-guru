import type { Edge } from '@xyflow/svelte';
import {
	deleteNode,
	deleteServerNode,
	disconnectEdge,
	moveNode,
	moveServerNode
} from '#lib/components/canvas/commands.js';
import { type FlowNode, type ForceTarget, parseFlowNodeId } from '#lib/components/canvas/graph.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * What a gesture on a selection sends over the wire. Both loops walk the
 * selection one call at a time and touch nothing on screen: the flow component
 * owns the mirror, and decides what a refusal looks like.
 */

/** What a delete gesture came to: whether anything went, and what refused. */
export type DeleteOutcome = { deleted: boolean; failures: ForceTarget[] };

/**
 * Asks the control plane to drop a selection, one call at a time, collecting
 * every refusal instead of stopping at the first: the operator gets one verdict
 * for the gesture, and an Admin gets the list to force.
 *
 * Edges go before nodes — a node takes its edges with it, and cutting first is
 * what lets a node whose only objection was a connection go through.
 *
 * Nothing here touches the on-screen mirror: the caller hides the selection
 * before this runs and re-reconciles after, which is what puts a refused item
 * back.
 */
export async function runDeletes(
	canvasId: string,
	doomed: { nodes: FlowNode[]; edges: Edge[] },
	failureMessage: (err: unknown) => string
): Promise<DeleteOutcome> {
	const failures: ForceTarget[] = [];
	let deleted = false;

	for (const edge of doomed.edges) {
		try {
			await disconnectEdge({ canvasId, edgeId: edge.id, force: false });
			deleted = true;
		} catch (err) {
			failures.push({
				kind: 'edge',
				id: edge.id,
				label: m.editor_disconnected(),
				message: failureMessage(err)
			});
		}
	}

	for (const node of doomed.nodes) {
		const { kind, id } = parseFlowNodeId(node.id);
		const label = node.data.kind === 'server' ? node.data.server.name : node.data.node.name;
		// Retiring an import hands its target back to the root listing; retiring
		// an export takes its mirrored port off the parent's import node.
		const subcanvasTarget = node.data.kind === 'canvas_import' ? node.data.node.targetCanvasId : '';
		const boundary = node.data.kind === 'canvas_export';
		try {
			if (kind === 'server') await deleteServerNode({ canvasId, serverId: id, force: false });
			else await deleteNode({ canvasId, nodeId: id, force: false, subcanvasTarget, boundary });
			deleted = true;
		} catch (err) {
			failures.push({ kind, id, label, message: failureMessage(err), subcanvasTarget, boundary });
		}
	}

	return { deleted, failures };
}

/**
 * Persists the positions a drag or a nudge just produced. Deliberately skips
 * the query refresh: the local position already matches, so re-rendering the
 * whole graph after every drag would only cost a flicker. A failure is the
 * caller's to report — and to re-read, since the mirror is then ahead of the
 * control plane.
 */
export async function runMoves(canvasId: string, dragged: FlowNode[]): Promise<void> {
	for (const node of dragged) {
		const { kind, id } = parseFlowNodeId(node.id);
		const x = Math.round(node.position.x);
		const y = Math.round(node.position.y);
		if (kind === 'server') {
			await moveServerNode({ canvasId, serverId: id, x, y });
		} else if (node.data.kind !== 'server') {
			// `UpdateNodeMeta` replaces name and comment, so they are resent as-is.
			// An export's y orders the mirrored ports on the parent's import node.
			await moveNode({
				canvasId,
				nodeId: id,
				name: node.data.node.name,
				comment: node.data.node.comment,
				x,
				y,
				boundary: node.data.kind === 'canvas_export'
			});
		}
	}
}
