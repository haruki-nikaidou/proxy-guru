import type { Edge } from '@xyflow/svelte';
import type { FlowNode } from './nodes.js';

/**
 * Merging a rebuilt mirror into the one already on screen. Svelte Flow owns
 * transient state on every node and edge — selection, measured geometry — so a
 * refresh must keep the objects it has not actually changed, and must not let a
 * delete that is still in flight come back on the next refresh.
 */

/**
 * Flow ids a delete has already removed locally but the control plane has not
 * confirmed yet. Every delete command refreshes the shared graph query, so a
 * multi-selection would otherwise see its still-pending items come back on the
 * first refresh.
 */
export type Tombstones = { nodes: Set<string>; edges: Set<string> };

export const keepNodes = (list: FlowNode[], gone: Tombstones): FlowNode[] =>
	gone.nodes.size === 0 ? list : list.filter(node => !gone.nodes.has(node.id));

/** A node takes its edges with it; the control plane disconnects them too. */
export const keepEdges = (list: Edge[], gone: Tombstones): Edge[] =>
	list.filter(
		edge => !gone.edges.has(edge.id) && !gone.nodes.has(edge.source) && !gone.nodes.has(edge.target)
	);

/**
 * The ids hidden by every delete batch still in flight. Overlapping gestures own
 * their own batch, so one settling never lifts another's protection.
 */
export function mergeTombstones(batches: readonly Tombstones[]): Tombstones | null {
	if (batches.length === 0) return null;
	if (batches.length === 1) return batches[0] ?? null;
	const nodes = new Set<string>();
	const edges = new Set<string>();
	for (const batch of batches) {
		for (const id of batch.nodes) nodes.add(id);
		for (const id of batch.edges) edges.add(id);
	}
	return { nodes, edges };
}

/** Structural equality over the plain JSON the DTOs are made of. */
function sameJson(a: unknown, b: unknown): boolean {
	if (a === b) return true;
	if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return false;
	if (Array.isArray(a) || Array.isArray(b)) {
		if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
		for (let i = 0; i < a.length; i += 1) if (!sameJson(a[i], b[i])) return false;
		return true;
	}
	const left = a as Record<string, unknown>;
	const right = b as Record<string, unknown>;
	const keys = Object.keys(left);
	if (keys.length !== Object.keys(right).length) return false;
	for (const key of keys) {
		if (!Object.hasOwn(right, key) || !sameJson(left[key], right[key])) return false;
	}
	return true;
}

/**
 * The mirror rebuilt from a refresh, merged into the one already on screen.
 * `remeasure` lists the nodes whose handles Svelte Flow has to measure again;
 * everything else keeps its previous object, so untouched node components never
 * re-render and their selection and measured geometry survive the refresh.
 */
export type NodeReconciliation = { nodes: FlowNode[]; remeasure: string[] };

export function reconcileFlowNodes(previous: FlowNode[], next: FlowNode[]): NodeReconciliation {
	const byId = new Map(previous.map(node => [node.id, node]));
	const remeasure: string[] = [];
	let changed = previous.length !== next.length;
	const nodes = next.map((node, index) => {
		const old = byId.get(node.id);
		if (!old) {
			changed = true;
			remeasure.push(node.id);
			return node;
		}
		if (previous[index]?.id !== node.id) changed = true;
		if (sameJson(old.data, node.data) && sameJson(old.position, node.position)) return old;
		changed = true;
		remeasure.push(node.id);
		// The built node carries no transient flow fields, so spreading it over the
		// old one keeps `selected`, `measured` and friends while the server wins on
		// position and data.
		return { ...old, ...node };
	});
	return { nodes: changed ? nodes : previous, remeasure };
}

export function reconcileFlowEdges(previous: Edge[], next: Edge[]): Edge[] {
	const byId = new Map(previous.map(edge => [edge.id, edge]));
	let changed = previous.length !== next.length;
	const edges = next.map((edge, index) => {
		const old = byId.get(edge.id);
		if (!old) {
			changed = true;
			return edge;
		}
		if (previous[index]?.id !== edge.id) changed = true;
		if (
			old.source === edge.source &&
			old.target === edge.target &&
			old.sourceHandle === edge.sourceHandle &&
			old.targetHandle === edge.targetHandle &&
			old.style === edge.style &&
			old.type === edge.type &&
			sameJson(old.data ?? null, edge.data ?? null)
		) {
			return old;
		}
		changed = true;
		return { ...old, ...edge };
	});
	return changed ? edges : previous;
}
