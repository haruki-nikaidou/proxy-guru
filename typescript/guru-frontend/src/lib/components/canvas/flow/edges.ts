import type { Edge } from '@xyflow/svelte';
import type { Drawing, Id } from 'guru-graph';
import type { ServerDto } from '#lib/dto/topology.js';
import type { ProblemIndex, ProblemLevel } from './nodes.js';

/**
 * The buses Svelte Flow draws. A bus is every edge that takes the same way
 * between two handles — a pod's own dot, or a card's where no row stands for
 * the end; it is drawn as one cable with a thin line per rule riding it, so a
 * rule reads as one colour from where it enters to where it leaves.
 */
export type BusEdgeData = {
	/** The rules riding the bus, in legend order. */
	rules: Id[];
	/** How many edges (and pods whose routes they belong to) it stands for. */
	edges: number;
	pods: number;
	problem: ProblemLevel;
	/**
	 * Where along the line its count sits, from 0 at the source to 1 at the
	 * target: the middle, unless other counted lines share an end with it.
	 */
	labelAt: number;
};

/**
 * Lines that share a handle run close together near it, so their counts would
 * sit on top of each other at the middle: each gets its own place along its
 * line instead, spread over the middle of the curve in a stable order.
 */
function labelPositions(drawing: Drawing<ServerDto>): Map<string, number> {
	const counted = drawing.buses.filter(bus => bus.edges.length > 1);
	const groups = new Map<string, string[]>();
	const join = (key: string, id: string) => {
		const group = groups.get(key) ?? [];
		group.push(id);
		groups.set(key, group);
	};
	for (const bus of counted) {
		join(`source ${bus.source.node}#${bus.source.handle}`, bus.id);
		join(`target ${bus.target.node}#${bus.target.handle}`, bus.id);
	}
	const at = new Map<string, number>();
	for (const bus of counted) {
		const siblings = [
			groups.get(`source ${bus.source.node}#${bus.source.handle}`) ?? [],
			groups.get(`target ${bus.target.node}#${bus.target.handle}`) ?? []
		].reduce((widest, group) => (group.length > widest.length ? group : widest));
		if (siblings.length < 2) continue;
		const index = [...siblings].sort().indexOf(bus.id);
		at.set(bus.id, 0.3 + (0.4 * index) / (siblings.length - 1));
	}
	return at;
}

export function buildFlowEdges(drawing: Drawing<ServerDto>, problems: ProblemIndex): Edge[] {
	const labels = labelPositions(drawing);
	return drawing.buses.map(bus => ({
		id: bus.id,
		type: 'bus',
		source: bus.source.node,
		sourceHandle: bus.source.handle,
		target: bus.target.node,
		targetHandle: bus.target.handle,
		deletable: true,
		data: {
			rules: bus.rules,
			edges: bus.edges.length,
			pods: bus.pods.length,
			problem: problems.buses.get(bus.id) ?? 'none',
			labelAt: labels.get(bus.id) ?? 0.5
		} satisfies BusEdgeData
	}));
}
