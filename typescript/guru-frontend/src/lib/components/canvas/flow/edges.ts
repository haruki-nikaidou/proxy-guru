import type { Edge } from '@xyflow/svelte';
import type { Drawing, Id } from 'guru-graph';
import type { ServerDto } from '#lib/dto/topology.js';
import type { ProblemIndex, ProblemLevel } from './nodes.js';

/**
 * The buses Svelte Flow draws. A bus is every edge that takes the same way
 * between two cards; it is drawn as one cable with a thin line per rule riding
 * it, so a rule reads as one colour from where it enters to where it leaves.
 */
export type BusEdgeData = {
	/** The rules riding the bus, in legend order. */
	rules: Id[];
	/** How many edges (and pods whose routes they belong to) it stands for. */
	edges: number;
	pods: number;
	problem: ProblemLevel;
};

export function buildFlowEdges(drawing: Drawing<ServerDto>, problems: ProblemIndex): Edge[] {
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
			problem: problems.buses.get(bus.id) ?? 'none'
		} satisfies BusEdgeData
	}));
}
