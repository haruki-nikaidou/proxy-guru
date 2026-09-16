import type { Node } from '@xyflow/svelte';
import type {
	CanvasExportNodeDto,
	CanvasGraph,
	CanvasImportNodeDto,
	ChannelDto,
	EntryNodeDto,
	ExitNodeDto,
	LoadBalanceNodeDto,
	RelayNodeDto,
	ServerDto
} from '#lib/dto/topology.js';
import { flowNodeId } from './ids.js';

/** The canvas graph as the cards Svelte Flow draws; the components stay dumb. */

export type ProblemLevel = 'none' | 'warning' | 'error';

export type FlowNodeData =
	| {
			kind: 'server';
			server: ServerDto;
			problem: ProblemLevel;
			/** Entry pod id → its channel, for the pods on this server that start one. */
			channels: Record<string, ChannelDto>;
	  }
	| { kind: 'entry'; node: EntryNodeDto; problem: ProblemLevel }
	| { kind: 'relay'; node: RelayNodeDto; problem: ProblemLevel }
	| { kind: 'exit'; node: ExitNodeDto; problem: ProblemLevel }
	| { kind: 'load_balance'; node: LoadBalanceNodeDto; problem: ProblemLevel }
	| { kind: 'canvas_import'; node: CanvasImportNodeDto; problem: ProblemLevel }
	| { kind: 'canvas_export'; node: CanvasExportNodeDto; problem: ProblemLevel };

export type FlowNode = Node<FlowNodeData>;
/** What the side panel is showing, in backend id space: a node, or an edge. */
export type PanelTarget = { kind: 'server' | 'node' | 'edge'; id: string };

/** A deletion the control plane refused, offered to admins as a force retry. */
export type ForceTarget = {
	kind: 'server' | 'node' | 'edge';
	id: string;
	label: string;
	message: string;
	/** An import node: forcing it also frees the canvas it embedded. */
	subcanvasTarget?: string;
	/** An export node: forcing it reshapes the parent's import node. */
	boundary?: boolean;
};

/** `error` beats `warning` beats `none`; a server inherits its pods' problems. */
function problemLevels(graph: CanvasGraph): Map<string, ProblemLevel> {
	const levels = new Map<string, ProblemLevel>();
	for (const problem of graph.problems) {
		if (problem.severity !== 'error' && problem.severity !== 'warning') continue;
		for (const nodeId of problem.nodeIds) {
			if (problem.severity === 'error' || levels.get(nodeId) === undefined) {
				levels.set(nodeId, problem.severity);
			}
		}
	}
	return levels;
}

function serverProblem(server: ServerDto, levels: Map<string, ProblemLevel>): ProblemLevel {
	let level: ProblemLevel = levels.get(server.id) ?? 'none';
	for (const pod of server.pods) {
		const podLevel = levels.get(pod.id);
		if (podLevel === 'error') return 'error';
		if (podLevel === 'warning') level = level === 'none' ? 'warning' : level;
	}
	return level;
}

export function buildFlowNodes(graph: CanvasGraph): FlowNode[] {
	const levels = problemLevels(graph);
	const nodes: FlowNode[] = graph.servers.map(server => ({
		id: flowNodeId('server', server.id),
		type: 'server',
		position: { x: server.x, y: server.y },
		data: {
			kind: 'server',
			server,
			problem: serverProblem(server, levels),
			channels: Object.fromEntries(
				server.pods.flatMap(pod => {
					const channel = graph.channels[pod.id];
					return channel ? [[pod.id, channel] as const] : [];
				})
			)
		}
	}));

	for (const node of graph.nodes) {
		const problem = levels.get(node.id) ?? 'none';
		nodes.push({
			id: flowNodeId('node', node.id),
			type:
				node.kind === 'entry'
					? 'entry'
					: node.kind === 'relay'
						? 'relay'
						: node.kind === 'exit'
							? 'exit'
							: node.kind === 'canvas_import'
								? 'canvasImport'
								: node.kind === 'canvas_export'
									? 'canvasExport'
									: 'loadBalance',
			position: { x: node.x, y: node.y },
			// The union is discriminated by the same `kind` the DTO carries.
			data: { kind: node.kind, node, problem } as FlowNodeData
		});
	}
	return nodes;
}

/**
 * Backend id (server, pod or standalone node) → the flow node that draws it and
 * the panel target that edits it. A pod resolves to its server.
 */
export function buildBackendIndex(
	graph: CanvasGraph
): Map<string, { flowId: string; target: PanelTarget }> {
	const index = new Map<string, { flowId: string; target: PanelTarget }>();
	for (const server of graph.servers) {
		const entry = {
			flowId: flowNodeId('server', server.id),
			target: { kind: 'server', id: server.id } as PanelTarget
		};
		index.set(server.id, entry);
		for (const pod of server.pods) index.set(pod.id, entry);
		if (server.universal) {
			index.set(server.universal.nodeId, entry);
			for (const lane of server.universal.lanes) index.set(lane.nodeId, entry);
		}
	}
	for (const node of graph.nodes) {
		index.set(node.id, {
			flowId: flowNodeId('node', node.id),
			target: { kind: 'node', id: node.id }
		});
	}
	return index;
}

/**
 * Every name visible on a canvas, so a fresh default never duplicates one. Pods
 * are included: they render inside their server and carry a name of their own,
 * so a node named after one would be just as confusing.
 */
export function canvasNames(graph: CanvasGraph | undefined): ReadonlySet<string> {
	const names = new Set<string>();
	if (!graph) return names;
	for (const server of graph.servers) {
		names.add(server.name);
		for (const pod of server.pods) names.add(pod.name);
	}
	for (const node of graph.nodes) names.add(node.name);
	for (const pod of graph.orphanPods) names.add(pod.name);
	return names;
}
