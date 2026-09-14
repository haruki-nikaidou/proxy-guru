import type { Connection, Edge, Node } from '@xyflow/svelte';
import type {
	CanvasExportNodeDto,
	CanvasGraph,
	CanvasImportNodeDto,
	CanvasPort,
	EntryNodeDto,
	ExitNodeDto,
	LoadBalanceNodeDto,
	PortDirectionName,
	PortKindName,
	RelayNodeDto,
	ServerDto,
	ServerHealthStatusName
} from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';

/** Pure graph → Svelte Flow translation; the node components stay dumb. */

export type ProblemLevel = 'none' | 'warning' | 'error';

export type FlowNodeData =
	| { kind: 'server'; server: ServerDto; problem: ProblemLevel }
	| { kind: 'entry'; node: EntryNodeDto; problem: ProblemLevel }
	| { kind: 'relay'; node: RelayNodeDto; problem: ProblemLevel }
	| { kind: 'exit'; node: ExitNodeDto; problem: ProblemLevel }
	| { kind: 'load_balance'; node: LoadBalanceNodeDto; problem: ProblemLevel }
	| { kind: 'canvas_import'; node: CanvasImportNodeDto; problem: ProblemLevel }
	| { kind: 'canvas_export'; node: CanvasExportNodeDto; problem: ProblemLevel };

export type FlowNode = Node<FlowNodeData>;
export type PortIndexEntry = {
	flowNodeId: string;
	kind: PortKindName;
	direction: PortDirectionName;
};

/** Servers and nodes share one id space in Svelte Flow but not in the backend. */
export const flowNodeId = (kind: 'server' | 'node', id: string): string => `${kind}:${id}`;

export function parseFlowNodeId(flowId: string): { kind: 'server' | 'node'; id: string } {
	const separator = flowId.indexOf(':');
	const prefix = flowId.slice(0, separator);
	return { kind: prefix === 'server' ? 'server' : 'node', id: flowId.slice(separator + 1) };
}

/** What the node panel is editing, in backend id space. */
export type PanelTarget = { kind: 'server' | 'node'; id: string };

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
		data: { kind: 'server', server, problem: serverProblem(server, levels) }
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

export function buildPortIndex(graph: CanvasGraph): Map<string, PortIndexEntry> {
	const index = new Map<string, PortIndexEntry>();
	for (const server of graph.servers) {
		const owner = flowNodeId('server', server.id);
		for (const pod of server.pods) {
			for (const port of pod.ports) {
				index.set(port.id, { flowNodeId: owner, kind: port.kind, direction: port.direction });
			}
		}
	}
	for (const node of graph.nodes) {
		const owner = flowNodeId('node', node.id);
		for (const port of node.ports) {
			index.set(port.id, { flowNodeId: owner, kind: port.kind, direction: port.direction });
		}
	}
	return index;
}

export function buildFlowEdges(graph: CanvasGraph): Edge[] {
	const index = buildPortIndex(graph);
	const edges: Edge[] = [];
	for (const edge of graph.edges) {
		const source = index.get(edge.sourcePortId);
		const target = index.get(edge.targetPortId);
		// A port outside this canvas cannot be drawn; the backend reports it too.
		if (!source || !target) continue;
		edges.push({
			id: edge.id,
			source: source.flowNodeId,
			sourceHandle: edge.sourcePortId,
			target: target.flowNodeId,
			targetHandle: edge.targetPortId,
			style:
				source.kind === 'derive_listen'
					? 'stroke: var(--canvas-port-listen); stroke-width: 2'
					: 'stroke: var(--canvas-port-destination); stroke-width: 2'
		});
	}
	return edges;
}

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
			old.style === edge.style
		) {
			return old;
		}
		changed = true;
		return { ...old, ...edge };
	});
	return changed ? edges : previous;
}

/**
 * Port ids already carrying an edge; a port is allowed exactly one.
 *
 * Both sources are needed. `buildFlowEdges` drops any edge whose opposite port
 * is off-canvas, so its surviving endpoint would otherwise look free; the graph
 * edges keep it occupied. The flow edges in turn carry the optimistic edge Svelte
 * Flow inserts before `onconnect`, which the graph has not been refreshed with
 * yet, so a second drag right after the first is still refused.
 */
export function connectedPortIds(graph: CanvasGraph, flowEdges: Edge[]): Set<string> {
	const used = new Set<string>();
	for (const edge of graph.edges) {
		used.add(edge.sourcePortId);
		used.add(edge.targetPortId);
	}
	for (const edge of flowEdges) {
		if (edge.sourceHandle) used.add(edge.sourceHandle);
		if (edge.targetHandle) used.add(edge.targetHandle);
	}
	return used;
}

/**
 * Mirrors `check_edges` in the control plane so a doomed drag never round-trips:
 * both ports must be known, sit on different nodes, share a kind, run
 * output → input, and still be free — a second edge on either endpoint is what
 * the backend reports as `PortOversubscribed`.
 */
export function canConnect(
	connection: Edge | Connection,
	portIndex: Map<string, PortIndexEntry>,
	graph: CanvasGraph,
	flowEdges: Edge[]
): boolean {
	const sourceHandle = connection.sourceHandle ?? '';
	const targetHandle = connection.targetHandle ?? '';
	const source = portIndex.get(sourceHandle);
	const target = portIndex.get(targetHandle);
	if (!source || !target) return false;
	if (connection.source === connection.target) return false;
	if (source.kind !== target.kind) return false;
	if (source.direction !== 'output' || target.direction !== 'input') return false;
	const used = connectedPortIds(graph, flowEdges);
	return !used.has(sourceHandle) && !used.has(targetHandle);
}

/**
 * The label of a port row. An import port carries the name of the export node
 * it mirrors; only the two shared keys are translated, and the load-balance
 * keys (`member_0`, `copy_0`, `source`) are shown verbatim because they are the
 * identifiers the control plane derives them as.
 */
export function portLabel(port: CanvasPort): string {
	if (port.label !== null) return port.label;
	if (port.key === 'listen') return m.editor_port_listen();
	if (port.key === 'destination') return m.editor_port_destination();
	return port.key;
}

/**
 * The shared health vocabulary of the canvas: the node badge and the server
 * panel must read the same way. `unknown` is never dressed as healthy.
 */
export function serverHealthLabel(status: ServerHealthStatusName): string {
	switch (status) {
		case 'online':
			return m.editor_health_online();
		case 'degraded':
			return m.editor_health_degraded();
		case 'offline':
			return m.editor_health_offline();
		default:
			return m.editor_health_unknown();
	}
}

/** Badge variant per status; `unknown` is an outline badge with muted text. */
export const serverHealthBadge = (
	status: ServerHealthStatusName
): { variant: 'secondary' | 'outline' | 'destructive'; class: string } =>
	status === 'online'
		? { variant: 'secondary', class: '' }
		: status === 'degraded'
			? { variant: 'outline', class: '' }
			: status === 'offline'
				? { variant: 'destructive', class: '' }
				: { variant: 'outline', class: 'text-muted-foreground' };

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
	}
	for (const node of graph.nodes) {
		index.set(node.id, {
			flowId: flowNodeId('node', node.id),
			target: { kind: 'node', id: node.id }
		});
	}
	return index;
}
