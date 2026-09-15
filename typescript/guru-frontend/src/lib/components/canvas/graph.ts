import type { Connection, Edge, Node } from '@xyflow/svelte';
import type {
	CanvasExportNodeDto,
	CanvasGraph,
	CanvasImportNodeDto,
	CanvasPort,
	ChannelDto,
	EntryNodeDto,
	ExitNodeDto,
	LoadBalanceNodeDto,
	PortDirectionName,
	PortKindName,
	RelayNodeDto,
	ServerDto,
	ServerHealthStatusName,
	UniversalGroupName
} from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';

/** Pure graph → Svelte Flow translation; the node components stay dumb. */

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
/**
 * One connection endpoint: a port (one edge), or one of a bundle-capable
 * node's "add" handles (`group`), which takes a new connection every time —
 * the control plane creates the port behind it.
 */
export type PortIndexEntry = {
	flowNodeId: string;
	kind: PortKindName;
	direction: PortDirectionName;
	group?: UniversalGroupName;
};

/** The edge data a bundle edge carries: how many channels ride on it. */
export type BundleEdgeData = { count: number };

/**
 * A bundle-capable node's "add" handles are Svelte Flow handles of their own,
 * with ids no port can collide with: one per group, taking a new connection
 * each time. The ports the control plane creates behind them are ordinary
 * one-edge handles of their own.
 */
export const groupHandleId = (flowId: string, group: UniversalGroupName): string =>
	`u:${flowId}:${group}`;

export function parseGroupHandle(
	handle: string
): { flowId: string; group: UniversalGroupName } | null {
	if (!handle.startsWith('u:')) return null;
	const separator = handle.lastIndexOf(':');
	const group = handle.slice(separator + 1);
	if (group !== 'channel_out' && group !== 'bundle_in' && group !== 'bundle_out') return null;
	return { flowId: handle.slice(2, separator), group };
}

/** The CSS colour of a channel; the palette wraps every 12 channels. */
export const channelColor = (channel: Pick<ChannelDto, 'colorIndex'>): string =>
	`var(--channel-${channel.colorIndex})`;

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
 * Port id → endpoint, plus one "add" entry per handle group of a
 * bundle-capable node under its synthetic id. Every existing port — a bundle
 * port, a `chan:` port, a hand-drawn port — is a one-edge endpoint of its own;
 * only the hidden `lane:` ports are left out.
 */
export function buildPortIndex(graph: CanvasGraph): Map<string, PortIndexEntry> {
	const index = new Map<string, PortIndexEntry>();
	const add = (owner: string, name: UniversalGroupName, kind: PortKindName): PortIndexEntry => ({
		flowNodeId: owner,
		kind,
		direction: name === 'bundle_in' ? 'input' : 'output',
		group: name
	});
	const port = (owner: string, port: CanvasPort) => {
		if (port.key.startsWith('lane:')) return;
		index.set(port.id, { flowNodeId: owner, kind: port.kind, direction: port.direction });
	};
	for (const server of graph.servers) {
		const owner = flowNodeId('server', server.id);
		for (const pod of server.pods) for (const p of pod.ports) port(owner, p);
		if (server.universal) {
			index.set(groupHandleId(owner, 'bundle_in'), add(owner, 'bundle_in', 'bundle'));
			index.set(groupHandleId(owner, 'channel_out'), add(owner, 'channel_out', 'derive_destination'));
			for (const p of server.universal.bundleIn) port(owner, p);
			for (const c of server.universal.channels) {
				index.set(c.portId, { flowNodeId: owner, kind: 'derive_destination', direction: 'output' });
			}
			// The fixed outgoing bundle port takes one edge, like any port.
			if (server.universal.bundleOut) port(owner, server.universal.bundleOut);
		}
	}
	for (const node of graph.nodes) {
		const owner = flowNodeId('node', node.id);
		if (node.kind === 'load_balance') {
			// Both take bundles in; a distribute node also starts channels and
			// bundles out.
			index.set(groupHandleId(owner, 'bundle_in'), add(owner, 'bundle_in', 'bundle'));
			if (node.mode === 'distribute') {
				index.set(groupHandleId(owner, 'channel_out'), add(owner, 'channel_out', 'derive_destination'));
				index.set(groupHandleId(owner, 'bundle_out'), add(owner, 'bundle_out', 'bundle'));
			}
		}
		for (const p of node.ports) port(owner, p);
	}
	return index;
}

/**
 * The channel each pod-side port belongs to: an entry pod's own two ports and
 * the aggregator's `chan:` port for it. Edges on these take the channel's
 * colour, so one rule reads as one colour from entry to exit.
 */
function channelPorts(graph: CanvasGraph): Map<string, ChannelDto> {
	const byPort = new Map<string, ChannelDto>();
	for (const server of graph.servers) {
		for (const pod of server.pods) {
			const channel = graph.channels[pod.id];
			if (!channel) continue;
			for (const port of pod.ports) byPort.set(port.id, channel);
		}
		for (const channel of server.universal?.channels ?? []) byPort.set(channel.portId, channel);
	}
	for (const node of graph.nodes) {
		if (node.kind !== 'load_balance') continue;
		for (const port of node.ports) {
			const channel = port.key.startsWith('chan:')
				? graph.channels[port.key.slice('chan:'.length)]
				: undefined;
			if (channel) byPort.set(port.id, channel);
		}
	}
	return byPort;
}

/**
 * How many channels each bundle edge carries: a distributor's channels, carried
 * along every bundle out of it and merged at every node bundles meet.
 */
function bundleCounts(graph: CanvasGraph, index: Map<string, PortIndexEntry>): Map<string, number> {
	const carried = new Map<string, Set<string>>();
	for (const node of graph.nodes) {
		if (node.kind !== 'load_balance' || node.mode !== 'distribute') continue;
		carried.set(flowNodeId('node', node.id), new Set(node.channels.map(c => c.podId)));
	}
	for (const server of graph.servers) {
		const own = server.universal?.channels ?? [];
		if (own.length > 0) {
			carried.set(flowNodeId('server', server.id), new Set(own.map(c => c.podId)));
		}
	}
	const bundles = graph.edges.flatMap(edge => {
		const source = index.get(edge.sourcePortId);
		const target = index.get(edge.targetPortId);
		return source?.kind === 'bundle' && target?.kind === 'bundle'
			? [{ id: edge.id, from: source.flowNodeId, to: target.flowNodeId }]
			: [];
	});
	// Bundles form a DAG (a cycle is a validation error); a few passes settle it.
	for (let pass = 0; pass < 16; pass += 1) {
		let changed = false;
		for (const bundle of bundles) {
			const upstream = carried.get(bundle.from);
			if (!upstream) continue;
			const downstream = carried.get(bundle.to) ?? new Set<string>();
			const before = downstream.size;
			for (const pod of upstream) downstream.add(pod);
			carried.set(bundle.to, downstream);
			changed ||= downstream.size !== before;
		}
		if (!changed) break;
	}
	return new Map(bundles.map(bundle => [bundle.id, carried.get(bundle.from)?.size ?? 0]));
}

export function buildFlowEdges(graph: CanvasGraph): Edge[] {
	const index = buildPortIndex(graph);
	const colours = channelPorts(graph);
	const counts = bundleCounts(graph, index);
	const edges: Edge[] = [];
	for (const edge of graph.edges) {
		const source = index.get(edge.sourcePortId);
		const target = index.get(edge.targetPortId);
		// A port outside this canvas cannot be drawn; the backend reports it too.
		if (!source || !target) continue;
		if (source.kind === 'bundle') {
			edges.push({
				id: edge.id,
				type: 'bundle',
				source: source.flowNodeId,
				sourceHandle: edge.sourcePortId,
				target: target.flowNodeId,
				targetHandle: edge.targetPortId,
				data: { count: counts.get(edge.id) ?? 0 } satisfies BundleEdgeData
			});
			continue;
		}
		const channel = colours.get(edge.sourcePortId) ?? colours.get(edge.targetPortId);
		edges.push({
			id: edge.id,
			source: source.flowNodeId,
			sourceHandle: edge.sourcePortId,
			target: target.flowNodeId,
			targetHandle: edge.targetPortId,
			style: channel
				? `stroke: ${channelColor(channel)}; stroke-width: 2.5`
				: source.kind === 'derive_listen'
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
 * the backend reports as `PortOversubscribed`. An "add" handle takes any number
 * of edges, a distribute node's channel handle only lands on a pod's free
 * `destination`, and bundles only land on an "add" handle.
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
	if (source.group === 'channel_out') {
		// Into a pod's destination, never into another bundle-capable node. A
		// universal pod's channel handle may take a pod of its own server: a
		// hop within one server is a warning, not an error.
		if (target.group || !target.flowNodeId.startsWith('server:')) return false;
	}
	// A bundle is drawn from an "add" handle to an "add" handle, or out of a
	// universal pod's fixed port into an "add" handle; existing bundle ports
	// are wired already.
	if (source.kind === 'bundle' && !target.group) return false;
	const used = connectedPortIds(graph, flowEdges);
	const sourceFree = source.group ? true : !used.has(sourceHandle);
	const targetFree = target.group ? true : !used.has(targetHandle);
	return sourceFree && targetFree;
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

/** Where suggested pod ports come from: the same high range the control plane
 * draws generated landing pods from. */
export const POD_PORT_RANGE: readonly [number, number] = [40000, 59999];

/**
 * A random port in [`POD_PORT_RANGE`] that no pod in `used` holds. Collisions
 * with anything else on the host surface as an apply error on the pod, which is
 * what the re-roll button is for.
 */
export function randomFreePort(used: Iterable<number>): number {
	const taken = new Set(used);
	const [low, high] = POD_PORT_RANGE;
	const span = high - low + 1;
	const buffer = new Uint32Array(1);
	for (let attempt = 0; attempt < 64; attempt += 1) {
		crypto.getRandomValues(buffer);
		const port = low + ((buffer[0] ?? 0) % span);
		if (!taken.has(port)) return port;
	}
	return low;
}
