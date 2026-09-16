import type { Connection, Edge } from '@xyflow/svelte';
import type {
	CanvasGraph,
	CanvasPort,
	PortDirectionName,
	PortKindName,
	UniversalGroupName
} from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import { flowNodeId, groupHandleId } from './ids.js';

/**
 * The endpoints an edge may attach to, and the rule for whether a drag between
 * two of them is allowed — which mirrors `check_edges` in the control plane so
 * a doomed drag never round-trips.
 */

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

/**
 * Port id → endpoint, plus one "add" entry per handle group of a
 * bundle-capable node under its synthetic id. Every existing port — a member,
 * a collected bundle, a `chan:` port, a universal pod's `bundle out` — is a
 * one-edge endpoint of its own; only the hidden `lane:` ports are left out.
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
			index.set(
				groupHandleId(owner, 'channel_out'),
				add(owner, 'channel_out', 'derive_destination')
			);
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
		if (node.kind === 'load_balance' && node.mode === 'distribute') {
			// A distribute node collects upstream bundles and starts channels;
			// its members and an aggregate node's members are ordinary ports.
			index.set(groupHandleId(owner, 'bundle_in'), add(owner, 'bundle_in', 'bundle'));
			index.set(
				groupHandleId(owner, 'channel_out'),
				add(owner, 'channel_out', 'derive_destination')
			);
		}
		for (const p of node.ports) port(owner, p);
	}
	return index;
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
 * `destination`, and a bundle leaves through a port (a member, a universal
 * pod's `bundle out`) and lands on a `+ bundle` handle or an aggregate node's
 * free member.
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
	// A bundle never starts at an "add" handle: it leaves through a member or
	// a universal pod's `bundle out`.
	if (source.kind === 'bundle' && source.group) return false;
	const used = connectedPortIds(graph, flowEdges);
	const sourceFree = source.group ? true : !used.has(sourceHandle);
	const targetFree = target.group ? true : !used.has(targetHandle);
	return sourceFree && targetFree;
}

/**
 * The label of a port row. An import port carries the name of the export node
 * it mirrors; only the two shared keys are translated, anything else is shown
 * verbatim.
 */
export function portLabel(port: CanvasPort): string {
	if (port.label !== null) return port.label;
	if (port.key === 'listen') return m.editor_port_listen();
	if (port.key === 'destination') return m.editor_port_destination();
	return port.key;
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
