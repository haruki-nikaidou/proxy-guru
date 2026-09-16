import type { Edge } from '@xyflow/svelte';
import type { CanvasGraph, ChannelDto, PortKindName } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import { flowNodeId } from './ids.js';
import { buildPortIndex, type PortIndexEntry, portLabel } from './ports.js';

/**
 * The edges Svelte Flow draws, and what the panel says about one. Colour is the
 * whole point: an edge carrying a single rule takes that rule's channel colour
 * from end to end, and a bundle carrying several is drawn as a counted bus.
 */

/** The edge data a bundle edge carries: how many channels ride on it. */
export type BundleEdgeData = { count: number };

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
 * The channels each bundle edge carries: a distributor's channels, carried
 * along every bundle out of it and merged at every node bundles meet. Listed
 * in ordinal order, so a panel reads the same list on every refresh.
 */
function bundleChannels(
	graph: CanvasGraph,
	index: Map<string, PortIndexEntry>
): Map<string, ChannelDto[]> {
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
	const resolve = (pods: Iterable<string>): ChannelDto[] =>
		[...pods]
			.flatMap(podId => {
				const channel = graph.channels[podId];
				return channel ? [channel] : [];
			})
			.sort((a, b) => a.ordinal - b.ordinal);
	return new Map(bundles.map(bundle => [bundle.id, resolve(carried.get(bundle.from) ?? [])]));
}

/** The CSS colour of a channel; the palette wraps every 12 channels. */
export const channelColor = (channel: Pick<ChannelDto, 'colorIndex'>): string =>
	`var(--channel-${channel.colorIndex})`;

/** A thin edge in a channel's colour: the one rule it carries, end to end. */
const channelStyle = (channel: ChannelDto): string =>
	`stroke: ${channelColor(channel)}; stroke-width: 2.5`;

/**
 * Every edge is emitted with the same keys (`type`, `style`, `data`): the
 * reconciler spreads the rebuilt edge over the old one, so a key left out
 * would keep its previous value — a bundle dropping to one channel would stay
 * drawn as a bus.
 */
export function buildFlowEdges(graph: CanvasGraph): Edge[] {
	const index = buildPortIndex(graph);
	const colours = channelPorts(graph);
	const riding = bundleChannels(graph, index);
	const edges: Edge[] = [];
	for (const edge of graph.edges) {
		const source = index.get(edge.sourcePortId);
		const target = index.get(edge.targetPortId);
		// A port outside this canvas cannot be drawn; the backend reports it too.
		if (!source || !target) continue;
		const ends = {
			id: edge.id,
			source: source.flowNodeId,
			sourceHandle: edge.sourcePortId,
			target: target.flowNodeId,
			targetHandle: edge.targetPortId
		};
		if (source.kind === 'bundle') {
			const channels = riding.get(edge.id) ?? [];
			const only = channels.length === 1 ? channels[0] : undefined;
			// A bundle carrying one rule reads as that rule: an ordinary thin line
			// in its colour. Anything else is a bus, counted.
			edges.push(
				only
					? { ...ends, type: 'default', style: channelStyle(only), data: undefined }
					: {
							...ends,
							type: 'bundle',
							style: undefined,
							data: { count: channels.length } satisfies BundleEdgeData
						}
			);
			continue;
		}
		const channel = colours.get(edge.sourcePortId) ?? colours.get(edge.targetPortId);
		edges.push({
			...ends,
			type: 'default',
			style: channel
				? channelStyle(channel)
				: source.kind === 'derive_listen'
					? 'stroke: var(--canvas-port-listen); stroke-width: 2'
					: 'stroke: var(--canvas-port-destination); stroke-width: 2',
			data: undefined
		});
	}
	return edges;
}

/** One end of an edge as the panel names it: the node's name and its port's label. */
export type EdgeEndpoint = { node: string; port: string };

/** What the edge panel shows: the two ends and the rules (channels) riding the edge. */
export type EdgeDetail = {
	id: string;
	kind: PortKindName;
	source: EdgeEndpoint;
	target: EdgeEndpoint;
	/** Every channel a bus carries, in ordinal order; at most one on a thin edge. */
	channels: ChannelDto[];
};

/**
 * Port id → how the node cards label it, so the panel reads like the canvas: a
 * pod's port under the pod's name, a universal pod's bundles by their far end,
 * a load-balance member by its name, a channel port by its entry pod.
 */
function portLabels(graph: CanvasGraph): Map<string, EdgeEndpoint> {
	const labels = new Map<string, EdgeEndpoint>();
	for (const server of graph.servers) {
		const node = server.name;
		for (const pod of server.pods) {
			for (const port of pod.ports) {
				labels.set(port.id, { node, port: `${pod.name} · ${portLabel(port)}` });
			}
		}
		const universal = server.universal;
		if (!universal) continue;
		for (const port of universal.bundleIn) labels.set(port.id, { node, port: port.peerName });
		for (const channel of universal.channels) {
			labels.set(channel.portId, { node, port: channel.podName });
		}
		if (universal.bundleOut) {
			labels.set(universal.bundleOut.id, { node, port: m.editor_universal_bundle_out() });
		}
	}
	for (const node of graph.nodes) {
		for (const port of node.ports) labels.set(port.id, { node: node.name, port: portLabel(port) });
		if (node.kind !== 'load_balance') continue;
		for (const member of node.members) {
			labels.set(member.port.id, { node: node.name, port: member.name });
		}
		for (const port of node.bundlesIn)
			labels.set(port.id, { node: node.name, port: port.peerName });
		for (const channel of node.channels) {
			labels.set(channel.portId, { node: node.name, port: channel.podName });
		}
	}
	return labels;
}

/**
 * The panel's view of one edge, or `null` once the graph no longer holds it
 * (which is how the panel closes after a delete) or an end is off-canvas.
 */
export function describeEdge(graph: CanvasGraph, edgeId: string): EdgeDetail | null {
	const edge = graph.edges.find(entry => entry.id === edgeId);
	if (!edge) return null;
	const index = buildPortIndex(graph);
	const source = index.get(edge.sourcePortId);
	const target = index.get(edge.targetPortId);
	if (!source || !target) return null;
	const labels = portLabels(graph);
	const unknown = { node: '', port: '' };
	let channels: ChannelDto[];
	if (source.kind === 'bundle') {
		channels = bundleChannels(graph, index).get(edge.id) ?? [];
	} else {
		const colours = channelPorts(graph);
		const channel = colours.get(edge.sourcePortId) ?? colours.get(edge.targetPortId);
		channels = channel ? [channel] : [];
	}
	return {
		id: edge.id,
		kind: source.kind,
		source: labels.get(edge.sourcePortId) ?? unknown,
		target: labels.get(edge.targetPortId) ?? unknown,
		channels
	};
}
