/**
 * The canvas graph the editor draws, assembled from the protobuf replies. Split
 * from `./decode.js` because this is the one mapping that needs the whole
 * picture — servers, their pods, the lanes a universal node generated and the
 * problems the control plane reported — rather than one message at a time.
 */
import type { GetCanvasReply, ValidateCanvasReply } from 'app-protobuf/orchestration/orchestration';
import type {
	CanvasGraph,
	LaneDto,
	PodDto,
	StandaloneNode,
	UniversalPodDto
} from '#lib/dto/topology.js';
import {
	bundlePeerOf,
	channelOf,
	collectChannels,
	NO_LABELS,
	toPod,
	toPorts,
	toProblem,
	toServer,
	toStandalone
} from './decode.js';

/**
 * The canvas as the editor draws it, from what `GetCanvas` and `ValidateCanvas`
 * answered. `exportNames` maps the canvas id of an import target to the names
 * of its export nodes — what the mirrored ports are keyed by; the caller reads
 * those canvases, because everything here is a pure mapping.
 */
export function toCanvasGraph(
	canvasId: string,
	detail: GetCanvasReply,
	validation: ValidateCanvasReply,
	exportNames: ReadonlyMap<string, ReadonlyMap<string, string>>
): CanvasGraph {
	// A pod is placed on a server directly; one on a server of another canvas
	// in the tree is an orphan here.
	const serverIds = new Set(detail.servers.map(server => server.id));
	const channels = collectChannels(detail.nodes);
	const problems = validation.problems.map(toProblem);
	// `CHANNEL_NO_EXIT` names the universal pod and the channel pod it warns
	// about; a landing pod of that pair has no exit yet.
	const noExit = new Set<string>();
	for (const problem of problems) {
		if (problem.kind !== 'CHANNEL_NO_EXIT') continue;
		const [group, pod] = problem.nodeIds;
		if (group && pod) noExit.add(`${group}:${pod}`);
	}
	const serverNames = new Map(detail.servers.map(server => [server.id, server.name]));
	// A lane's source is a distributor (named by its node) or a universal pod
	// (named by its server).
	const sourceName = (nodeId: string): string => {
		const source = detail.nodes.find(node => node.id === nodeId);
		const server = source?.spec?.universalPod?.serverId;
		return (server ? serverNames.get(server) : undefined) ?? source?.name ?? nodeId;
	};
	// The node on the far end of the edge on a port, for a member's bundle.
	const ownerOfPort = new Map<string, string>();
	for (const node of detail.nodes) for (const port of node.ports) ownerOfPort.set(port.id, node.id);
	const peerOfPort = (portId: string): string | null => {
		const edge = detail.edges.find(e => e.sourcePortId === portId || e.targetPortId === portId);
		if (!edge) return null;
		const far = edge.sourcePortId === portId ? edge.targetPortId : edge.sourcePortId;
		return ownerOfPort.get(far) ?? null;
	};

	const podsByServer = new Map<string, PodDto[]>();
	const universalByServer = new Map<string, UniversalPodDto>();
	const orphanPods: PodDto[] = [];
	const nodes: StandaloneNode[] = [];
	for (const node of detail.nodes) {
		const universal = node.spec?.universalPod;
		if (universal) {
			const ports = toPorts(node, NO_LABELS);
			universalByServer.set(universal.serverId, {
				nodeId: node.id,
				bundleIn: ports
					.filter(port => port.key.startsWith('bundle_in:'))
					.map(port => ({ ...port, peerName: sourceName(bundlePeerOf(port.key) ?? '') }))
					.sort((a, b) => a.peerName.localeCompare(b.peerName)),
				channels: ports
					.flatMap(port => {
						const pod = channelOf(port.key);
						const channel = pod === null ? undefined : channels.get(pod);
						return channel ? [{ ...channel, portId: port.id }] : [];
					})
					.sort((a, b) => a.ordinal - b.ordinal),
				bundleOut: ports.find(port => port.key === 'bundle_out') ?? null,
				lanes: []
			});
			continue;
		}
		// Generated lanes are not drawn: a landing pod is listed on its server,
		// everything else lives behind the universal node that made it.
		if (node.lane) continue;
		const pod = node.spec?.pod;
		if (pod) {
			const dto = toPod(node, pod);
			if (!serverIds.has(pod.serverId)) {
				orphanPods.push(dto);
				continue;
			}
			const bucket = podsByServer.get(pod.serverId);
			if (bucket) bucket.push(dto);
			else podsByServer.set(pod.serverId, [dto]);
			continue;
		}
		const target = node.spec?.canvasImport?.canvasId;
		const standalone = toStandalone(
			node,
			(target === undefined ? undefined : exportNames.get(target)) ?? NO_LABELS,
			channels,
			sourceName,
			peerOfPort
		);
		if (standalone) nodes.push(standalone);
	}
	for (const node of detail.nodes) {
		const lane = node.lane;
		const pod = node.spec?.pod;
		if (!lane || !pod) continue;
		const universal = universalByServer.get(pod.serverId);
		const channel = channels.get(lane.channelPodId);
		if (!universal || !channel) continue;
		const entry: LaneDto = {
			nodeId: node.id,
			channel,
			sourceName: sourceName(lane.sourceNodeId),
			serverId: pod.serverId,
			port: pod.port,
			bindIp: pod.bindIp === '' ? null : pod.bindIp,
			advertiseIp: pod.advertiseIp === '' ? null : pod.advertiseIp,
			hasExit: !noExit.has(`${lane.groupNodeId}:${lane.channelPodId}`)
		};
		universal.lanes.push(entry);
	}
	for (const universal of universalByServer.values()) {
		universal.lanes.sort(
			(a, b) => a.channel.ordinal - b.channel.ordinal || a.sourceName.localeCompare(b.sourceName)
		);
	}
	return {
		canvas: {
			id: detail.canvas?.id ?? canvasId,
			name: detail.canvas?.name ?? '',
			description: detail.canvas?.description ?? ''
		},
		servers: detail.servers.map(server =>
			toServer(server, podsByServer.get(server.id) ?? [], universalByServer.get(server.id) ?? null)
		),
		nodes,
		edges: detail.edges.map(edge => ({
			id: edge.id,
			sourcePortId: edge.sourcePortId,
			targetPortId: edge.targetPortId
		})),
		problems,
		orphanPods,
		ancestors: detail.ancestors.map(canvas => ({
			id: canvas.id,
			name: canvas.name,
			description: canvas.description
		})),
		channels: Object.fromEntries(channels)
	};
}
