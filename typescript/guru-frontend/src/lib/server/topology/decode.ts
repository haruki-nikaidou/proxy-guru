/**
 * Protobuf replies as the DTOs in `#lib/dto/topology.js`. Pure mapping: no
 * transport, no validation — the remote functions in
 * `routes/(canvas)/canvas/[canvasId]/` do the calling, this does the shaping.
 * Scalar enums are mapped in `./enums.js`, the whole canvas in `./canvas.js`.
 */
import type {
	ConfigSnapshot as ProtoConfigSnapshot,
	Node as ProtoNode,
	PodConfig as ProtoPodConfig,
	Server as ProtoServer,
	ServerAddresses as ProtoServerAddresses
} from 'app-protobuf/orchestration/orchestration';
import {
	AddressSource,
	CanvasExportAs,
	PortKind,
	ProblemKind,
	ProblemSeverity
} from 'app-protobuf/orchestration/orchestration';
import type {
	BundlePortDto,
	CanvasPort,
	ChannelDto,
	ConfigSnapshotDto,
	MemberDto,
	PodDto,
	ServerAddressesDto,
	ServerDto,
	StandaloneNode,
	TopologyProblem,
	UniversalPodDto
} from '#lib/dto/topology.js';
import {
	toAddressSource,
	toBalanceMode,
	toIpv6,
	toPortDirection,
	toPortKind,
	toProxy,
	toRelayProtocol,
	toServerHealth
} from './enums.js';

/** Shared by every node whose ports carry no derived label. */
export const NO_LABELS: ReadonlyMap<string, string> = new Map();

export const toPorts = (node: ProtoNode, labels: ReadonlyMap<string, string>): CanvasPort[] =>
	node.ports
		.map(port => ({
			id: port.id,
			kind: toPortKind(port.kind),
			direction: toPortDirection(port.direction),
			key: port.key,
			position: Number(port.position),
			label: labels.get(port.key) ?? null
		}))
		.sort((a, b) => a.position - b.position);

export function toProblem(problem: {
	severity: ProblemSeverity;
	kind: ProblemKind;
	message: string;
	nodeIds: string[];
	edgeIds: string[];
	portIds: string[];
}): TopologyProblem {
	return {
		severity:
			problem.severity === ProblemSeverity.PROBLEM_ERROR
				? 'error'
				: problem.severity === ProblemSeverity.PROBLEM_WARNING
					? 'warning'
					: 'unknown',
		kind: ProblemKind[problem.kind] ?? 'UNSPECIFIED',
		message: problem.message,
		nodeIds: problem.nodeIds,
		edgeIds: problem.edgeIds,
		portIds: problem.portIds
	};
}

export const toPod = (node: ProtoNode, pod: ProtoPodConfig): PodDto => ({
	id: node.id,
	name: node.name,
	comment: node.comment,
	serverId: pod.serverId,
	port: pod.port,
	bindIp: pod.bindIp === '' ? null : pod.bindIp,
	advertiseIp: pod.advertiseIp === '' ? null : pod.advertiseIp,
	ports: toPorts(node, NO_LABELS)
});

const toAddresses = (addresses: ProtoServerAddresses | undefined): ServerAddressesDto => ({
	v4: { reported: addresses?.v4?.reported ?? '', pinned: addresses?.v4?.pinned ?? '' },
	v6: { reported: addresses?.v6?.reported ?? '', pinned: addresses?.v6?.pinned ?? '' },
	extra: addresses?.extra ?? [],
	reportedInterfaces: addresses?.reportedInterfaces ?? [],
	reportedAt: addresses?.reportedAt ?? '',
	observedAddress: addresses?.observedAddress ?? '',
	observedAt: addresses?.observedAt ?? '',
	effectiveAddress: addresses?.effectiveAddress ?? '',
	effectiveSource: toAddressSource(addresses?.effectiveSource ?? AddressSource.UNSPECIFIED),
	reportedCountry: addresses?.reportedCountry ?? ''
});

/** The 12-colour channel palette: `--channel-0` … `--channel-11`. */
const CHANNEL_COLORS = 12;

/** The pod id a `chan:<pod>` / `lane:<pod>` port names, or `null`. */
export const channelOf = (key: string): string | null =>
	key.startsWith('chan:') ? key.slice('chan:'.length) : null;

/** The far node id a `bundle_in:<id>` port names, or `null`. */
export const bundlePeerOf = (key: string): string | null =>
	key.startsWith('bundle_in:') ? key.slice('bundle_in:'.length) : null;

/** The key of a member's bundle port. */
const memberKey = (slot: number): string => `member_${slot}`;

/**
 * A standalone node, or `null` for a pod / an unsupported spec. `exportNames`
 * maps the export node ids of an import target to their names, which is what
 * the mirrored ports are keyed by; `channels` resolves the `chan:` ports of a
 * universal node; `peerName` names a node (a universal pod by its server) and
 * `peerOfPort` finds the node on the far end of the edge on a port.
 */
export function toStandalone(
	node: ProtoNode,
	exportNames: ReadonlyMap<string, string>,
	channels: ReadonlyMap<string, ChannelDto>,
	peerName: (nodeId: string) => string,
	peerOfPort: (portId: string) => string | null
): StandaloneNode | null {
	const spec = node.spec;
	const base = {
		id: node.id,
		name: node.name,
		comment: node.comment,
		x: Number(node.position?.x ?? 0n),
		y: Number(node.position?.y ?? 0n),
		ports: toPorts(node, exportNames)
	};
	if (spec?.entry) {
		const tls = spec.entry.tls;
		return {
			...base,
			kind: 'entry',
			receiveProxyProtocol: toProxy(spec.entry.receiveProxyProtocol),
			tls: tls
				? {
						sni: tls.sni,
						dnsProviderId: tls.dnsProviderId,
						domainId: tls.domainId,
						acmeDirectory: tls.acmeDirectory
					}
				: null
		};
	}
	if (spec?.relay) {
		return {
			...base,
			kind: 'relay',
			protocol: toRelayProtocol(spec.relay.protocol),
			overrideIpAddress: spec.relay.overrideIpAddress,
			overridePort: spec.relay.overridePort
		};
	}
	if (spec?.exit) {
		return {
			...base,
			kind: 'exit',
			destination: spec.exit.destination,
			passProxyProtocol: toProxy(spec.exit.passProxyProtocol)
		};
	}
	const channelsOf = () =>
		base.ports
			.flatMap(port => {
				const pod = channelOf(port.key);
				const channel = pod === null ? undefined : channels.get(pod);
				return channel ? [{ ...channel, portId: port.id }] : [];
			})
			.sort((a, b) => a.ordinal - b.ordinal);
	// The members in the order declared, each with its port (a lane laid out
	// thin has none declared and is never drawn) and the far end of its bundle.
	const membersOf = (declared: { slot: number; name: string }[]): MemberDto[] =>
		declared.flatMap(member => {
			const port = base.ports.find(p => p.key === memberKey(member.slot));
			if (!port) return [];
			const peer = peerOfPort(port.id);
			return [
				{
					slot: member.slot,
					name: member.name,
					port,
					peerName: peer === null ? null : peerName(peer)
				}
			];
		});
	// Collected bundles all sit at position 0: order them by the far node's name.
	const bundlesIn = (): BundlePortDto[] =>
		base.ports
			.filter(port => port.key.startsWith('bundle_in:'))
			.map(port => ({ ...port, peerName: peerName(bundlePeerOf(port.key) ?? '') }))
			.sort((a, b) => a.peerName.localeCompare(b.peerName));
	if (spec?.loadBalanceDistribute) {
		return {
			...base,
			kind: 'load_balance',
			mode: 'distribute',
			balanceMode: toBalanceMode(spec.loadBalanceDistribute.mode),
			protocol: toRelayProtocol(spec.loadBalanceDistribute.protocol),
			members: membersOf(spec.loadBalanceDistribute.members),
			channels: channelsOf(),
			bundlesIn: bundlesIn()
		};
	}
	if (spec?.loadBalanceAggregate) {
		return {
			...base,
			kind: 'load_balance',
			mode: 'aggregate',
			balanceMode: 'round_robin',
			protocol: 'tcp_raw',
			members: membersOf(spec.loadBalanceAggregate.members),
			channels: channelsOf(),
			bundlesIn: []
		};
	}
	if (spec?.canvasImport) {
		return {
			...base,
			kind: 'canvas_import',
			targetCanvasId: spec.canvasImport.canvasId,
			// Unset when the target is gone: `CANVAS_IMPORT_UNRESOLVED` says so.
			targetName: node.importTarget?.name ?? ''
		};
	}
	if (spec?.canvasExport) {
		return {
			...base,
			kind: 'canvas_export',
			portKind:
				spec.canvasExport.kind === PortKind.DERIVE_LISTEN ? 'derive_listen' : 'derive_destination',
			exportAs:
				spec.canvasExport.direction === CanvasExportAs.INPUT_INTO_CANVAS
					? 'input_into_canvas'
					: 'output_out_of_canvas'
		};
	}
	return null;
}

/**
 * The channels of a canvas: every `chan:` port of a distribute node or a
 * universal pod names the entry pod that is the channel; the port's position is
 * the channel's ordinal.
 */
export function collectChannels(nodes: ProtoNode[]): Map<string, ChannelDto> {
	const names = new Map(nodes.map(node => [node.id, node.name]));
	const channels = new Map<string, ChannelDto>();
	for (const node of nodes) {
		if (!node.spec?.loadBalanceDistribute && !node.spec?.universalPod) continue;
		for (const port of node.ports) {
			const podId = channelOf(port.key);
			if (podId === null) continue;
			const ordinal = Number(port.position);
			channels.set(podId, {
				podId,
				podName: names.get(podId) ?? podId,
				ordinal,
				colorIndex: ((ordinal % CHANNEL_COLORS) + CHANNEL_COLORS) % CHANNEL_COLORS,
				distributorId: node.id
			});
		}
	}
	return channels;
}

export const toServer = (
	server: ProtoServer,
	pods: PodDto[],
	universal: UniversalPodDto | null
): ServerDto => ({
	id: server.id,
	name: server.name,
	icon: server.icon,
	comment: server.comment,
	x: Number(server.position?.x ?? 0n),
	y: Number(server.position?.y ?? 0n),
	ipv6Resolve: toIpv6(server.ipv6Resolve),
	logLevel: server.logLevel,
	lastSeenAt: server.lastSeenAt,
	lastHealthReportAt: server.lastHealthReportAt,
	healthStatus: toServerHealth(server.healthStatus),
	addresses: toAddresses(server.addresses),
	agentVersion: server.agentVersion,
	agentArch: server.agentArch,
	agentUnit: server.agentUnit,
	agentUpdateRequested: server.agentUpdateRequested,
	agentUpdateError: server.agentUpdateError,
	agentKeyIssuedAt: server.agentKeyIssuedAt,
	pods,
	universal
});

/** `revision` is an `int64`: it is narrowed here so no bigint reaches a client. */
export const toSnapshot = (snapshot: ProtoConfigSnapshot | undefined): ConfigSnapshotDto | null =>
	snapshot === undefined
		? null
		: {
				revision: Number(snapshot.revision),
				createdAt: snapshot.createdAt,
				forwardings: snapshot.forwardings.map(forwarding => ({
					serves: forwarding.serves
						? {
								serverId: forwarding.serves.serverId,
								port: forwarding.serves.port,
								protocol: forwarding.serves.protocol
							}
						: null,
					pointsAt: forwarding.pointsAt.map(cap => ({
						serverId: cap.serverId,
						port: cap.port,
						protocol: cap.protocol
					}))
				}))
			};
