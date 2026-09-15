import type {
	CanvasTreeNode as ProtoCanvasTreeNode,
	ConfigSnapshot as ProtoConfigSnapshot,
	Node as ProtoNode,
	PodConfig as ProtoPodConfig,
	Server as ProtoServer,
	ServerAddresses as ProtoServerAddresses
} from 'app-protobuf/orchestration/orchestration';
import {
	AddressSource,
	CanvasExportAs,
	Ipv6Resolve,
	LoadBalanceMode,
	PortDirection,
	PortKind,
	ProblemKind,
	ProblemSeverity,
	ProxyProtocolVersion,
	RelayProtocol,
	ServerHealthStatus,
	UniversalGroup
} from 'app-protobuf/orchestration/orchestration';
import * as v from 'valibot';
import type { CanvasOption } from '#lib/dto/canvas.js';
import type {
	AddressSourceName,
	CanvasExportAsName,
	CanvasGraph,
	CanvasPort,
	ChannelDto,
	ConfigSnapshotDto,
	ExportPortKindName,
	Ipv6ResolveName,
	LaneDto,
	LoadBalanceModeName,
	PodDto,
	PortDirectionName,
	PortKindName,
	ProxyProtocolName,
	RelayProtocolName,
	ServerConfigTomlDto,
	ServerDto,
	ServerAddressesDto,
	ServerHealthStatusName,
	ServerRolloutDto,
	StandaloneNode,
	TopologyProblem,
	UniversalGroupName,
	UniversalPodDto
} from '#lib/dto/topology.js';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { command, query } from '$app/server';
import {
	getCanvasTrail,
	listCanvases,
	listCanvasOptions
} from '../../../(home)/canvases.remote.js';

// The control plane is authoritative on permissions: no role check happens here.
const idSchema = v.pipe(v.string(), v.minLength(1, 'id_required'));
const nameSchema = v.pipe(
	v.string(),
	v.trim(),
	v.minLength(1, 'node_name_required'),
	v.maxLength(128, 'node_name_too_long')
);
const commentSchema = v.optional(
	v.pipe(v.string(), v.trim(), v.maxLength(1000, 'node_comment_too_long')),
	''
);
const coordSchema = v.pipe(v.number(), v.integer(), v.safeInteger());
const portSchema = v.pipe(
	v.number(),
	v.integer(),
	v.minValue(1, 'port_out_of_range'),
	v.maxValue(65535, 'port_out_of_range')
);
/** 0 means no hand-drawn ports (a node used through bundles only); else 2-256. */
const memberCountSchema = v.pipe(
	v.number(),
	v.integer(),
	v.minValue(0, 'member_count_out_of_range'),
	v.maxValue(256, 'member_count_out_of_range'),
	v.check(value => value !== 1, 'member_count_out_of_range')
);
/** An optional IP literal: empty means unset. */
const optionalIpSchema = v.optional(
	v.pipe(
		v.string(),
		v.trim(),
		v.check(value => value === '' || v.safeParse(v.pipe(v.string(), v.ip()), value).success, 'ip_invalid')
	),
	''
);
const extraAddressesSchema = v.optional(
	v.array(v.pipe(v.string(), v.trim(), v.ip('ip_invalid'))),
	[]
);
const logLevelSchema = v.pipe(v.string(), v.trim(), v.minLength(1, 'log_level_required'));
const proxySchema = v.picklist(['none', 'v1', 'v2'] as const);
const relayProtocolSchema = v.picklist(['tcp_raw', 'tcp_tls', 'quic'] as const);
const balanceModeSchema = v.picklist(['round_robin', 'random', 'ip_hash', 'fallback'] as const);
const ipv6Schema = v.picklist(['required', 'preferred', 'tolerated', 'forbidden'] as const);
/**
 * One hostname, optionally with a leading `*.` wildcard label. Deliberately
 * narrower than the RFC: the ACME order is built from this verbatim.
 */
const SNI_PATTERN = /^(\*\.)?[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$/i;
/**
 * `null` clears TLS termination on the entry. Every field is required except
 * `acme_directory`, where empty means "use the installation default".
 */
const tlsSchema = v.nullable(
	v.object({
		sni: v.pipe(
			v.string(),
			v.trim(),
			v.minLength(1, 'sni_required'),
			v.maxLength(253, 'sni_too_long'),
			v.regex(SNI_PATTERN, 'sni_invalid')
		),
		dnsProviderId: v.pipe(v.string(), v.trim(), v.minLength(1, 'dns_provider_required')),
		domainId: v.pipe(v.string(), v.trim(), v.minLength(1, 'domain_id_required')),
		acmeDirectory: v.pipe(
			v.string(),
			v.trim(),
			v.check(value => value === '' || /^https:\/\/[^\s]+$/.test(value), 'acme_directory_invalid')
		)
	})
);

// Every unknown / UNSPECIFIED value decodes to the backend's own default.
function toProxy(value: ProxyProtocolVersion): ProxyProtocolName {
	switch (value) {
		case ProxyProtocolVersion.PROXY_V1:
			return 'v1';
		case ProxyProtocolVersion.PROXY_V2:
			return 'v2';
		default:
			return 'none';
	}
}
function fromProxy(value: ProxyProtocolName): ProxyProtocolVersion {
	switch (value) {
		case 'v1':
			return ProxyProtocolVersion.PROXY_V1;
		case 'v2':
			return ProxyProtocolVersion.PROXY_V2;
		default:
			return ProxyProtocolVersion.UNSPECIFIED;
	}
}
function toRelayProtocol(value: RelayProtocol): RelayProtocolName {
	switch (value) {
		case RelayProtocol.RELAY_TCP_TLS:
			return 'tcp_tls';
		case RelayProtocol.RELAY_QUIC:
			return 'quic';
		default:
			return 'tcp_raw';
	}
}
function fromRelayProtocol(value: RelayProtocolName): RelayProtocol {
	switch (value) {
		case 'tcp_tls':
			return RelayProtocol.RELAY_TCP_TLS;
		case 'quic':
			return RelayProtocol.RELAY_QUIC;
		default:
			return RelayProtocol.RELAY_TCP_RAW;
	}
}
function toBalanceMode(value: LoadBalanceMode): LoadBalanceModeName {
	switch (value) {
		case LoadBalanceMode.RANDOM:
			return 'random';
		case LoadBalanceMode.IP_HASH:
			return 'ip_hash';
		case LoadBalanceMode.FALLBACK:
			return 'fallback';
		default:
			return 'round_robin';
	}
}
function fromBalanceMode(value: LoadBalanceModeName): LoadBalanceMode {
	switch (value) {
		case 'random':
			return LoadBalanceMode.RANDOM;
		case 'ip_hash':
			return LoadBalanceMode.IP_HASH;
		case 'fallback':
			return LoadBalanceMode.FALLBACK;
		default:
			return LoadBalanceMode.ROUND_ROBIN;
	}
}
function toIpv6(value: Ipv6Resolve): Ipv6ResolveName {
	switch (value) {
		case Ipv6Resolve.IPV6_REQUIRED:
			return 'required';
		case Ipv6Resolve.IPV6_PREFERRED:
			return 'preferred';
		case Ipv6Resolve.IPV6_FORBIDDEN:
			return 'forbidden';
		default:
			// `IPV6_RESOLVE_UNSPECIFIED` decodes to `Tolerated` in the control plane.
			return 'tolerated';
	}
}
function fromIpv6(value: Ipv6ResolveName): Ipv6Resolve {
	switch (value) {
		case 'required':
			return Ipv6Resolve.IPV6_REQUIRED;
		case 'preferred':
			return Ipv6Resolve.IPV6_PREFERRED;
		case 'forbidden':
			return Ipv6Resolve.IPV6_FORBIDDEN;
		default:
			return Ipv6Resolve.IPV6_TOLERATED;
	}
}
/**
 * A server that never reported, and any status this build does not know, both
 * read as `unknown`: the dashboard must not claim a worker is online.
 */
function toServerHealth(value: ServerHealthStatus): ServerHealthStatusName {
	switch (value) {
		case ServerHealthStatus.SERVER_ONLINE:
			return 'online';
		case ServerHealthStatus.SERVER_DEGRADED:
			return 'degraded';
		case ServerHealthStatus.SERVER_OFFLINE:
			return 'offline';
		default:
			return 'unknown';
	}
}
const toPortKind = (value: PortKind): PortKindName =>
	value === PortKind.DERIVE_LISTEN
		? 'derive_listen'
		: value === PortKind.BUNDLE
			? 'bundle'
			: 'derive_destination';
const toPortDirection = (value: PortDirection): PortDirectionName =>
	value === PortDirection.PORT_OUTPUT ? 'output' : 'input';

/** Shared by every node whose ports carry no derived label. */
const NO_LABELS: ReadonlyMap<string, string> = new Map();

const toPorts = (node: ProtoNode, labels: ReadonlyMap<string, string>): CanvasPort[] =>
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

function toProblem(problem: {
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

const toPod = (node: ProtoNode, pod: ProtoPodConfig): PodDto => ({
	id: node.id,
	name: node.name,
	comment: node.comment,
	serverId: pod.serverId,
	port: pod.port,
	bindIp: pod.bindIp === '' ? null : pod.bindIp,
	advertiseIp: pod.advertiseIp === '' ? null : pod.advertiseIp,
	ports: toPorts(node, NO_LABELS)
});

const toAddressSource = (value: AddressSource): AddressSourceName => {
	switch (value) {
		case AddressSource.ADDRESS_OVERRIDE:
			return 'override';
		case AddressSource.ADDRESS_REPORTED:
			return 'reported';
		case AddressSource.ADDRESS_OBSERVED:
			return 'observed';
		default:
			return 'none';
	}
};

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
const channelOf = (key: string): string | null =>
	key.startsWith('chan:') ? key.slice('chan:'.length) : null;

/**
 * A standalone node, or `null` for a pod / an unsupported spec. `exportNames`
 * maps the export node ids of an import target to their names, which is what
 * the mirrored ports are keyed by; `channels` resolves the `chan:` ports of a
 * universal node.
 */
function toStandalone(
	node: ProtoNode,
	exportNames: ReadonlyMap<string, string>,
	channels: ReadonlyMap<string, ChannelDto>
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
	// The on-demand ports of a load-balance node are the control plane's
	// (channels, bundles); only the rest are the operator's hand-drawn layout.
	const onDemand = (port: CanvasPort) =>
		port.kind === 'bundle' || port.key.startsWith('chan:') || port.key.startsWith('lane:');
	const channelsOf = (withPort: boolean) =>
		base.ports
			.flatMap(port => {
				const pod = channelOf(port.key);
				const channel = pod === null ? undefined : channels.get(pod);
				return channel ? [withPort ? { ...channel, portId: port.id } : channel] : [];
			})
			.sort((a, b) => a.ordinal - b.ordinal);
	if (spec?.loadBalanceDistribute) {
		return {
			...base,
			kind: 'load_balance',
			mode: 'distribute',
			balanceMode: toBalanceMode(spec.loadBalanceDistribute.mode),
			protocol: toRelayProtocol(spec.loadBalanceDistribute.protocol),
			memberCount: base.ports.filter(port => port.key.startsWith('member_')).length,
			manualPorts: base.ports.filter(port => !onDemand(port)),
			channels: channelsOf(false),
			bundleCount: base.ports.filter(port => port.kind === 'bundle').length
		};
	}
	if (spec?.loadBalanceAggregate) {
		return {
			...base,
			kind: 'load_balance',
			mode: 'aggregate',
			balanceMode: 'round_robin',
			protocol: 'tcp_raw',
			memberCount: base.ports.filter(port => port.key.startsWith('copy_')).length,
			manualPorts: base.ports.filter(port => !onDemand(port)),
			channels: channelsOf(true),
			bundleCount: base.ports.filter(port => port.kind === 'bundle').length
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
			portKind: spec.canvasExport.kind === PortKind.DERIVE_LISTEN ? 'derive_listen' : 'derive_destination',
			exportAs:
				spec.canvasExport.direction === CanvasExportAs.INPUT_INTO_CANVAS
					? 'input_into_canvas'
					: 'output_out_of_canvas'
		};
	}
	return null;
}

/**
 * The channels of a canvas: every `chan:` port of a distribute node names the
 * entry pod that is the channel; the port's position is the channel's ordinal.
 */
function collectChannels(nodes: ProtoNode[]): Map<string, ChannelDto> {
	const names = new Map(nodes.map(node => [node.id, node.name]));
	const channels = new Map<string, ChannelDto>();
	for (const node of nodes) {
		if (!node.spec?.loadBalanceDistribute) continue;
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

const toServer = (
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
	healthStatus: toServerHealth(server.healthStatus),
	addresses: toAddresses(server.addresses),
	pods,
	universal
});

export const getCanvasGraph = query(
	v.object({ canvasId: idSchema }),
	async ({ canvasId }): Promise<CanvasGraph> => {
		const metadata = sessionMetadata(requireSessionId());

		const [detail, validation] = await callGrpc(() =>
			Promise.all([
				orchestrationClient().getCanvas({ canvasId }, { metadata }),
				orchestrationClient().validateCanvas({ canvasId }, { metadata })
			])
		);

		// A pod is placed on a server directly; one on a server of another canvas
		// in the tree is an orphan here.
		const serverIds = new Set(detail.servers.map(server => server.id));

		// An import node's ports are keyed by the record id of the export node
		// they mirror, which is unreadable on screen: the names live on the target
		// canvas, so each distinct target is read once for them.
		const exportNames = new Map<string, Map<string, string>>();
		await Promise.all(
			[
				...new Set(
					detail.nodes.flatMap(node =>
						node.spec?.canvasImport ? [node.spec.canvasImport.canvasId] : []
					)
				)
			].map(async targetId => {
				try {
					const target = await orchestrationClient().getCanvas(
						{ canvasId: targetId },
						{ metadata }
					);
					const names = new Map<string, string>();
					for (const node of target.nodes) {
						if (node.spec?.canvasExport) names.set(node.id, node.name);
					}
					exportNames.set(targetId, names);
				} catch {
					// A target that vanished is `CANVAS_IMPORT_UNRESOLVED` in the
					// problems panel; its ports simply keep their raw keys.
				}
			})
		);

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
		const nodeNames = new Map(detail.nodes.map(node => [node.id, node.name]));
		const serverNames = new Map(detail.servers.map(server => [server.id, server.name]));
		// A lane's source is a distributor (named by its node) or a universal pod
		// (named by its server).
		const sourceName = (nodeId: string): string => {
			const source = detail.nodes.find(node => node.id === nodeId);
			const server = source?.spec?.universalPod?.serverId;
			return (server ? serverNames.get(server) : undefined) ?? source?.name ?? nodeId;
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
					bundleIn: ports.filter(port => port.key.startsWith('bundle_in:')),
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
				channels
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
		void nodeNames;

		return {
			canvas: {
				id: detail.canvas?.id ?? canvasId,
				name: detail.canvas?.name ?? '',
				description: detail.canvas?.description ?? ''
			},
			servers: detail.servers.map(server =>
				toServer(
					server,
					podsByServer.get(server.id) ?? [],
					universalByServer.get(server.id) ?? null
				)
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
);

export const createServerNode = command(
	v.object({ canvasId: idSchema, name: nameSchema, x: coordSchema, y: coordSchema }),
	async ({ canvasId, name, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().createServer(
				{
					canvasId,
					name,
					icon: '',
					comment: '',
					position: { x, y },
					ipv6Resolve: Ipv6Resolve.IPV6_TOLERATED,
					logLevel: 'info',
					overrideV4: '',
					overrideV6: '',
					extraAddresses: []
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

export const updateServerNode = command(
	v.object({
		canvasId: idSchema,
		serverId: idSchema,
		name: nameSchema,
		icon: v.optional(v.string(), ''),
		comment: commentSchema,
		ipv6Resolve: ipv6Schema,
		logLevel: logLevelSchema,
		overrideV4: optionalIpSchema,
		overrideV6: optionalIpSchema,
		extraAddresses: extraAddressesSchema
	}),
	async ({
		canvasId,
		serverId,
		name,
		icon,
		comment,
		ipv6Resolve,
		logLevel,
		overrideV4,
		overrideV6,
		extraAddresses
	}) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().updateServer(
				{
					serverId,
					name,
					icon,
					comment,
					ipv6Resolve: fromIpv6(ipv6Resolve),
					logLevel,
					overrideV4,
					overrideV6,
					extraAddresses
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

/** Deliberately does not refresh: the dragged position already matches locally. */
export const moveServerNode = command(
	v.object({ canvasId: idSchema, serverId: idSchema, x: coordSchema, y: coordSchema }),
	async ({ serverId, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().moveServer({ serverId, position: { x, y } }, { metadata })
		);
		return { ok: true as const };
	}
);

export const deleteServerNode = command(
	v.object({
		canvasId: idSchema,
		serverId: idSchema,
		force: v.optional(v.boolean(), false)
	}),
	async ({ canvasId, serverId, force }) => {
		const metadata = sessionMetadata(requireSessionId());
		// `DeleteServer` refuses while any pod is still placed on it, so the pods go
		// first. The list is re-read here rather than taken from the client: a pod
		// added since the last refresh would otherwise block the delete.
		const detail = await callGrpc(() =>
			orchestrationClient().getCanvas({ canvasId }, { metadata })
		);
		for (const node of detail.nodes) {
			const pod = node.spec?.pod;
			// A landing lane is not retirable by hand and the universal pod goes
			// with the server: both are left to `DeleteServer`, which refuses with
			// the channels still landing here.
			if (!pod || pod.serverId !== serverId || node.lane) continue;
			const nodeId = node.id;
			await callGrpc(() =>
				force
					? orchestrationClient().forceDeleteNode({ nodeId }, { metadata })
					: orchestrationClient().retireNode({ nodeId }, { metadata })
			);
		}
		await callGrpc(() => orchestrationClient().deleteServer({ serverId }, { metadata }));
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

/** `revision` is an `int64`: it is narrowed here so no bigint reaches a client. */
const toSnapshot = (snapshot: ProtoConfigSnapshot | undefined): ConfigSnapshotDto | null =>
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

/**
 * Where one server stands between the config the control plane derived and the
 * config its worker confirmed. Read by the server panel while it is open.
 */
export const getServerRollout = query(
	v.object({ serverId: idSchema }),
	async ({ serverId }): Promise<ServerRolloutDto> => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			orchestrationClient().getServerRolloutStatus({ serverId }, { metadata })
		);
		return {
			desired: toSnapshot(reply.desired),
			inFlight: toSnapshot(reply.inFlight),
			applied: toSnapshot(reply.applied),
			applyError: reply.applyError,
			deriveError: reply.deriveError,
			waitingForServerIds: [...reply.waitingForServerIds],
			derivationPending: reply.derivationPending,
			lastSeenAt: reply.lastSeenAt,
			invalidPods: reply.invalidPods.map(pod => ({
				nodeId: pod.nodeId,
				podName: pod.podName,
				listen: pod.listen,
				error: pod.error
			}))
		};
	}
);

/** The worker TOML as rendered for this server, fetched only when asked for. */
export const getServerConfigToml = query(
	v.object({ serverId: idSchema }),
	async ({ serverId }): Promise<ServerConfigTomlDto> => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			orchestrationClient().getServerConfig({ serverId }, { metadata })
		);
		return { revision: Number(reply.revision), toml: reply.toml };
	}
);

/**
 * Admin only, destructive: declares the server dead so its dependants may
 * switch away from listeners it might still be serving. The graph goes stale
 * with the rollout, because forgetting re-derives every dependant.
 */
export const forgetServerApplied = command(
	v.object({ canvasId: idSchema, serverId: idSchema }),
	async ({ canvasId, serverId }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() => orchestrationClient().forgetServerApplied({ serverId }, { metadata }));
		await Promise.all([
			getServerRollout({ serverId }).refresh(),
			getCanvasGraph({ canvasId }).refresh()
		]);
		return { ok: true as const };
	}
);

const standaloneKindSchema = v.picklist([
	'entry',
	'relay',
	'exit',
	'load_balance_distribute',
	'load_balance_aggregate'
] as const);

export const createStandaloneNode = command(
	v.object({
		canvasId: idSchema,
		kind: standaloneKindSchema,
		name: nameSchema,
		x: coordSchema,
		y: coordSchema,
		memberCount: v.optional(memberCountSchema, 2)
	}),
	async ({ canvasId, kind, name, x, y, memberCount }) => {
		const metadata = sessionMetadata(requireSessionId());
		// A half-drawn chain is deliberately storable (see `services/topology.rs`):
		// an exit with no destination yet is a warning in the problems panel, not a
		// write rejection, so the palette never invents a placeholder value.
		const spec =
			kind === 'entry'
				? { entry: { receiveProxyProtocol: ProxyProtocolVersion.UNSPECIFIED, tls: undefined } }
				: kind === 'relay'
					? {
							relay: {
								protocol: RelayProtocol.RELAY_TCP_RAW,
								overrideIpAddress: '',
								overridePort: 0
							}
						}
					: kind === 'exit'
						? { exit: { destination: '', passProxyProtocol: ProxyProtocolVersion.UNSPECIFIED } }
						: kind === 'load_balance_distribute'
							? {
									loadBalanceDistribute: {
										mode: LoadBalanceMode.ROUND_ROBIN,
										protocol: RelayProtocol.RELAY_TCP_RAW
									}
								}
							: { loadBalanceAggregate: {} };
		const itemCount =
			kind === 'load_balance_distribute' || kind === 'load_balance_aggregate' ? memberCount : 0;

		await callGrpc(() =>
			orchestrationClient().createNode(
				{ canvasId, name, comment: '', spec, position: { x, y }, itemCount },
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

const portKindSchema = v.picklist(['derive_listen', 'derive_destination'] as const);
const exportAsSchema = v.picklist(['input_into_canvas', 'output_out_of_canvas'] as const);

const fromPortKind = (value: ExportPortKindName): PortKind =>
	value === 'derive_listen' ? PortKind.DERIVE_LISTEN : PortKind.DERIVE_DESTINATION;
const fromExportAs = (value: CanvasExportAsName): CanvasExportAs =>
	value === 'input_into_canvas'
		? CanvasExportAs.INPUT_INTO_CANVAS
		: CanvasExportAs.OUTPUT_OUT_OF_CANVAS;

/** Every canvas id of the tree `node` roots. */
function treeCanvasIds(node: ProtoCanvasTreeNode | undefined, into: Set<string>) {
	if (!node) return;
	if (node.canvas) into.add(node.canvas.id);
	for (const child of node.children) treeCanvasIds(child, into);
}

/**
 * What this canvas may embed: root canvases outside its own tree. A canvas of
 * the same tree would be a self, ancestor or duplicate import, all of which the
 * control plane refuses; a canvas that is already imported elsewhere is not a
 * root, so it never appears here.
 */
export const listImportableCanvases = query(
	v.object({ canvasId: idSchema }),
	async ({ canvasId }): Promise<CanvasOption[]> => {
		const metadata = sessionMetadata(requireSessionId());
		const [roots, tree] = await callGrpc(() =>
			Promise.all([
				orchestrationClient().listCanvases({ includeSubcanvases: false }, { metadata }),
				orchestrationClient().getCanvasTree({ canvasId }, { metadata })
			])
		);
		const own = new Set<string>();
		treeCanvasIds(tree.root, own);
		return roots.canvases
			.filter(canvas => !own.has(canvas.id))
			.map(canvas => ({ id: canvas.id, name: canvas.name, description: canvas.description }))
			.sort((a, b) => a.name.localeCompare(b.name));
	}
);

/**
 * Which canvas of this tree owns `nodeId`. Topology problems are reported for
 * the flattened tree, so a problem opened on one canvas can name a node that
 * lives in another; this is how the editor finds where to go. Empty when the
 * node is nowhere in the tree.
 */
export const locateNodeCanvas = query(
	v.object({ canvasId: idSchema, nodeId: idSchema }),
	async ({ canvasId, nodeId }): Promise<string> => {
		const metadata = sessionMetadata(requireSessionId());
		const tree = await callGrpc(() =>
			orchestrationClient().getCanvasTree({ canvasId }, { metadata })
		);
		const ids = new Set<string>();
		treeCanvasIds(tree.root, ids);
		ids.delete(canvasId); // already on screen: the caller looked there first
		const found = await Promise.all(
			[...ids].map(async id => {
				const detail = await orchestrationClient().getCanvas({ canvasId: id }, { metadata });
				return detail.nodes.some(node => node.id === nodeId) ? id : '';
			})
		);
		return found.find(id => id !== '') ?? '';
	}
);

/**
 * Attaching or detaching a subcanvas moves a canvas between the root listing
 * and its parent's tree, so the shell's listings go stale with the graph — for
 * the canvas that was edited and for the canvas that changed hands.
 */
const refreshSubcanvasViews = (canvasId: string, targetCanvasId: string) =>
	Promise.all([
		getCanvasGraph({ canvasId }).refresh(),
		getCanvasTrail({ canvasId }).refresh(),
		getCanvasTrail({ canvasId: targetCanvasId }).refresh(),
		listImportableCanvases({ canvasId }).refresh(),
		listImportableCanvases({ canvasId: targetCanvasId }).refresh(),
		listCanvases({ includeSubcanvases: false }).refresh(),
		listCanvases({ includeSubcanvases: true }).refresh(),
		listCanvasOptions().refresh()
	]);

/**
 * An export edit reshapes — or relabels — the mirrored port on the importing
 * node, so the parent's graph is as stale as this one. A root canvas has no
 * parent and refreshes only itself.
 */
async function refreshAcrossBoundary(
	canvasId: string,
	metadata: ReturnType<typeof sessionMetadata>
) {
	const detail = await callGrpc(() => orchestrationClient().getCanvas({ canvasId }, { metadata }));
	const parent = detail.ancestors.at(-1);
	await Promise.all([
		getCanvasGraph({ canvasId }).refresh(),
		...(parent ? [getCanvasGraph({ canvasId: parent.id }).refresh()] : [])
	]);
}

/**
 * A brand-new canvas plus the import node that embeds it. The two steps are not
 * atomic in the control plane, so a failed import takes the canvas it just
 * created back out rather than leaving a stray root behind.
 */
export const createSubcanvas = command(
	v.object({ canvasId: idSchema, name: nameSchema, x: coordSchema, y: coordSchema }),
	async ({ canvasId, name, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		const created = await callGrpc(() =>
			orchestrationClient().createCanvas({ name, description: '' }, { metadata })
		);
		const targetCanvasId = created.canvas?.id ?? '';
		try {
			await callGrpc(() =>
				orchestrationClient().createNode(
					{
						canvasId,
						name,
						comment: '',
						spec: { canvasImport: { canvasId: targetCanvasId } },
						position: { x, y },
						itemCount: 0
					},
					{ metadata }
				)
			);
		} catch (err) {
			await orchestrationClient()
				.deleteCanvas({ canvasId: targetCanvasId }, { metadata })
				.catch(() => undefined);
			throw err;
		}
		await refreshSubcanvasViews(canvasId, targetCanvasId);
		return { ok: true as const, subcanvasId: targetCanvasId };
	}
);

/** Embeds an existing canvas. Its target is immutable once the node exists. */
export const importCanvas = command(
	v.object({
		canvasId: idSchema,
		targetCanvasId: idSchema,
		name: nameSchema,
		x: coordSchema,
		y: coordSchema
	}),
	async ({ canvasId, targetCanvasId, name, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().createNode(
				{
					canvasId,
					name,
					comment: '',
					spec: { canvasImport: { canvasId: targetCanvasId } },
					position: { x, y },
					itemCount: 0
				},
				{ metadata }
			)
		);
		await refreshSubcanvasViews(canvasId, targetCanvasId);
		return { ok: true as const };
	}
);

/**
 * One boundary port of this canvas. Creating it reshapes the importer's ports
 * in the same transaction, so the parent gains a matching port at once.
 */
export const createExportNode = command(
	v.object({
		canvasId: idSchema,
		name: nameSchema,
		portKind: portKindSchema,
		exportAs: exportAsSchema,
		x: coordSchema,
		y: coordSchema
	}),
	async ({ canvasId, name, portKind, exportAs, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().createNode(
				{
					canvasId,
					name,
					comment: '',
					spec: {
						canvasExport: { kind: fromPortKind(portKind), direction: fromExportAs(exportAs) }
					},
					position: { x, y },
					itemCount: 0
				},
				{ metadata }
			)
		);
		await refreshAcrossBoundary(canvasId, metadata);
		return { ok: true as const };
	}
);

/**
 * Re-kinding an export reshapes the mirrored port on the importer, which drops
 * whatever edge the parent had attached to it.
 */
export const replaceExportSpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		portKind: portKindSchema,
		exportAs: exportAsSchema
	}),
	async ({ canvasId, nodeId, portKind, exportAs }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().replaceNodeSpec(
				{
					nodeId,
					spec: {
						canvasExport: { kind: fromPortKind(portKind), direction: fromExportAs(exportAs) }
					},
					itemCount: 0
				},
				{ metadata }
			)
		);
		await refreshAcrossBoundary(canvasId, metadata);
		return { ok: true as const };
	}
);

export const createPodNode = command(
	v.object({
		canvasId: idSchema,
		name: nameSchema,
		serverId: idSchema,
		port: portSchema,
		bindIp: optionalIpSchema,
		advertiseIp: optionalIpSchema,
		x: coordSchema,
		y: coordSchema
	}),
	async ({ canvasId, name, serverId, port, bindIp, advertiseIp, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().createNode(
				{
					canvasId,
					name,
					comment: '',
					spec: { pod: { serverId, port, bindIp, advertiseIp } },
					position: { x, y },
					itemCount: 0
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

export const updateNodeText = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		name: nameSchema,
		comment: commentSchema,
		/**
		 * Set for an export node: the parent labels its mirrored port with this
		 * name, so a rename here changes what the parent draws.
		 */
		boundary: v.optional(v.boolean(), false)
	}),
	async ({ canvasId, nodeId, name, comment, boundary }) => {
		const metadata = sessionMetadata(requireSessionId());
		// An unset `position` means "do not move".
		await callGrpc(() =>
			orchestrationClient().updateNodeMeta(
				{ nodeId, name, comment, position: undefined },
				{ metadata }
			)
		);
		if (boundary) await refreshAcrossBoundary(canvasId, metadata);
		else await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

/**
 * `UpdateNodeMeta` replaces name and comment wholesale, so a move has to resend
 * them. Deliberately does not refresh: the dragged position already matches —
 * except for an export node, whose `position.y` orders the mirrored ports on
 * the importing node, so the parent has to be re-read.
 */
export const moveNode = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		name: nameSchema,
		comment: commentSchema,
		x: coordSchema,
		y: coordSchema,
		boundary: v.optional(v.boolean(), false)
	}),
	async ({ canvasId, nodeId, name, comment, x, y, boundary }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().updateNodeMeta(
				{ nodeId, name, comment, position: { x, y } },
				{ metadata }
			)
		);
		if (boundary) await refreshAcrossBoundary(canvasId, metadata);
		return { ok: true as const };
	}
);

/**
 * A `null` `tls` drops TLS termination from the spec, which clears it. The
 * certificate itself is not touched: the derivation pass creates a certificate
 * row from this config, and `/tls` manages the result.
 */
export const replaceEntrySpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		receiveProxyProtocol: proxySchema,
		tls: tlsSchema
	}),
	async ({ canvasId, nodeId, receiveProxyProtocol, tls }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().replaceNodeSpec(
				{
					nodeId,
					// `tls: undefined` is the wire form of "no TLS on this entry".
					spec: {
						entry: { receiveProxyProtocol: fromProxy(receiveProxyProtocol), tls: tls ?? undefined }
					},
					itemCount: 0
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

export const replaceRelaySpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		protocol: relayProtocolSchema,
		overrideIpAddress: v.optional(v.pipe(v.string(), v.trim()), ''),
		overridePort: v.pipe(v.number(), v.integer(), v.minValue(0), v.maxValue(65535))
	}),
	async ({ canvasId, nodeId, protocol, overrideIpAddress, overridePort }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().replaceNodeSpec(
				{
					nodeId,
					spec: {
						relay: { protocol: fromRelayProtocol(protocol), overrideIpAddress, overridePort }
					},
					itemCount: 0
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

export const replaceExitSpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		destination: v.optional(v.pipe(v.string(), v.trim()), ''),
		passProxyProtocol: proxySchema
	}),
	async ({ canvasId, nodeId, destination, passProxyProtocol }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().replaceNodeSpec(
				{
					nodeId,
					spec: { exit: { destination, passProxyProtocol: fromProxy(passProxyProtocol) } },
					itemCount: 0
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

/**
 * A distribute node's mode and protocol apply to every channel at once. A
 * protocol change re-rolls the ports of every landing pod its channels reach,
 * since a listener cannot change protocol in place; the control plane does that
 * in the same write. The on-demand ports survive a member-count change.
 */
export const replaceLoadBalanceSpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		mode: v.picklist(['distribute', 'aggregate'] as const),
		balanceMode: balanceModeSchema,
		protocol: v.optional(relayProtocolSchema, 'tcp_raw'),
		memberCount: memberCountSchema
	}),
	async ({ canvasId, nodeId, mode, balanceMode, protocol, memberCount }) => {
		const metadata = sessionMetadata(requireSessionId());
		// The spec kind cannot change, so `mode` only picks which config to resend.
		await callGrpc(() =>
			orchestrationClient().replaceNodeSpec(
				{
					nodeId,
					spec:
						mode === 'distribute'
							? {
									loadBalanceDistribute: {
										mode: fromBalanceMode(balanceMode),
										protocol: fromRelayProtocol(protocol)
									}
								}
							: { loadBalanceAggregate: {} },
					itemCount: memberCount
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

export const replacePodSpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		serverId: idSchema,
		port: portSchema,
		bindIp: optionalIpSchema,
		advertiseIp: optionalIpSchema
	}),
	async ({ canvasId, nodeId, serverId, port, bindIp, advertiseIp }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().replaceNodeSpec(
				{ nodeId, spec: { pod: { serverId, port, bindIp, advertiseIp } }, itemCount: 0 },
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

export const deleteNode = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		force: v.optional(v.boolean(), false),
		/**
		 * The canvas an import node embeds. Retiring it hands that canvas back to
		 * the root listing, which every listing and both trails have to be told
		 * about. The caller supplies it because the node is gone by the time the
		 * graph is re-read.
		 */
		subcanvasTarget: v.optional(v.string(), ''),
		/** Set for an export node: its mirrored port leaves the parent's graph. */
		boundary: v.optional(v.boolean(), false)
	}),
	async ({ canvasId, nodeId, force, subcanvasTarget, boundary }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			force
				? orchestrationClient().forceDeleteNode({ nodeId }, { metadata })
				: orchestrationClient().retireNode({ nodeId }, { metadata })
		);
		if (subcanvasTarget) await refreshSubcanvasViews(canvasId, subcanvasTarget);
		else if (boundary) await refreshAcrossBoundary(canvasId, metadata);
		else await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

const universalGroupSchema = v.picklist(['channel_out', 'bundle_in', 'bundle_out'] as const);
/** One end of a connect: a port id, or a universal node's handle group. */
const connectEndSchema = v.union([
	v.object({ portId: idSchema }),
	v.object({ nodeId: idSchema, group: universalGroupSchema })
]);
const fromGroup = (value: UniversalGroupName): UniversalGroup =>
	value === 'channel_out'
		? UniversalGroup.CHANNEL_OUT
		: value === 'bundle_in'
			? UniversalGroup.BUNDLE_IN
			: UniversalGroup.BUNDLE_OUT;

/**
 * Connects two ports, or a universal node's handle group to a port / another
 * group: the port behind a group is created by the control plane in the same
 * write as the edge and the lanes it calls for.
 */
export const connectNodePorts = command(
	v.object({ canvasId: idSchema, output: connectEndSchema, input: connectEndSchema }),
	async ({ canvasId, output, input }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().connectPorts(
				{
					outputPortId: 'portId' in output ? output.portId : '',
					inputPortId: 'portId' in input ? input.portId : '',
					outputHandle:
						'portId' in output
							? undefined
							: { nodeId: output.nodeId, group: fromGroup(output.group) },
					inputHandle:
						'portId' in input ? undefined : { nodeId: input.nodeId, group: fromGroup(input.group) }
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

export const disconnectEdge = command(
	v.object({ canvasId: idSchema, edgeId: idSchema, force: v.optional(v.boolean(), false) }),
	async ({ canvasId, edgeId, force }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			force
				? orchestrationClient().forceDisconnect({ edgeId }, { metadata })
				: orchestrationClient().disconnect({ edgeId }, { metadata })
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);
