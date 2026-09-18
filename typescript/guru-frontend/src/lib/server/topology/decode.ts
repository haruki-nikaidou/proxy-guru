/**
 * Protobuf replies as the DTOs in `#lib/dto/topology.js` and the graph model of
 * `guru-graph`. Pure mapping: no transport, no validation — the remote functions
 * in `routes/(canvas)/canvas/[canvasId]/` do the calling, this does the shaping.
 * Scalar enums are mapped in `./enums.js`; the way back is `./encode.js`.
 */
import type {
	ApplyGraphReply,
	GetGraphReply,
	Canvas as ProtoCanvas,
	ConfigSnapshot as ProtoConfigSnapshot,
	Diagnostic as ProtoDiagnostic,
	Edge as ProtoEdge,
	Exit as ProtoExit,
	Group as ProtoGroup,
	Pod as ProtoPod,
	Server as ProtoServer,
	ServerAddresses as ProtoServerAddresses
} from 'app-protobuf/orchestration/orchestration';
import { AddressSource, QuicCongestion } from 'app-protobuf/orchestration/orchestration';
import type { Canvas, Edge, Exit, Group, GroupMember, Ingress, Pod, Route } from 'guru-graph';
import type {
	ApplyOutcomeDto,
	CanvasGraph,
	ConfigSnapshotDto,
	DiagnosticDto,
	DiagnosticSubjectDto,
	ServerAddressesDto,
	ServerDto
} from '#lib/dto/topology.js';
import {
	toAddressSource,
	toIngressKind,
	toIpFamily,
	toIpv6,
	toLogLevel,
	toProxyVersion,
	toQuicCongestion,
	toServerHealth
} from './enums.js';

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
	country: addresses?.country ?? ''
});

export const toServer = (server: ProtoServer): ServerDto => ({
	id: server.id,
	canvasId: server.canvasId,
	name: server.name,
	icon: server.icon,
	comment: server.comment,
	x: Number(server.position?.x ?? 0n),
	y: Number(server.position?.y ?? 0n),
	ipv6Resolve: toIpv6(server.ipv6Resolve),
	logLevel: toLogLevel(server.logLevel),
	quic: {
		congestion: toQuicCongestion(server.quic?.congestion ?? QuicCongestion.UNSPECIFIED),
		upMbps: server.quic?.upMbps ?? 0,
		downMbps: server.quic?.downMbps ?? 0,
		streamReceiveWindow: Number(server.quic?.streamReceiveWindow ?? 0n),
		connReceiveWindow: Number(server.quic?.connReceiveWindow ?? 0n)
	},
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
	capabilities: [...server.capabilities]
});

export const toCanvas = (canvas: ProtoCanvas): Canvas => ({
	id: canvas.id,
	name: canvas.name,
	description: canvas.description,
	parentId: canvas.parentId === '' ? null : canvas.parentId,
	x: Number(canvas.position?.x ?? 0n),
	y: Number(canvas.position?.y ?? 0n)
});

/** A JSON document the control plane wrote; `fallback` when it is empty or unreadable. */
function parseJson<T>(json: string, fallback: T): T {
	if (json.trim() === '') return fallback;
	try {
		return JSON.parse(json) as T;
	} catch {
		return fallback;
	}
}

function toIngress(pod: ProtoPod): Ingress {
	const kind = toIngressKind(pod.ingress);
	const receiveProxyProtocol = toProxyVersion(pod.receiveProxyProtocol);
	switch (kind) {
		case 'client_tls':
			return {
				kind,
				receiveProxyProtocol,
				tls: {
					sni: pod.tls?.sni ?? '',
					dnsProviderId: pod.tls?.dnsProviderId ?? '',
					domainId: pod.tls?.domainId ?? '',
					acmeDirectory: pod.tls?.acmeDirectory ?? ''
				}
			};
		case 'client_raw':
			return { kind, receiveProxyProtocol };
		default:
			return { kind };
	}
}

export const toPod = (pod: ProtoPod): Pod => ({
	id: pod.id,
	canvasId: pod.canvasId,
	serverId: pod.serverId,
	name: pod.name,
	comment: pod.comment,
	port: pod.port,
	bindIp: pod.bindIp === '' ? null : pod.bindIp,
	advertiseIp: pod.advertiseIp === '' ? null : pod.advertiseIp,
	ingress: toIngress(pod),
	route: parseJson<Route | null>(pod.routeJson, null)
});

export const toExit = (exit: ProtoExit): Exit => ({
	id: exit.id,
	canvasId: exit.canvasId,
	name: exit.name,
	comment: exit.comment,
	destination: exit.destination,
	sendProxyProtocol: toProxyVersion(exit.sendProxyProtocol),
	x: Number(exit.position?.x ?? 0n),
	y: Number(exit.position?.y ?? 0n)
});

/** An edge naming no target is dropped: the control plane never stores one. */
export const toEdge = (edge: ProtoEdge): Edge | null => {
	const target =
		edge.targetPodId !== undefined
			? { pod: edge.targetPodId }
			: edge.targetExitId !== undefined
				? { exit: edge.targetExitId }
				: null;
	if (!target) return null;
	return {
		id: edge.id,
		sourcePodId: edge.sourcePodId,
		target,
		overrideIp: edge.overrideIp === '' ? null : edge.overrideIp,
		overridePort: edge.overridePort === 0 ? null : edge.overridePort,
		ipFamily: toIpFamily(edge.ipFamily)
	};
};

export const toGroup = (group: ProtoGroup): Group => ({
	id: group.id,
	canvasId: group.canvasId,
	kind: group.kind,
	name: group.name,
	props: parseJson<Record<string, unknown>>(group.propsJson, {}),
	members: group.members.flatMap((member): GroupMember[] =>
		member.podId !== undefined
			? [{ pod: member.podId }]
			: member.edgeId !== undefined
				? [{ edge: member.edgeId }]
				: member.exitId !== undefined
					? [{ exit: member.exitId }]
					: member.serverId !== undefined
						? [{ server: member.serverId }]
						: []
	)
});

export const toDiagnostic = (diagnostic: ProtoDiagnostic): DiagnosticDto => ({
	problem: diagnostic.problem,
	error: diagnostic.error,
	subjects: diagnostic.subjects.flatMap((subject): DiagnosticSubjectDto[] =>
		subject.serverId !== undefined
			? [{ server: subject.serverId }]
			: subject.podId !== undefined
				? [{ pod: subject.podId }]
				: subject.exitId !== undefined
					? [{ exit: subject.exitId }]
					: subject.edgeId !== undefined
						? [{ edge: subject.edgeId }]
						: subject.groupId !== undefined
							? [{ group: subject.groupId }]
							: subject.canvasId !== undefined
								? [{ canvas: subject.canvasId }]
								: []
	),
	message: diagnostic.message
});

export const toCanvasGraph = (reply: GetGraphReply): CanvasGraph => ({
	canvases: reply.canvases.map(toCanvas),
	servers: reply.servers.map(toServer),
	pods: reply.pods.map(toPod),
	exits: reply.exits.map(toExit),
	edges: reply.edges.flatMap(edge => toEdge(edge) ?? []),
	groups: reply.groups.map(toGroup),
	generation: Number(reply.generation),
	diagnostics: reply.diagnostics.map(toDiagnostic)
});

export const toApplyOutcome = (reply: ApplyGraphReply): ApplyOutcomeDto => ({
	applied: reply.applied,
	generation: Number(reply.generation),
	diagnostics: reply.diagnostics.map(toDiagnostic),
	pods: reply.pods.map(toPod)
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
