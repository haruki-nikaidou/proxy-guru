/**
 * The graph model of `guru-graph` as the protobuf messages `ApplyGraph` takes.
 * The inverse of `./decode.js`: `null` becomes the empty string or 0 the proto
 * uses for "none", and routes and group props travel as JSON documents.
 */
import type {
	Edge as ProtoEdge,
	Exit as ProtoExit,
	GraphChange as ProtoGraphChange,
	Group as ProtoGroup,
	Pod as ProtoPod
} from 'app-protobuf/orchestration/orchestration';
import { ProxyProtocolVersion } from 'app-protobuf/orchestration/orchestration';
import type { Edge, Exit, GraphChange, Group, Pod } from 'guru-graph';
import { fromIngressKind, fromProxy } from './enums.js';

export const fromPod = (pod: Pod): ProtoPod => ({
	id: pod.id,
	canvasId: pod.canvasId,
	serverId: pod.serverId,
	name: pod.name,
	comment: pod.comment,
	port: pod.port,
	bindIp: pod.bindIp ?? '',
	advertiseIp: pod.advertiseIp ?? '',
	ingress: fromIngressKind(pod.ingress.kind),
	receiveProxyProtocol:
		pod.ingress.kind === 'client_raw' || pod.ingress.kind === 'client_tls'
			? fromProxy(pod.ingress.receiveProxyProtocol)
			: ProxyProtocolVersion.UNSPECIFIED,
	tls: pod.ingress.kind === 'client_tls' ? { ...pod.ingress.tls } : undefined,
	routeJson: pod.route ? JSON.stringify(pod.route) : ''
});

export const fromExit = (exit: Exit): ProtoExit => ({
	id: exit.id,
	canvasId: exit.canvasId,
	name: exit.name,
	comment: exit.comment,
	destination: exit.destination,
	sendProxyProtocol: fromProxy(exit.sendProxyProtocol),
	position: { x: BigInt(Math.round(exit.x)), y: BigInt(Math.round(exit.y)) }
});

export const fromEdge = (edge: Edge): ProtoEdge => ({
	id: edge.id,
	sourcePodId: edge.sourcePodId,
	...('pod' in edge.target ? { targetPodId: edge.target.pod } : { targetExitId: edge.target.exit }),
	overrideIp: edge.overrideIp ?? '',
	overridePort: edge.overridePort ?? 0
});

export const fromGroup = (group: Group): ProtoGroup => ({
	id: group.id,
	canvasId: group.canvasId,
	kind: group.kind,
	name: group.name,
	propsJson: JSON.stringify(group.props),
	members: group.members.map(member =>
		'pod' in member
			? { podId: member.pod }
			: 'edge' in member
				? { edgeId: member.edge }
				: 'exit' in member
					? { exitId: member.exit }
					: { serverId: member.server }
	)
});

export const fromChange = (change: GraphChange): ProtoGraphChange => ({
	putPods: change.putPods.map(fromPod),
	putExits: change.putExits.map(fromExit),
	putEdges: change.putEdges.map(fromEdge),
	putGroups: change.putGroups.map(fromGroup),
	deletePodIds: [...change.deletePodIds],
	deleteExitIds: [...change.deleteExitIds],
	deleteEdgeIds: [...change.deleteEdgeIds],
	deleteGroupIds: [...change.deleteGroupIds]
});
