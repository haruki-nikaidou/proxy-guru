import type { CanvasOption, CanvasProblem } from '#lib/dto/canvas.js';

/**
 * The canvas editor's view of a topology. Protobuf never reaches the client:
 * numeric enums become string unions and `int64` positions become numbers.
 */

export type PortKindName = 'derive_listen' | 'derive_destination';
export type PortDirectionName = 'input' | 'output';
export type ProxyProtocolName = 'none' | 'v1' | 'v2';
export type RelayProtocolName = 'tcp_raw' | 'tcp_tls' | 'quic';
export type LoadBalanceModeName = 'round_robin' | 'random' | 'ip_hash' | 'fallback';
export type Ipv6ResolveName = 'required' | 'preferred' | 'tolerated' | 'forbidden';
/**
 * Which side of the boundary an export node feeds, named from the subcanvas's
 * point of view: `input_into_canvas` emits inside, so the mirrored port on the
 * importer is an input.
 */
export type CanvasExportAsName = 'input_into_canvas' | 'output_out_of_canvas';

export type CanvasPort = {
	id: string;
	kind: PortKindName;
	direction: PortDirectionName;
	/** Derived server-side (`port_layout`); the client never invents one. */
	key: string;
	position: number;
	/**
	 * An import port is keyed by the record id of the export node it mirrors,
	 * which is meaningless on screen: the label is that export node's name,
	 * resolved from the target canvas. `null` for every other port.
	 */
	label: string | null;
};

type NodeBase = {
	id: string;
	name: string;
	comment: string;
	x: number;
	y: number;
	ports: CanvasPort[];
};

/**
 * ACME/TLS termination for an entry, edited by the entry sheet. `null` means the
 * entry terminates no TLS; sending `null` through `ReplaceNodeSpec` clears it.
 * The certificate itself is never created here: the derivation pass turns this
 * config into a certificate row, which is managed on `/tls`.
 */
export type EntryTls = {
	sni: string;
	dnsProviderId: string;
	domainId: string;
	acmeDirectory: string;
} | null;

export type EntryNodeDto = NodeBase & {
	kind: 'entry';
	receiveProxyProtocol: ProxyProtocolName;
	tls: EntryTls;
};
export type RelayNodeDto = NodeBase & {
	kind: 'relay';
	protocol: RelayProtocolName;
	overrideIpAddress: string;
	overridePort: number;
};
export type ExitNodeDto = NodeBase & {
	kind: 'exit';
	destination: string;
	passProxyProtocol: ProxyProtocolName;
};
export type LoadBalanceNodeDto = NodeBase & {
	kind: 'load_balance';
	mode: 'distribute' | 'aggregate';
	/** Only meaningful for `distribute`; `aggregate` has no mode field in the proto. */
	balanceMode: LoadBalanceModeName;
	memberCount: number;
};
/**
 * Embeds another canvas as one node. Its ports mirror the target's export
 * nodes and are derived server-side; the target itself is immutable.
 */
export type CanvasImportNodeDto = NodeBase & {
	kind: 'canvas_import';
	targetCanvasId: string;
	/** Empty when the target was deleted out from under the import. */
	targetName: string;
};
/** One boundary port of the canvas it sits on, seen as a port on the importer. */
export type CanvasExportNodeDto = NodeBase & {
	kind: 'canvas_export';
	portKind: PortKindName;
	exportAs: CanvasExportAsName;
};
export type StandaloneNode =
	| EntryNodeDto
	| RelayNodeDto
	| ExitNodeDto
	| LoadBalanceNodeDto
	| CanvasImportNodeDto
	| CanvasExportNodeDto;

export type PodDto = {
	id: string;
	name: string;
	comment: string;
	serverId: string;
	port: number;
	/** `null` binds every address of the host (dual-stack). */
	bindIp: string | null;
	/** `null` means the server's effective address. */
	advertiseIp: string | null;
	ports: CanvasPort[];
};
/** One of the two fixed address slots of a server; empty strings mean unset. */
export type AddressSlotDto = { reported: string; pinned: string };
export type AddressSourceName = 'override' | 'reported' | 'observed' | 'none';
export type ServerAddressesDto = {
	v4: AddressSlotDto;
	v6: AddressSlotDto;
	extra: string[];
	reportedInterfaces: string[];
	reportedAt: string;
	observedAddress: string;
	observedAt: string;
	/** What other servers dial by default; empty when nothing is known yet. */
	effectiveAddress: string;
	effectiveSource: AddressSourceName;
	/** ISO 3166-1 alpha-2 of the public address, as the worker reported it. */
	reportedCountry: string;
};
/**
 * How the control plane last judged a worker. `unknown` covers both "never
 * reported" and an enum value this build does not know.
 */
export type ServerHealthStatusName = 'unknown' | 'online' | 'degraded' | 'offline';
export type ServerDto = {
	id: string;
	name: string;
	icon: string;
	comment: string;
	x: number;
	y: number;
	ipv6Resolve: Ipv6ResolveName;
	logLevel: string;
	lastSeenAt: string;
	healthStatus: ServerHealthStatusName;
	addresses: ServerAddressesDto;
	pods: PodDto[];
};

/** One listener a forwarding either serves or points at, by its server. */
export type ListenerCapDto = { serverId: string; port: number; protocol: string };
export type ForwardingDepsDto = {
	/** Absent when the forwarding serves nothing (a pure outbound hop). */
	serves: ListenerCapDto | null;
	pointsAt: ListenerCapDto[];
};
/** `revision` is an `int64` in the proto, narrowed to a number by the remote. */
export type ConfigSnapshotDto = {
	revision: number;
	createdAt: string;
	forwardings: ForwardingDepsDto[];
};
/**
 * A pod the derivation pass could not turn into a listener. Only these pods
 * failed: the rest of the server's config was published normally, and each pod
 * listed here keeps whatever listener shape it was already serving.
 */
export type InvalidPodDto = { nodeId: string; podName: string; listen: string; error: string };
/**
 * The three-state view of one server's config rollout: what the control plane
 * wants, what it handed to the worker, and what the worker confirmed.
 */
export type ServerRolloutDto = {
	desired: ConfigSnapshotDto | null;
	inFlight: ConfigSnapshotDto | null;
	applied: ConfigSnapshotDto | null;
	applyError: string;
	deriveError: string;
	/** Servers that must serve a listener this one points at before it converges. */
	waitingForServerIds: string[];
	derivationPending: boolean;
	lastSeenAt: string;
	invalidPods: InvalidPodDto[];
};
/** The rendered worker TOML, fetched on demand. */
export type ServerConfigTomlDto = { revision: number; toml: string };

export type CanvasEdgeDto = { id: string; sourcePortId: string; targetPortId: string };
export type TopologyProblem = CanvasProblem & {
	nodeIds: string[];
	edgeIds: string[];
	portIds: string[];
};

export type CanvasGraph = {
	canvas: { id: string; name: string; description: string };
	servers: ServerDto[];
	nodes: StandaloneNode[];
	edges: CanvasEdgeDto[];
	problems: TopologyProblem[];
	/** Pods placed on a server that is not on this canvas. */
	orphanPods: PodDto[];
	/** Root first, parent last; empty when this canvas is a root. */
	ancestors: CanvasOption[];
};
