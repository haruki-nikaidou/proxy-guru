import type { CanvasOption, CanvasProblem } from '#lib/dto/canvas.js';

/**
 * The canvas editor's view of a topology. Protobuf never reaches the client:
 * numeric enums become string unions and `int64` positions become numbers.
 */

export type PortKindName = 'derive_listen' | 'derive_destination' | 'bundle';
/** The kinds an export node may mirror: bundles never cross a canvas boundary. */
export type ExportPortKindName = 'derive_listen' | 'derive_destination';
export type PortDirectionName = 'input' | 'output';
export type ProxyProtocolName = 'none' | 'v1' | 'v2';
export type RelayProtocolName = 'tcp_raw' | 'tcp_tls' | 'quic';
export type LoadBalanceModeName = 'round_robin' | 'random' | 'ip_hash' | 'fallback';
export type Ipv6ResolveName = 'required' | 'preferred' | 'tolerated' | 'forbidden';
export type QuicCongestionName = 'cubic' | 'brutal';
/**
 * A server's side of every QUIC relay link it takes part in. `upMbps` is what
 * it sends at (brutal's fixed rate), `downMbps` what it can receive; the master
 * pairs each link's two ends. Zero means "quinn's default".
 */
export type ServerQuicDto = {
	congestion: QuicCongestionName;
	upMbps: number;
	downMbps: number;
	/** Bytes; 0 derives the per-stream window from `downMbps`. */
	streamReceiveWindow: number;
	/** Bytes; 0 leaves the whole-connection window unlimited. */
	connReceiveWindow: number;
};
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
/**
 * A load-balance node. The operator's rule is its **members**: named bundle
 * ports, one per member, in the order the operator listed them — the bundles
 * out of a distribute node, the bundles into an aggregate node. Everything
 * else on the card is grown by the control plane: a distribute node takes
 * entry pods as *channels* and collects upstream bundles on the left, an
 * aggregate node grows one coloured input per channel for an exit.
 */
export type LoadBalanceNodeDto = NodeBase & {
	kind: 'load_balance';
	mode: 'distribute' | 'aggregate';
	/** Only meaningful for `distribute`; `aggregate` has no mode field in the proto. */
	balanceMode: LoadBalanceModeName;
	/** How a distribute node's channels are relayed to the universal pods. */
	protocol: RelayProtocolName;
	/** The declared members, in order, each with its bundle port. */
	members: MemberDto[];
	/**
	 * The channels this node carries: drawn into a distribute node (`portId`
	 * is its `chan:` output), or brought by bundles into an aggregate node
	 * (`portId` is the `chan:` input that takes the exit).
	 */
	channels: (ChannelDto & { portId: string })[];
	/**
	 * A distribute node's `bundle_in:` ports, one per upstream bundle
	 * collected on it, with the far node's name; always empty on an aggregate
	 * node, which takes bundles on its members.
	 */
	bundlesIn: BundlePortDto[];
};
/**
 * One member as the operator declared it. `slot` is the stable number behind
 * the port key (`member_<slot>`): renaming or reordering keeps the bundle drawn
 * on the port. `peerName` is the far end of that bundle, or `null` while the
 * member is unwired.
 */
export type MemberDto = {
	slot: number;
	name: string;
	port: CanvasPort;
	peerName: string | null;
};
/** A bundle port and what is on the other end of its bundle. */
export type BundlePortDto = CanvasPort & { peerName: string };
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
	portKind: ExportPortKindName;
	exportAs: CanvasExportAsName;
};
/**
 * The handle groups a bundle-capable node offers next to its one-edge ports:
 * the ports the control plane creates on connect. Bundles leave through ports
 * that exist already (a member, a universal pod's `bundle out`).
 */
export type UniversalGroupName = 'channel_out' | 'bundle_in';
/**
 * One channel: an entry pod connected to a distribute node. `ordinal` is
 * the position of its `chan:` port, handed out once per canvas tree and never
 * reused, so `colorIndex` (ordinal modulo the palette) stays put when other
 * channels come and go.
 */
export type ChannelDto = {
	podId: string;
	podName: string;
	ordinal: number;
	colorIndex: number;
	distributorId: string;
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
/**
 * A generated pod on this server: one per channel per bundle landing here.
 * Only its port and addresses are the operator's to edit; the row itself goes
 * with the bundle that brought it.
 */
export type LaneDto = {
	nodeId: string;
	channel: ChannelDto;
	/** The node the bundle came from: a distribute node's name or a server's. */
	sourceName: string;
	serverId: string;
	port: number;
	bindIp: string | null;
	advertiseIp: string | null;
	/** `false` while the channel goes nowhere from this server (`CHANNEL_NO_EXIT`). */
	hasExit: boolean;
};
/** The server's universal pod, drawn inside the server card. */
export type UniversalPodDto = {
	nodeId: string;
	/** `bundle_in:<source>` ports, one per bundle drawn into it. */
	bundleIn: BundlePortDto[];
	/**
	 * Entry pods drawn straight into this universal pod (a raw TCP hop of their
	 * own), each with the `chan:` output the edge starts at.
	 */
	channels: (ChannelDto & { portId: string })[];
	/** The fixed outgoing bundle port. */
	bundleOut: CanvasPort | null;
	lanes: LaneDto[];
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
	/**
	 * ISO 3166-1 alpha-2 of the server's IPv4 address, as the control plane
	 * looked it up; empty until a lookup for that address succeeded. The card's
	 * flag falls back to it when no icon is set.
	 */
	country: string;
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
	quic: ServerQuicDto;
	/** Advanced by the config stream's heartbeat as well as by health reports. */
	lastSeenAt: string;
	/** When the worker's last health report was accepted; empty until one is. */
	lastHealthReportAt: string;
	healthStatus: ServerHealthStatusName;
	addresses: ServerAddressesDto;
	/** The worker build the last registration reported; empty until one does. */
	agentVersion: string;
	agentArch: string;
	/** The systemd instance the install command creates: `guru-worker@<unit>`. */
	agentUnit: string;
	/** A pending self-update (the requested version), and why the last one failed. */
	agentUpdateRequested: string;
	agentUpdateError: string;
	/** When the server's own agent key was issued; empty when it has none. */
	agentKeyIssuedAt: string;
	pods: PodDto[];
	/** `null` only for a server created before universal pods existed. */
	universal: UniversalPodDto | null;
};

/** The install command issued for a server, carrying its freshly issued key. */
export type AgentInstallDto = { command: string; unit: string; version: string };
/** The published worker release; `version` empty means none is published yet. */
export type AgentReleaseDto = {
	version: string;
	sha256: string;
	arch: string;
	publishedAt: string;
	/** The control plane knows its public origin, so a command can be rendered. */
	baseUrlConfigured: boolean;
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
	/** Every channel of the canvas, by entry pod id. */
	channels: Record<string, ChannelDto>;
};
