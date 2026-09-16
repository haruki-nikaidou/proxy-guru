import type { Graph, Pod } from 'guru-graph';

/**
 * The canvas editor's view of a canvas tree. Protobuf never reaches the client:
 * numeric enums become string unions and `int64` values become numbers. The
 * graph itself — pods, exits, edges, routes, groups — is `guru-graph`'s model,
 * which is plain JSON and travels as it is.
 */

export type ProxyProtocolName = 'none' | 'v1' | 'v2';
export type Ipv6ResolveName = 'required' | 'preferred' | 'tolerated' | 'forbidden';
export type QuicCongestionName = 'cubic' | 'brutal';
/** The level a server's worker logs at; the worker switches to a new one live. */
export type LogLevelName = 'trace' | 'debug' | 'info' | 'warn' | 'error';
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
/** A server: a set of pods, and the host they run on. */
export type ServerDto = {
	id: string;
	canvasId: string;
	name: string;
	icon: string;
	comment: string;
	x: number;
	y: number;
	ipv6Resolve: Ipv6ResolveName;
	logLevel: LogLevelName;
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
	/** What the worker reads beyond the tree form (`route_table`, `relay_confirm`). */
	capabilities: string[];
};

/** What a diagnostic is about. */
export type DiagnosticSubjectDto =
	| { server: string }
	| { pod: string }
	| { exit: string }
	| { edge: string }
	| { group: string }
	| { canvas: string };

/**
 * A problem with the graph, or with a change to it. `problem` is a stable
 * snake_case name for grouping; `message` is the control plane's own sentence.
 * An error blocks a write, a warning never does.
 */
export type DiagnosticDto = {
	problem: string;
	error: boolean;
	subjects: DiagnosticSubjectDto[];
	message: string;
};

/** A canvas tree as `GetGraph` answers it: the whole tree, whichever canvas was asked. */
export type CanvasGraph = Graph<ServerDto> & { diagnostics: DiagnosticDto[] };

/** What `ApplyGraph` answered: whether it wrote, and what it found. */
export type ApplyOutcomeDto = {
	applied: boolean;
	generation: number;
	/** Every diagnostic of the graph the change leads to; any error means nothing was written. */
	diagnostics: DiagnosticDto[];
	/** The pods the change put, as written: a pod put with port 0 carries its port. */
	pods: Pod[];
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
export type InvalidPodDto = { podId: string; podName: string; listen: string; error: string };
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
