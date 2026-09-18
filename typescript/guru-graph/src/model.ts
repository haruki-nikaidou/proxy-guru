/**
 * The pod graph of one canvas tree, as the control plane stores it.
 *
 * A pod is one listener on one server and a vertex of a directed acyclic graph;
 * an edge sends a pod's traffic on to another pod (dialed in the protocol that
 * pod listens with) or to an exit; each pod's route is a tree over exactly its
 * own out-edges. Groups are how the dashboard remembers what it drew; nothing is
 * derived from them.
 */

export type Id = string;

export type ProxyVersion = 'v1' | 'v2';

/** The ACME certificate a TLS client listener terminates with. */
export type TlsConfig = {
	sni: string;
	dnsProviderId: string;
	domainId: string;
	/** Empty means the control plane's default directory. */
	acmeDirectory: string;
};

/** How traffic arrives at a pod. */
export type Ingress =
	| { kind: 'client_raw'; receiveProxyProtocol: ProxyVersion | null }
	| { kind: 'client_tls'; receiveProxyProtocol: ProxyVersion | null; tls: TlsConfig }
	| { kind: 'relay_tcp' }
	| { kind: 'relay_tls' }
	| { kind: 'relay_quic' };

export type IngressKind = Ingress['kind'];
export type RelayKind = 'relay_tcp' | 'relay_tls' | 'relay_quic';

export const isRelay = (ingress: Ingress): boolean => ingress.kind.startsWith('relay_');

/**
 * The route document a pod stores: a leaf names one of the pod's out-edges, a
 * `balance` spreads connections over its members by weight (optionally sticky
 * by client address), a `failover` uses the first member that is alive.
 */
export type Route =
	| { edge: Id }
	| { balance: Weighted[]; sticky?: 'client_ip' }
	| { failover: Route[] };

/** A balance member; an absent weight is 1. */
export type Weighted = { weight?: number; to: Route };

export type Canvas = {
	id: Id;
	name: string;
	description: string;
	parentId: Id | null;
	/** Where the canvas is drawn on its parent. */
	x: number;
	y: number;
};

/** What the graph needs to know about a server; the dashboard's rows carry more. */
export type Server = {
	id: Id;
	canvasId: Id;
	name: string;
	x: number;
	y: number;
};

export type Pod = {
	id: Id;
	/** The canvas the pod is drawn on; any canvas of its server's tree. */
	canvasId: Id;
	serverId: Id;
	name: string;
	comment: string;
	/** 0 asks the control plane for a free port. */
	port: number;
	bindIp: string | null;
	advertiseIp: string | null;
	ingress: Ingress;
	route: Route | null;
};

export type Exit = {
	id: Id;
	canvasId: Id;
	name: string;
	comment: string;
	/** `host:port`. */
	destination: string;
	sendProxyProtocol: ProxyVersion | null;
	x: number;
	y: number;
};

export type EdgeTarget = { pod: Id } | { exit: Id };

/**
 * Which of its target pod's addresses an edge dials: `auto` is the server's
 * effective address (IPv4 when it has one); `v4` and `v6` insist on that
 * family — the pod's advertised address when it is of it, else the server's
 * address of it. An override address wins over all three, and an edge into an
 * exit dials the exit's destination as written.
 */
export type IpFamily = 'auto' | 'v4' | 'v6';

export type Edge = {
	id: Id;
	sourcePodId: Id;
	target: EdgeTarget;
	overrideIp: string | null;
	overridePort: number | null;
	ipFamily: IpFamily;
};

export type GroupMember = { pod: Id } | { edge: Id } | { exit: Id } | { server: Id };

export type Group = {
	id: Id;
	canvasId: Id;
	kind: string;
	name: string;
	props: Record<string, unknown>;
	members: GroupMember[];
};

export type Graph<S extends Server = Server> = {
	/** Root first. */
	canvases: Canvas[];
	servers: S[];
	pods: Pod[];
	exits: Exit[];
	edges: Edge[];
	groups: Group[];
	/** The root's edit counter the graph was read at. */
	generation: number;
};

/** One batch of changes, all of it or none. */
export type GraphChange = {
	putPods: Pod[];
	putExits: Exit[];
	putEdges: Edge[];
	putGroups: Group[];
	deletePodIds: Id[];
	deleteExitIds: Id[];
	deleteEdgeIds: Id[];
	deleteGroupIds: Id[];
};

export const emptyChange = (): GraphChange => ({
	putPods: [],
	putExits: [],
	putEdges: [],
	putGroups: [],
	deletePodIds: [],
	deleteExitIds: [],
	deleteEdgeIds: [],
	deleteGroupIds: []
});

export const isEmptyChange = (change: GraphChange): boolean =>
	change.putPods.length === 0 &&
	change.putExits.length === 0 &&
	change.putEdges.length === 0 &&
	change.putGroups.length === 0 &&
	change.deletePodIds.length === 0 &&
	change.deleteExitIds.length === 0 &&
	change.deleteEdgeIds.length === 0 &&
	change.deleteGroupIds.length === 0;

export const edgeTargetPod = (edge: Edge): Id | null =>
	'pod' in edge.target ? edge.target.pod : null;
export const edgeTargetExit = (edge: Edge): Id | null =>
	'exit' in edge.target ? edge.target.exit : null;
