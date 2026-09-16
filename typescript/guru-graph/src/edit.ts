/**
 * What a gesture on the drawing asks of the graph, as one change batch.
 *
 * Every function takes the graph as the dashboard last read it and returns the
 * rows to put and delete; nothing is written here, and the control plane checks
 * the batch as a whole. A gesture that cannot mean anything throws an
 * [`EditError`] naming why, for the dashboard to say in the operator's language.
 */

import type { Drawing, Point, SplitterCard } from './drawing.js';
import { LAYOUT_KIND, layoutGroup } from './drawing.js';
import { newId } from './ids.js';
import { drawnPositions } from './layout.js';
import type {
	Edge,
	Exit,
	Graph,
	GraphChange,
	Group,
	Id,
	Ingress,
	Pod,
	RelayKind,
	Route,
	Server
} from './model.js';
import { edgeTargetExit, edgeTargetPod, emptyChange, isRelay } from './model.js';
import {
	appendMember,
	at,
	children,
	leaves,
	mapLeaves,
	type Path,
	removeEdges as removeRouteEdges,
	reordered,
	replaceAt,
	withPolicy
} from './route.js';

export type EditErrorCode =
	| 'pod_not_found'
	| 'exit_not_found'
	| 'server_not_found'
	| 'splitter_not_found'
	| 'target_not_dialable'
	| 'self_dial'
	| 'client_pod_dialed';

export class EditError extends Error {
	constructor(readonly code: EditErrorCode) {
		super(code);
	}
}

/** Where a new way on leads. */
export type Target =
	| { pod: Id }
	| { exit: Id }
	/** A new relay pod on this server, listening as `ingress`. */
	| { server: Id; ingress: RelayKind; canvasId: Id };

/**
 * A working copy of the graph that remembers what changed. Rows are replaced,
 * never mutated, so the graph the dashboard holds stays as it was read.
 */
class Draft<S extends Server> {
	readonly pods: Map<Id, Pod>;
	readonly exits: Map<Id, Exit>;
	readonly edges: Map<Id, Edge>;
	readonly groups: Map<Id, Group>;
	private readonly putPods = new Set<Id>();
	private readonly putExits = new Set<Id>();
	private readonly putEdges = new Set<Id>();
	private readonly putGroups = new Set<Id>();
	private readonly deletedPods = new Set<Id>();
	private readonly deletedExits = new Set<Id>();
	private readonly deletedEdges = new Set<Id>();

	constructor(readonly graph: Graph<S>) {
		this.pods = new Map(graph.pods.map(pod => [pod.id, pod]));
		this.exits = new Map(graph.exits.map(exit => [exit.id, exit]));
		this.edges = new Map(graph.edges.map(edge => [edge.id, edge]));
		this.groups = new Map(graph.groups.map(group => [group.id, group]));
	}

	pod(id: Id): Pod {
		const pod = this.pods.get(id);
		if (!pod) throw new EditError('pod_not_found');
		return pod;
	}

	putPod(pod: Pod) {
		this.pods.set(pod.id, pod);
		this.putPods.add(pod.id);
		this.deletedPods.delete(pod.id);
	}

	putExit(exit: Exit) {
		this.exits.set(exit.id, exit);
		this.putExits.add(exit.id);
	}

	putEdge(edge: Edge) {
		this.edges.set(edge.id, edge);
		this.putEdges.add(edge.id);
	}

	putGroup(group: Group) {
		this.groups.set(group.id, group);
		this.putGroups.add(group.id);
	}

	deletePod(id: Id) {
		this.pods.delete(id);
		this.putPods.delete(id);
		this.deletedPods.add(id);
	}

	deleteExit(id: Id) {
		this.exits.delete(id);
		this.putExits.delete(id);
		this.deletedExits.add(id);
	}

	deleteEdge(id: Id) {
		this.edges.delete(id);
		this.putEdges.delete(id);
		this.deletedEdges.add(id);
	}

	outOf(pod: Id): Edge[] {
		return [...this.edges.values()].filter(edge => edge.sourcePodId === pod);
	}

	into(target: Id): Edge[] {
		return [...this.edges.values()].filter(
			edge => edgeTargetPod(edge) === target || edgeTargetExit(edge) === target
		);
	}

	/** Rows that existed when the graph was read and are deleted now. */
	private existed(kind: 'pods' | 'exits' | 'edges', id: Id): boolean {
		return this.graph[kind].some(row => row.id === id);
	}

	change(): GraphChange {
		const change = emptyChange();
		change.putPods = [...this.putPods].flatMap(id => this.pods.get(id) ?? []);
		change.putExits = [...this.putExits].flatMap(id => this.exits.get(id) ?? []);
		change.putEdges = [...this.putEdges].flatMap(id => this.edges.get(id) ?? []);
		change.putGroups = [...this.putGroups].flatMap(id => this.groups.get(id) ?? []);
		change.deletePodIds = [...this.deletedPods].filter(id => this.existed('pods', id));
		change.deleteExitIds = [...this.deletedExits].filter(id => this.existed('exits', id));
		change.deleteEdgeIds = [...this.deletedEdges].filter(id => this.existed('edges', id));
		return change;
	}

	/** A new edge from `source`, added to its route as a new member. */
	dial(source: Pod, target: Edge['target'], member?: (route: Route | null, leaf: Route) => Route) {
		const edge: Edge = {
			id: newId(),
			sourcePodId: source.id,
			target,
			overrideIp: null,
			overridePort: null
		};
		this.putEdge(edge);
		const current = this.pod(source.id);
		const leaf: Route = { edge: edge.id };
		this.putPod({ ...current, route: (member ?? appendMember)(current.route, leaf) });
		return edge;
	}

	/** A relay pod on a server, listening as `ingress` on a port the control plane picks. */
	relayPod(serverId: Id, canvasId: Id, ingress: Ingress, name: string): Pod {
		const pod: Pod = {
			id: newId(),
			canvasId,
			serverId,
			name,
			comment: '',
			port: 0,
			bindIp: null,
			advertiseIp: null,
			ingress,
			route: null
		};
		this.putPod(pod);
		return pod;
	}

	/**
	 * Removes edges from the graph and from their pods' routes. With `prune`, a
	 * relay pod left with no way in goes too, and the edges out of it with it.
	 */
	cut(edgeIds: Iterable<Id>, prune: boolean) {
		const doomed = new Set(edgeIds);
		const orphanCandidates = new Set<Id>();
		const bySource = new Map<Id, Set<Id>>();
		for (const id of doomed) {
			const edge = this.edges.get(id);
			if (!edge) continue;
			const set = bySource.get(edge.sourcePodId) ?? new Set<Id>();
			set.add(id);
			bySource.set(edge.sourcePodId, set);
			const target = edgeTargetPod(edge);
			if (target !== null) orphanCandidates.add(target);
			this.deleteEdge(id);
		}
		for (const [podId, set] of bySource) {
			const pod = this.pods.get(podId);
			if (!pod) continue;
			this.putPod({ ...pod, route: removeRouteEdges(pod.route, set) });
		}
		if (!prune) return;
		for (const podId of orphanCandidates) {
			const pod = this.pods.get(podId);
			if (!pod || !isRelay(pod.ingress) || this.into(podId).length > 0) continue;
			this.removePod(podId, true);
		}
	}

	/** Removes a pod with the edges into it (fixing their routes) and out of it. */
	removePod(podId: Id, prune: boolean) {
		if (!this.pods.has(podId)) return;
		this.cut(
			this.into(podId).map(edge => edge.id),
			false
		);
		const out = this.outOf(podId).map(edge => edge.id);
		this.deletePod(podId);
		this.cut(out, prune);
	}
}

// --- connecting --------------------------------------------------------------

/**
 * A new way on for `podId`: an edge to a relay pod or an exit, or to a new relay
 * pod on a server. The pod's route gains it as a new member — of the group at
 * `path` when one is named, else of the whole route.
 */
export function connect<S extends Server>(
	graph: Graph<S>,
	podId: Id,
	target: Target,
	path: Path = []
): GraphChange {
	const draft = new Draft(graph);
	const source = draft.pod(podId);
	draft.dial(source, resolveTarget(draft, source, target), (route, leaf) => {
		const group = path.length > 0 ? at(route, path) : null;
		if (!route || !group || 'edge' in group) return appendMember(route, leaf);
		return replaceAt(route, path, appendMember(group, leaf)) ?? leaf;
	});
	return draft.change();
}

function resolveTarget<S extends Server>(
	draft: Draft<S>,
	source: Pod,
	target: Target
): Edge['target'] {
	if ('exit' in target) {
		if (!draft.exits.has(target.exit)) throw new EditError('exit_not_found');
		return { exit: target.exit };
	}
	if ('pod' in target) {
		if (target.pod === source.id) throw new EditError('self_dial');
		const pod = draft.pods.get(target.pod);
		if (!pod) throw new EditError('pod_not_found');
		if (!isRelay(pod.ingress)) throw new EditError('client_pod_dialed');
		return { pod: pod.id };
	}
	if (!draft.graph.servers.some(server => server.id === target.server)) {
		throw new EditError('server_not_found');
	}
	const landing = draft.relayPod(
		target.server,
		target.canvasId,
		{ kind: target.ingress },
		source.name
	);
	return { pod: landing.id };
}

function splitter<S extends Server>(drawing: Drawing<S>, id: string): SplitterCard {
	const card = drawing.cards.find(card => card.id === id);
	if (card?.kind !== 'splitter' || card.members.length === 0) {
		throw new EditError('splitter_not_found');
	}
	return card;
}

/**
 * A copy of `template` (a route node of `owner`) for `pod`: every leaf that goes
 * to an exit goes there from `pod` too, and every leaf that goes to a relay pod
 * lands on a new relay pod of the same server and ingress, which goes on where
 * the template's did.
 */
function cloneFor<S extends Server>(draft: Draft<S>, pod: Pod, template: Route): Route {
	return mapLeaves(template, edgeId => {
		const edge = draft.edges.get(edgeId);
		if (!edge) return { edge: edgeId };
		const created = cloneEdge(draft, pod, edge);
		return { edge: created.id };
	});
}

function cloneEdge<S extends Server>(draft: Draft<S>, pod: Pod, template: Edge): Edge {
	const exit = edgeTargetExit(template);
	let target: Edge['target'];
	if (exit !== null) {
		target = { exit };
	} else {
		const far = draft.pods.get(edgeTargetPod(template) ?? '');
		if (!far) throw new EditError('pod_not_found');
		const landing = draft.relayPod(far.serverId, far.canvasId, far.ingress, pod.name);
		if (far.route) {
			const route = mapLeaves(far.route, onward => {
				const next = draft.edges.get(onward);
				if (!next) return { edge: onward };
				const copy: Edge = { ...next, id: newId(), sourcePodId: landing.id };
				draft.putEdge(copy);
				return { edge: copy.id };
			});
			draft.putPod({ ...draft.pod(landing.id), route });
		}
		target = { pod: landing.id };
	}
	const edge: Edge = {
		id: newId(),
		sourcePodId: pod.id,
		target,
		overrideIp: template.overrideIp,
		overridePort: template.overridePort
	};
	draft.putEdge(edge);
	return edge;
}

/**
 * `podId` takes the way a splitter goes: its route gains a copy of the
 * splitter's subtree, with relay pods of its own where the splitter's members
 * land on relay pods.
 */
export function joinSplitter<S extends Server>(
	graph: Graph<S>,
	drawing: Drawing<S>,
	podId: Id,
	splitterId: string
): GraphChange {
	const draft = new Draft(graph);
	const card = splitter(drawing, splitterId);
	const source = draft.pod(podId);
	const [first] = card.members;
	if (!first) throw new EditError('splitter_not_found');
	const template = at(draft.pod(first.podId).route, first.path);
	if (!template) throw new EditError('splitter_not_found');
	const subtree = cloneFor(draft, source, template);
	const current = draft.pod(podId);
	draft.putPod({ ...current, route: appendMember(current.route, subtree) });
	return draft.change();
}

/**
 * Every route node a splitter stands for gains a member: an edge to a relay pod
 * or an exit, or a new relay pod per member pod on a server.
 */
export function addSplitterMember<S extends Server>(
	graph: Graph<S>,
	drawing: Drawing<S>,
	splitterId: string,
	target: Target
): GraphChange {
	const draft = new Draft(graph);
	const card = splitter(drawing, splitterId);
	for (const { podId, path } of card.members) {
		const pod = draft.pod(podId);
		const edgeTarget = resolveTarget(draft, pod, target);
		const edge: Edge = {
			id: newId(),
			sourcePodId: pod.id,
			target: edgeTarget,
			overrideIp: null,
			overridePort: null
		};
		draft.putEdge(edge);
		const current = draft.pod(podId);
		if (!current.route) continue;
		const node = at(current.route, path);
		if (!node) continue;
		draft.putPod({
			...current,
			route: replaceAt(current.route, path, appendMember(node, { edge: edge.id }))
		});
	}
	return draft.change();
}

// --- changing a splitter -----------------------------------------------------

export type SplitterPolicy = {
	kind: 'balance' | 'failover';
	sticky: boolean;
	/** One per member, in the new order. */
	weights: number[];
	/** The old index of each member, in the new order. */
	order: number[];
};

/** Rewrites every route node a splitter stands for. */
export function setSplitterPolicy<S extends Server>(
	graph: Graph<S>,
	drawing: Drawing<S>,
	splitterId: string,
	policy: SplitterPolicy
): GraphChange {
	const draft = new Draft(graph);
	const card = splitter(drawing, splitterId);
	// Deepest first: a pod whose route holds this splitter twice keeps valid
	// paths for the shallower one.
	const members = [...card.members].sort((a, b) => b.path.length - a.path.length);
	for (const { podId, path } of members) {
		const pod = draft.pod(podId);
		if (!pod.route) continue;
		const node = at(pod.route, path);
		if (!node || children(node).length !== policy.order.length) continue;
		const next = withPolicy(reordered(node, policy.order), {
			kind: policy.kind,
			sticky: policy.sticky,
			weights: policy.weights
		});
		draft.putPod({ ...pod, route: replaceAt(pod.route, path, next) });
	}
	return draft.change();
}

// --- removing ----------------------------------------------------------------

/** Edges gone from the graph and from their routes; see [`Draft.cut`] for `prune`. */
export function cutEdges<S extends Server>(
	graph: Graph<S>,
	edgeIds: Iterable<Id>,
	prune: boolean
): GraphChange {
	const draft = new Draft(graph);
	draft.cut(edgeIds, prune);
	return draft.change();
}

/** Every route node a splitter stands for is gone, with the edges under it. */
export function removeSplitter<S extends Server>(
	graph: Graph<S>,
	drawing: Drawing<S>,
	splitterId: string,
	prune: boolean
): GraphChange {
	const draft = new Draft(graph);
	const card = splitter(drawing, splitterId);
	const doomed = new Set<Id>();
	for (const { podId, path } of card.members) {
		for (const edge of leaves(at(draft.pod(podId).route, path as Path))) doomed.add(edge);
	}
	draft.cut(doomed, prune);
	return draft.change();
}

/** Pods gone, with every edge into and out of them. */
export function removePods<S extends Server>(
	graph: Graph<S>,
	podIds: Iterable<Id>,
	prune: boolean
): GraphChange {
	const draft = new Draft(graph);
	for (const id of podIds) draft.removePod(id, prune);
	return draft.change();
}

/** Exits gone, with every edge into them. */
export function removeExits<S extends Server>(graph: Graph<S>, exitIds: Iterable<Id>): GraphChange {
	const draft = new Draft(graph);
	for (const id of exitIds) {
		draft.cut(
			draft.into(id).map(edge => edge.id),
			false
		);
		draft.deleteExit(id);
	}
	return draft.change();
}

/** What a delete gesture on the drawing takes away. */
export type Removal = {
	/** Pods, with every edge into and out of them. */
	podIds?: Id[];
	/** Exits, with every edge into them. */
	exitIds?: Id[];
	/** Splitters, with every edge under the route nodes they stand for. */
	splitterIds?: string[];
	/** Edges, out of their routes. */
	edgeIds?: Id[];
};

/**
 * Everything a selection takes away, as one batch; see [`Draft.cut`] for
 * `prune`. Splitters are resolved against the routes as they were before any
 * of the rest was cut, so a selection holding both a splitter and a bus into it
 * means what it says.
 */
export function removeAll<S extends Server>(
	graph: Graph<S>,
	drawing: Drawing<S>,
	removal: Removal,
	prune: boolean
): GraphChange {
	const draft = new Draft(graph);
	const doomed = new Set<Id>(removal.edgeIds ?? []);
	for (const id of removal.splitterIds ?? []) {
		const card = drawing.cards.find(entry => entry.id === id);
		if (card?.kind !== 'splitter') continue;
		for (const { podId, path } of card.members) {
			const pod = draft.pods.get(podId);
			for (const edge of leaves(at(pod?.route ?? null, path))) doomed.add(edge);
		}
	}
	draft.cut(doomed, prune);
	for (const id of removal.podIds ?? []) draft.removePod(id, prune);
	for (const id of removal.exitIds ?? []) {
		if (!draft.exits.has(id)) continue;
		draft.cut(
			draft.into(id).map(edge => edge.id),
			prune
		);
		draft.deleteExit(id);
	}
	return draft.change();
}

// --- rows --------------------------------------------------------------------

export function putPod<S extends Server>(graph: Graph<S>, pod: Pod): GraphChange {
	const draft = new Draft(graph);
	const before = draft.pods.get(pod.id);
	if (before && isRelay(before.ingress) && !isRelay(pod.ingress) && draft.into(pod.id).length > 0) {
		throw new EditError('client_pod_dialed');
	}
	draft.putPod(pod);
	return draft.change();
}

export function putExit<S extends Server>(graph: Graph<S>, exit: Exit): GraphChange {
	const draft = new Draft(graph);
	draft.putExit(exit);
	return draft.change();
}

export function putEdge<S extends Server>(graph: Graph<S>, edge: Edge): GraphChange {
	const draft = new Draft(graph);
	draft.putEdge(edge);
	return draft.change();
}

/** A new pod with no route, on `serverId`, drawn on `canvasId`. */
export function newPod(canvasId: Id, serverId: Id, name: string, ingress: Ingress): Pod {
	return {
		id: newId(),
		canvasId,
		serverId,
		name,
		comment: '',
		port: 0,
		bindIp: null,
		advertiseIp: null,
		ingress,
		route: null
	};
}

export function newExit(canvasId: Id, name: string, destination: string, at: Point): Exit {
	return {
		id: newId(),
		canvasId,
		name,
		comment: '',
		destination,
		sendProxyProtocol: null,
		x: Math.round(at.x),
		y: Math.round(at.y)
	};
}

// --- positions ---------------------------------------------------------------

export type Moves = {
	servers: { id: Id; position: Point }[];
	exits: { id: Id; position: Point }[];
	canvases: { id: Id; position: Point }[];
	/** The layout group to put when cards only the drawing has moved. */
	layout: GraphChange | null;
};

/**
 * Where dragged cards end up: rows for servers, exits and subcanvases of this
 * canvas, the layout group for the rest (splitters, aggregators, portals, and
 * servers of other canvases drawn here for pods of this one).
 */
export function moveCards<S extends Server>(
	graph: Graph<S>,
	canvasId: Id,
	moved: { node: string; position: Point }[]
): Moves {
	const out: Moves = { servers: [], exits: [], canvases: [], layout: null };
	const existing = layoutGroup(graph, canvasId);
	const positions = drawnPositions(graph, canvasId);
	let layoutMoved = false;
	for (const { node, position } of moved) {
		const rounded = { x: Math.round(position.x), y: Math.round(position.y) };
		const [kind, ...rest] = node.split(':');
		const id = rest.join(':');
		const home = kind === 'server' ? graph.servers.find(s => s.id === id)?.canvasId : canvasId;
		if (kind === 'server' && home === canvasId) out.servers.push({ id, position: rounded });
		else if (kind === 'exit') out.exits.push({ id, position: rounded });
		else if (kind === 'canvas') out.canvases.push({ id, position: rounded });
		else {
			positions.set(node, rounded);
			layoutMoved = true;
		}
	}
	if (layoutMoved) {
		const group: Group = {
			id: existing?.id ?? newId(),
			canvasId,
			kind: LAYOUT_KIND,
			name: '',
			props: { ...(existing?.props ?? {}), positions: Object.fromEntries(positions) },
			members: []
		};
		const change = emptyChange();
		change.putGroups = [group];
		out.layout = change;
	}
	return out;
}
