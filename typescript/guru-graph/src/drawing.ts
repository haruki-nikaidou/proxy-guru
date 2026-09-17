/**
 * What one canvas of a tree draws, computed from the graph alone.
 *
 * - Servers are cards holding the pods drawn on the canvas; exits and child
 *   canvases are cards of their own, and a canvas outside the subtree an edge
 *   crosses into is a portal.
 * - A route's groups are splitters. Groups that balance or fail over the same
 *   way between the same *cards* are one splitter however many pods they belong
 *   to: two rules fanned out over the same five servers are one splitter, not
 *   two.
 * - Edges travel in buses between handles: a line leaves the pod whose route
 *   it belongs to and lands on the relay pod it dials, so a pod's own dot is
 *   where its traffic is drawn. A card stands in only where no row can: an exit,
 *   a subcanvas or portal, and a splitter's or aggregator's own handles. A bus
 *   carries every edge and rule that takes the same way.
 * - Where buses from several places meet in front of a splitter or an exit, an
 *   aggregator gathers them.
 *
 * Nothing here is stored: positions of what only the drawing has (splitters,
 * aggregators, portals) live in the canvas's `layout` group.
 */

import { hashKey } from './ids.js';
import type { Canvas, Edge, Exit, Graph, Group, Id, Pod, Route, Server } from './model.js';
import { edgeTargetExit, edgeTargetPod, isRelay } from './model.js';
import { children, kindOf, leaves, signature, weightsOf } from './route.js';
import { type RuleIndex, ruleIndex } from './rules.js';

export type Point = { x: number; y: number };

/** One end of a bus: a node of the drawing and a handle on it (a pod's row, or the card's own). */
export type Handle = { node: string; handle: string };

export type DrawnPod = {
	pod: Pod;
	rules: Id[];
	/** Out-edges. */
	dials: number;
	/** In-edges. */
	dialedBy: number;
};

export type ServerCard<S extends Server = Server> = {
	kind: 'server';
	id: string;
	server: S;
	/** A server of another canvas, drawn here because pods of this canvas run on it. */
	ghost: boolean;
	pods: DrawnPod[];
	rules: Id[];
	position: Point;
};

export type ExitCard = {
	kind: 'exit';
	id: string;
	exit: Exit;
	rules: Id[];
	dialedBy: number;
	position: Point;
};

export type SubcanvasCard = { kind: 'canvas'; id: string; canvas: Canvas; position: Point };

/** Everything of one outside canvas that edges of this one reach or come from. */
export type PortalCard = {
	kind: 'portal';
	id: string;
	canvas: Canvas | null;
	pods: Pod[];
	exits: Exit[];
	position: Point;
};

/** One route node a splitter stands for: the pod and where in its route. */
export type SplitterMember = { podId: Id; path: number[] };

export type SplitterCard = {
	kind: 'splitter';
	id: string;
	policy: 'balance' | 'failover';
	sticky: boolean;
	weights: number[];
	/** How many members (tiers) each route node has. */
	arity: number;
	members: SplitterMember[];
	rules: Id[];
	position: Point;
};

export type AggregatorCard = {
	kind: 'aggregator';
	id: string;
	/** The nodes whose buses it gathers. */
	sources: string[];
	/** The nodes it hands them on to. */
	targets: string[];
	rules: Id[];
	position: Point;
};

export type Card<S extends Server = Server> =
	| ServerCard<S>
	| ExitCard
	| SubcanvasCard
	| PortalCard
	| SplitterCard
	| AggregatorCard;

export type Bus = {
	id: string;
	source: Handle;
	target: Handle;
	edges: Id[];
	/** The pods whose routes the edges belong to. */
	pods: Id[];
	rules: Id[];
};

export type Drawing<S extends Server = Server> = {
	canvasId: Id;
	cards: Card<S>[];
	buses: Bus[];
	rules: RuleIndex;
};

export const serverNode = (id: Id) => `server:${id}`;
export const exitNode = (id: Id) => `exit:${id}`;
export const canvasNode = (id: Id) => `canvas:${id}`;
export const portalNode = (id: Id) => `portal:${id}`;
export const podInHandle = (id: Id) => `pod-in:${id}`;
export const podOutHandle = (id: Id) => `pod-out:${id}`;
export const splitterOutHandle = (index: number) => `out:${index}`;
export const aggregatorOutHandle = (target: string) => `out:${target}`;

/** Where the dashboard keeps what only the drawing has. */
export const LAYOUT_KIND = 'layout';

const SPLITTER_GAP = 320;
const CARD_WIDTH = 300;

type Place =
	| { kind: 'here' }
	| { kind: 'child'; node: string }
	| { kind: 'portal'; node: string; canvas: Id };

export function draw<S extends Server>(graph: Graph<S>, canvasId: Id): Drawing<S> {
	const rules = ruleIndex(graph);
	const canvases = new Map(graph.canvases.map(canvas => [canvas.id, canvas]));
	const servers = new Map(graph.servers.map(server => [server.id, server]));
	const pods = new Map(graph.pods.map(pod => [pod.id, pod]));
	const exits = new Map(graph.exits.map(exit => [exit.id, exit]));
	const edges = new Map(graph.edges.map(edge => [edge.id, edge]));
	const outCount = new Map<Id, number>();
	const inCount = new Map<Id, number>();
	for (const edge of graph.edges) {
		outCount.set(edge.sourcePodId, (outCount.get(edge.sourcePodId) ?? 0) + 1);
		const target = edgeTargetPod(edge) ?? edgeTargetExit(edge);
		if (target !== null) inCount.set(target, (inCount.get(target) ?? 0) + 1);
	}

	/** The child of this canvas whose subtree holds `id`, if any. */
	const placeOf = (id: Id): Place => {
		if (id === canvasId) return { kind: 'here' };
		let current = canvases.get(id);
		for (let depth = 0; current && depth < 64; depth += 1) {
			if (current.parentId === canvasId) return { kind: 'child', node: canvasNode(current.id) };
			current = current.parentId === null ? undefined : canvases.get(current.parentId);
		}
		return { kind: 'portal', node: portalNode(id), canvas: id };
	};

	const remembered = positionsOf(layoutGroup(graph, canvasId));
	const cards = new Map<string, Card<S>>();
	const portal = (canvas: Id): PortalCard => {
		const node = portalNode(canvas);
		const existing = cards.get(node);
		if (existing?.kind === 'portal') return existing;
		const card: PortalCard = {
			kind: 'portal',
			id: node,
			canvas: canvases.get(canvas) ?? null,
			pods: [],
			exits: [],
			position: { x: 0, y: 0 }
		};
		cards.set(node, card);
		return card;
	};

	for (const server of graph.servers) {
		if (server.canvasId !== canvasId) continue;
		cards.set(serverNode(server.id), {
			kind: 'server',
			id: serverNode(server.id),
			server,
			ghost: false,
			pods: [],
			rules: [],
			position: { x: server.x, y: server.y }
		});
	}
	const podsHere = graph.pods.filter(pod => pod.canvasId === canvasId);
	for (const pod of podsHere) {
		const node = serverNode(pod.serverId);
		let card = cards.get(node);
		if (!card) {
			const server = servers.get(pod.serverId);
			if (!server) continue;
			card = {
				kind: 'server',
				id: node,
				server,
				ghost: true,
				pods: [],
				rules: [],
				// Its own position is where it sits on its own canvas.
				position: remembered.get(node) ?? { x: server.x, y: server.y }
			};
			cards.set(node, card);
		}
		if (card.kind !== 'server') continue;
		card.pods.push({
			pod,
			rules: rules.pods.get(pod.id) ?? [],
			dials: outCount.get(pod.id) ?? 0,
			dialedBy: inCount.get(pod.id) ?? 0
		});
	}
	for (const exit of graph.exits) {
		if (exit.canvasId !== canvasId) continue;
		cards.set(exitNode(exit.id), {
			kind: 'exit',
			id: exitNode(exit.id),
			exit,
			rules: rules.exits.get(exit.id) ?? [],
			dialedBy: inCount.get(exit.id) ?? 0,
			position: { x: exit.x, y: exit.y }
		});
	}
	for (const canvas of graph.canvases) {
		if (canvas.parentId !== canvasId) continue;
		cards.set(canvasNode(canvas.id), {
			kind: 'canvas',
			id: canvasNode(canvas.id),
			canvas,
			position: { x: canvas.x, y: canvas.y }
		});
	}

	/**
	 * The handle traffic into this pod or exit arrives at: the pod's own row when
	 * it is drawn here, else the card that stands for where it is. A client pod
	 * has no row handle to land on — dialing one is refused by the check — so an
	 * edge into one lands on its server's card.
	 */
	const intoHandle = (edge: Edge): Handle | null => {
		const podId = edgeTargetPod(edge);
		if (podId !== null) {
			const pod = pods.get(podId);
			if (!pod) return null;
			const place = placeOf(pod.canvasId);
			if (place.kind === 'here') {
				return {
					node: serverNode(pod.serverId),
					handle: dialable(pod) ? podInHandle(pod.id) : 'in'
				};
			}
			if (place.kind === 'child') return { node: place.node, handle: 'in' };
			const card = portal(place.canvas);
			if (!card.pods.some(p => p.id === pod.id)) card.pods.push(pod);
			return { node: place.node, handle: 'in' };
		}
		const exitId = edgeTargetExit(edge);
		const exit = exitId === null ? undefined : exits.get(exitId);
		if (!exit) return null;
		const place = placeOf(exit.canvasId);
		if (place.kind === 'here') return { node: exitNode(exit.id), handle: 'in' };
		if (place.kind === 'child') return { node: place.node, handle: 'in' };
		const card = portal(place.canvas);
		if (!card.exits.some(e => e.id === exit.id)) card.exits.push(exit);
		return { node: place.node, handle: 'in' };
	};

	const buses = new Map<string, Bus>();
	const carry = (source: Handle, target: Handle, edgeIds: Id[], podId: Id) => {
		const id = `${source.node}#${source.handle}>${target.node}#${target.handle}`;
		let bus = buses.get(id);
		if (!bus) {
			bus = { id, source, target, edges: [], pods: [], rules: [] };
			buses.set(id, bus);
		}
		for (const edge of edgeIds) if (!bus.edges.includes(edge)) bus.edges.push(edge);
		if (!bus.pods.includes(podId)) bus.pods.push(podId);
	};

	const leafKey = (edgeId: Id): string => {
		const edge = edges.get(edgeId);
		const handle = edge ? intoHandle(edge) : null;
		return handle ? handle.node : `missing:${edgeId}`;
	};

	const splitterOrder: string[] = [];
	const walk = (pod: Pod, route: Route, path: number[], from: Handle) => {
		if ('edge' in route) {
			const edge = edges.get(route.edge);
			const target = edge ? intoHandle(edge) : null;
			if (target) carry(from, target, [route.edge], pod.id);
			return;
		}
		const id = `split:${hashKey(signature(route, leafKey))}`;
		let card = cards.get(id);
		if (!card) {
			card = {
				kind: 'splitter',
				id,
				policy: kindOf(route) === 'failover' ? 'failover' : 'balance',
				sticky: 'balance' in route && route.sticky === 'client_ip',
				weights: weightsOf(route),
				arity: children(route).length,
				members: [],
				rules: [],
				position: { x: 0, y: 0 }
			};
			cards.set(id, card);
			splitterOrder.push(id);
		}
		if (card.kind === 'splitter') card.members.push({ podId: pod.id, path });
		carry(from, { node: id, handle: 'in' }, leaves(route), pod.id);
		children(route).forEach((child, i) => {
			walk(pod, child, [...path, i], { node: id, handle: splitterOutHandle(i) });
		});
	};

	for (const pod of podsHere) {
		if (!pod.route) continue;
		walk(pod, pod.route, [], { node: serverNode(pod.serverId), handle: podOutHandle(pod.id) });
	}
	// Traffic arriving from pods drawn elsewhere: their routes are drawn where
	// they live, here only where they come from.
	for (const edge of graph.edges) {
		const source = pods.get(edge.sourcePodId);
		if (!source || source.canvasId === canvasId) continue;
		const targetPod = edgeTargetPod(edge);
		const targetExit = edgeTargetExit(edge);
		const targetCanvas =
			targetPod !== null
				? pods.get(targetPod)?.canvasId
				: targetExit !== null
					? exits.get(targetExit)?.canvasId
					: undefined;
		if (targetCanvas !== canvasId) continue;
		const target = intoHandle(edge);
		if (!target) continue;
		const place = placeOf(source.canvasId);
		if (place.kind === 'here') continue;
		if (place.kind === 'portal') {
			const card = portal(place.canvas);
			if (!card.pods.some(p => p.id === source.id)) card.pods.push(source);
		}
		carry({ node: place.node, handle: 'out' }, target, [edge.id], source.id);
	}

	gatherAggregators(cards, buses);

	for (const bus of buses.values()) {
		const set = new Set<Id>();
		for (const edge of bus.edges) for (const rule of rules.edges.get(edge) ?? []) set.add(rule);
		bus.rules = orderRules(rules, set);
	}
	for (const card of cards.values()) {
		if (card.kind === 'server') {
			const set = new Set<Id>();
			for (const pod of card.pods) for (const rule of pod.rules) set.add(rule);
			card.rules = orderRules(rules, set);
			card.pods.sort(
				(a, b) => a.pod.name.localeCompare(b.pod.name) || a.pod.id.localeCompare(b.pod.id)
			);
		} else if (card.kind === 'splitter' || card.kind === 'aggregator') {
			const set = new Set<Id>();
			for (const bus of buses.values()) {
				if (bus.target.node === card.id || bus.source.node === card.id) {
					for (const rule of bus.rules) set.add(rule);
				}
			}
			card.rules = orderRules(rules, set);
		}
	}

	placeDrawnCards(graph, canvasId, cards, buses, splitterOrder);

	return {
		canvasId,
		cards: [...cards.values()].sort((a, b) => a.id.localeCompare(b.id)),
		buses: [...buses.values()].sort((a, b) => a.id.localeCompare(b.id)),
		rules
	};
}

/**
 * The route nodes a handle stands for, where a drag may start that gives each
 * of them a way on:
 * - a splitter's member row, `out:<i>`: that member of every route node the
 *   splitter stands for;
 * - a splitter's `add` handle: those route nodes themselves;
 * - an aggregator's way out, `out:<card>`: the root of the route of every pod
 *   whose line runs through it. An aggregator only gathers lines that leave a
 *   pod's own row, and a pod's row is where its route starts.
 *
 * Anything else stands for nothing: an empty list, never an error, since a
 * drag asks while it hovers.
 */
export function routeNodesAt<S extends Server>(drawing: Drawing<S>, at: Handle): SplitterMember[] {
	const card = drawing.cards.find(entry => entry.id === at.node);
	if (card?.kind === 'splitter') {
		if (at.handle === 'add')
			return card.members.map(({ podId, path }) => ({ podId, path: [...path] }));
		const index = /^out:(\d+)$/.exec(at.handle)?.[1];
		if (index === undefined || Number(index) >= card.arity) return [];
		return card.members.map(({ podId, path }) => ({ podId, path: [...path, Number(index)] }));
	}
	if (card?.kind === 'aggregator') {
		const bus = drawing.buses.find(
			entry => entry.source.node === at.node && entry.source.handle === at.handle
		);
		return (bus?.pods ?? []).map(podId => ({ podId, path: [] }));
	}
	return [];
}

function orderRules(index: RuleIndex, set: Set<Id>): Id[] {
	return index.rules.filter(rule => set.has(rule));
}

/**
 * Buses from two or more servers into one splitter or exit meet in an
 * aggregator. Targets fed from the same servers share one: five servers handing
 * two rules on to two exits are one aggregator with two ways out.
 */
function gatherAggregators<S extends Server>(cards: Map<string, Card<S>>, buses: Map<string, Bus>) {
	const into = new Map<string, Bus[]>();
	for (const bus of buses.values()) {
		const kind = cards.get(bus.target.node)?.kind;
		if (kind !== 'splitter' && kind !== 'exit') continue;
		if (cards.get(bus.source.node)?.kind !== 'server') continue;
		const list = into.get(bus.target.node) ?? [];
		list.push(bus);
		into.set(bus.target.node, list);
	}
	const bySources = new Map<string, { sources: string[]; targets: string[] }>();
	for (const [target, list] of into) {
		const sources = [...new Set(list.map(bus => bus.source.node))].sort();
		if (sources.length < 2) continue;
		const key = sources.join('|');
		const entry = bySources.get(key) ?? { sources, targets: [] };
		entry.targets.push(target);
		bySources.set(key, entry);
	}
	for (const [key, { sources, targets }] of bySources) {
		const id = `agg:${hashKey(key)}`;
		targets.sort();
		cards.set(id, {
			kind: 'aggregator',
			id,
			sources,
			targets,
			rules: [],
			position: { x: 0, y: 0 }
		});
		for (const target of targets) {
			for (const bus of into.get(target) ?? []) {
				buses.delete(bus.id);
				const inward = mergeBus(buses, bus.source, { node: id, handle: 'in' });
				const outward = mergeBus(
					buses,
					{ node: id, handle: aggregatorOutHandle(target) },
					bus.target
				);
				for (const merged of [inward, outward]) {
					for (const edge of bus.edges) if (!merged.edges.includes(edge)) merged.edges.push(edge);
					for (const pod of bus.pods) if (!merged.pods.includes(pod)) merged.pods.push(pod);
				}
			}
		}
	}
}

function mergeBus(buses: Map<string, Bus>, source: Handle, target: Handle): Bus {
	const id = `${source.node}#${source.handle}>${target.node}#${target.handle}`;
	let bus = buses.get(id);
	if (!bus) {
		bus = { id, source, target, edges: [], pods: [], rules: [] };
		buses.set(id, bus);
	}
	return bus;
}

/**
 * Positions of the cards only the drawing has: what the canvas's layout group
 * remembers, else — on a canvas with no layout group yet — what an older drawing
 * left (the `splitter` and `aggregator` groups of a conversion), else a place
 * between where the traffic comes from and where it goes, clear of other cards.
 */
function placeDrawnCards<S extends Server>(
	graph: Graph<S>,
	canvasId: Id,
	cards: Map<string, Card<S>>,
	buses: Map<string, Bus>,
	splitterOrder: string[]
) {
	const layout = layoutGroup(graph, canvasId);
	const remembered = positionsOf(layout);
	// An older drawing only seeds a canvas the dashboard has not laid out yet:
	// once there is a layout group, it holds every card that was drawn.
	const legacy = layout
		? []
		: graph.groups.filter(
				group =>
					group.canvasId === canvasId && (group.kind === 'splitter' || group.kind === 'aggregator')
			);
	const placed = new Set<string>();
	const center = (node: string): Point | null => {
		const card = cards.get(node);
		if (!card) return null;
		return card.position;
	};
	const mean = (points: Point[]): Point | null =>
		points.length === 0
			? null
			: {
					x: points.reduce((sum, p) => sum + p.x, 0) / points.length,
					y: points.reduce((sum, p) => sum + p.y, 0) / points.length
				};

	const drawn = [
		...splitterOrder,
		...[...cards.values()].filter(card => card.kind === 'aggregator').map(card => card.id),
		...[...cards.values()].filter(card => card.kind === 'portal').map(card => card.id)
	];
	// An older drawing's group goes to the card it overlaps most, once: a
	// splitter that split in two leaves one of them where it was.
	const claims: { id: string; group: Group; overlap: number }[] = [];
	for (const id of drawn) {
		const card = cards.get(id);
		if (!card) continue;
		const pinned = remembered.get(id);
		if (pinned) {
			card.position = pinned;
			placed.add(id);
			continue;
		}
		const carried = new Set<Id>();
		for (const bus of buses.values()) {
			if (bus.target.node === id) for (const edge of bus.edges) carried.add(edge);
		}
		for (const group of legacy) {
			const overlap = legacyOverlap(group, card.kind, carried);
			if (overlap > 0) claims.push({ id, group, overlap });
		}
	}
	claims.sort((a, b) => b.overlap - a.overlap || a.id.localeCompare(b.id));
	const usedGroups = new Set<Id>();
	for (const { id, group } of claims) {
		const card = cards.get(id);
		const { x, y } = group.props;
		if (!card || placed.has(id) || usedGroups.has(group.id)) continue;
		if (typeof x !== 'number' || typeof y !== 'number') continue;
		card.position = { x, y };
		placed.add(id);
		usedGroups.add(group.id);
	}
	// Whatever is left goes right of what feeds it, level with what it feeds.
	for (const id of drawn) {
		if (placed.has(id)) continue;
		const card = cards.get(id);
		if (!card) continue;
		const sources: Point[] = [];
		const targets: Point[] = [];
		for (const bus of buses.values()) {
			if (bus.target.node === id) {
				const point = center(bus.source.node);
				if (point) sources.push(point);
			}
			if (bus.source.node === id) {
				const point = center(bus.target.node);
				if (point) targets.push(point);
			}
		}
		const from = mean(sources);
		const to = mean(targets);
		// A group nested in another is drawn under it; anything else right of
		// what feeds it, level with what it feeds.
		const parent = [...buses.values()]
			.filter(bus => bus.target.node === id)
			.map(bus => cards.get(bus.source.node))
			.find(source => source?.kind === 'splitter');
		const wanted = parent
			? {
					x: parent.position.x + NESTED_INDENT,
					y: parent.position.y + footprint(parent).height + CLEARANCE / 2
				}
			: {
					x: Math.round(
						from ? from.x + CARD_WIDTH + SPLITTER_GAP / 2 : to ? to.x - SPLITTER_GAP : 0
					),
					y: Math.round(to ? to.y : from ? from.y : 0)
				};
		// Clear of every card already in place, moving down.
		const others = [...cards.values()].filter(
			other => other.id !== id && (placed.has(other.id) || !drawn.includes(other.id))
		);
		for (let tries = 0; tries < 200; tries += 1) {
			const hit = others.some(other => overlaps(card, wanted, other));
			if (!hit) break;
			wanted.y += CLEARANCE;
		}
		card.position = wanted;
		placed.add(id);
	}
}

/** How far a card is moved at a time to clear another. */
const CLEARANCE = 40;
/** How far right of its parent a nested splitter is drawn. */
const NESTED_INDENT = 40;

/** Roughly how much room a card takes: enough to keep new cards off it. */
function footprint(card: Card): { width: number; height: number } {
	switch (card.kind) {
		case 'server':
			return { width: 320, height: 90 + 22 * card.pods.length };
		case 'exit':
			return { width: 260, height: 80 };
		case 'canvas':
			return { width: 260, height: 60 };
		case 'portal':
			return { width: 220, height: 50 + 16 * (card.pods.length + card.exits.length) };
		case 'splitter':
			return { width: 220, height: 70 + 20 * (card.arity + 1) };
		case 'aggregator':
			return { width: 200, height: 60 + 20 * card.targets.length };
	}
}

/**
 * Where a new card of `kind` can go near `near` without covering a card of the
 * drawing: `near` itself when it is clear, else the closest clear spot on rings
 * around it.
 */
export function clearSpot<S extends Server>(
	drawing: Drawing<S>,
	near: Point,
	kind: 'server' | 'exit' | 'canvas'
): Point {
	const size =
		kind === 'server'
			? { width: 320, height: 110 }
			: kind === 'exit'
				? { width: 260, height: 80 }
				: { width: 260, height: 60 };
	const clear = (at: Point) =>
		drawing.cards.every(other => {
			const b = footprint(other);
			return !(
				at.x < other.position.x + b.width + CLEARANCE / 2 &&
				other.position.x < at.x + size.width + CLEARANCE / 2 &&
				at.y < other.position.y + b.height + CLEARANCE / 2 &&
				other.position.y < at.y + size.height + CLEARANCE / 2
			);
		});
	const start = { x: Math.round(near.x), y: Math.round(near.y) };
	if (clear(start)) return start;
	for (let ring = 1; ring <= 40; ring += 1) {
		const reach = ring * CLEARANCE;
		for (let step = 0; step < 8 * ring; step += 1) {
			const angle = (step / (8 * ring)) * 2 * Math.PI;
			const at = {
				x: Math.round(start.x + reach * Math.cos(angle)),
				y: Math.round(start.y + reach * Math.sin(angle))
			};
			if (clear(at)) return at;
		}
	}
	return start;
}

function overlaps(card: Card, at: Point, other: Card): boolean {
	const a = footprint(card);
	const b = footprint(other);
	return (
		at.x < other.position.x + b.width &&
		other.position.x < at.x + a.width &&
		at.y < other.position.y + b.height &&
		other.position.y < at.y + a.height
	);
}

/** How many of the edges a card carries an older drawing's group of its kind held. */
function legacyOverlap(group: Group, kind: Card['kind'], carried: Set<Id>): number {
	if (group.kind !== kind || (kind !== 'splitter' && kind !== 'aggregator')) return 0;
	let overlap = 0;
	for (const member of group.members)
		if ('edge' in member && carried.has(member.edge)) overlap += 1;
	return overlap;
}

/** The canvas's layout group, if the dashboard has written one. */
export function layoutGroup(graph: Graph, canvasId: Id): Group | undefined {
	return graph.groups.find(group => group.canvasId === canvasId && group.kind === LAYOUT_KIND);
}

/** `props.positions` of a layout group, checked. */
export function positionsOf(group: Group | undefined): Map<string, Point> {
	const out = new Map<string, Point>();
	const positions = group?.props.positions;
	if (!positions || typeof positions !== 'object') return out;
	for (const [id, value] of Object.entries(positions as Record<string, unknown>)) {
		if (!value || typeof value !== 'object') continue;
		const { x, y } = value as Record<string, unknown>;
		if (typeof x === 'number' && typeof y === 'number') out.set(id, { x, y });
	}
	return out;
}

/** Whether traffic can be dialed into this pod at all. */
export const dialable = (pod: Pod): boolean => isRelay(pod.ingress);
