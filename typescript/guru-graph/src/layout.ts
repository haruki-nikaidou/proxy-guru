/**
 * What the dashboard needs around a change besides the change itself: the graph
 * it leads to, positions that survive it, and where on a drawing something of
 * the graph is.
 */

import {
	type Card,
	canvasNode,
	type Drawing,
	draw,
	exitNode,
	LAYOUT_KIND,
	layoutGroup,
	type Point,
	positionsOf,
	serverNode
} from './drawing.js';
import { newId } from './ids.js';
import type { Graph, GraphChange, Group, Id, Server } from './model.js';
import { edgeTargetExit, edgeTargetPod } from './model.js';

/** The graph after `change`, applied the way the control plane applies it. */
export function applyChange<S extends Server>(graph: Graph<S>, change: GraphChange): Graph<S> {
	const upsert = <T extends { id: Id }>(rows: T[], puts: T[], deletes: Id[]): T[] => {
		const byId = new Map(rows.map(row => [row.id, row]));
		for (const id of deletes) byId.delete(id);
		for (const row of puts) byId.set(row.id, row);
		return [...byId.values()];
	};
	return {
		...graph,
		pods: upsert(graph.pods, change.putPods, change.deletePodIds),
		exits: upsert(graph.exits, change.putExits, change.deleteExitIds),
		edges: upsert(graph.edges, change.putEdges, change.deleteEdgeIds),
		groups: upsert(graph.groups, change.putGroups, change.deleteGroupIds)
	};
}

/** Cards whose position only the layout group holds. */
const drawnOnly = (card: Card): boolean =>
	card.kind === 'splitter' ||
	card.kind === 'aggregator' ||
	card.kind === 'portal' ||
	(card.kind === 'server' && card.ghost);

/** How much of `before` a card of the new drawing continues; 0 is nothing. */
function overlap(before: Card, after: Card): number {
	if (before.kind === 'splitter' && after.kind === 'splitter') {
		const members = new Set(before.members.map(m => `${m.podId}@${m.path.join('.')}`));
		return after.members.filter(m => members.has(`${m.podId}@${m.path.join('.')}`)).length;
	}
	if (before.kind === 'aggregator' && after.kind === 'aggregator') {
		const sources = new Set(before.sources);
		const targets = new Set(before.targets);
		return (
			after.sources.filter(s => sources.has(s)).length +
			after.targets.filter(t => targets.has(t)).length
		);
	}
	return 0;
}

/**
 * `change`, plus whatever keeps the drawing of `canvasId` where it was. Every
 * card only the drawing has is pinned where it was drawn before the change —
 * placed by hand, by an older drawing or afresh — so an edit never rearranges
 * cards it did not touch. A splitter or an aggregator is named by its shape, so
 * an edit that gives one a member gives it a new name too: the card that
 * continues one that is gone takes its place. Positions of cards no longer drawn
 * are dropped from the layout group.
 */
export function carryLayout<S extends Server>(
	graph: Graph<S>,
	canvasId: Id,
	change: GraphChange
): GraphChange {
	const before = draw(graph, canvasId);
	const afterGraph = applyChange(graph, change);
	const after = draw(afterGraph, canvasId);
	const group = layoutGroup(afterGraph, canvasId);
	const stored = positionsOf(group);
	const positions = new Map(stored);

	const beforeCards = new Map(before.cards.map(card => [card.id, card]));
	const afterIds = new Set(after.cards.map(card => card.id));
	const gone = before.cards.filter(card => drawnOnly(card) && !afterIds.has(card.id));
	const claimed = new Set<string>();
	for (const card of after.cards) {
		if (!drawnOnly(card) || stored.has(card.id)) continue;
		const same = beforeCards.get(card.id);
		if (same) {
			positions.set(card.id, same.position);
			continue;
		}
		// A card nothing continues is left to be placed afresh, clear of the rest.
		let best: { card: Card; score: number } | null = null;
		for (const candidate of gone) {
			if (claimed.has(candidate.id)) continue;
			const score = overlap(candidate, card);
			if (score > 0 && (!best || score > best.score)) best = { card: candidate, score };
		}
		if (!best) continue;
		claimed.add(best.card.id);
		positions.set(card.id, best.card.position);
	}
	for (const id of [...positions.keys()]) if (!afterIds.has(id)) positions.delete(id);

	if (samePositions(stored, positions)) return change;
	const put: Group = {
		id: group?.id ?? newId(),
		canvasId,
		kind: LAYOUT_KIND,
		name: '',
		props: { ...(group?.props ?? {}), positions: Object.fromEntries(positions) },
		members: []
	};
	return {
		...change,
		putGroups: [...change.putGroups.filter(g => g.id !== put.id), put]
	};
}

function samePositions(a: Map<string, Point>, b: Map<string, Point>): boolean {
	if (a.size !== b.size) return false;
	for (const [id, point] of a) {
		const other = b.get(id);
		if (!other || other.x !== point.x || other.y !== point.y) return false;
	}
	return true;
}

/** The canvases of the subtree `canvasId` roots, itself included. */
export function subtreeOf(graph: Graph, canvasId: Id): Set<Id> {
	const out = new Set<Id>([canvasId]);
	let grew = true;
	while (grew) {
		grew = false;
		for (const canvas of graph.canvases) {
			if (canvas.parentId !== null && out.has(canvas.parentId) && !out.has(canvas.id)) {
				out.add(canvas.id);
				grew = true;
			}
		}
	}
	return out;
}

/** Something of the graph a diagnostic or a search names. */
export type Subject =
	| { server: Id }
	| { pod: Id }
	| { exit: Id }
	| { edge: Id }
	| { group: Id }
	| { canvas: Id };

/**
 * Where a subject is on a drawing: the card that draws it (a pod is drawn on its
 * server's card), the bus that carries an edge, or else the canvas it lives on,
 * for the dashboard to open.
 */
export type Location =
	| { kind: 'card'; node: string }
	| { kind: 'bus'; bus: string }
	| { kind: 'canvas'; canvasId: Id }
	| { kind: 'nowhere' };

export function locate<S extends Server>(
	graph: Graph<S>,
	drawing: Drawing<S>,
	subject: Subject
): Location {
	const here = drawing.canvasId;
	const cards = new Set(drawing.cards.map(card => card.id));
	const onCanvas = (canvasId: Id | undefined, node: string): Location => {
		if (canvasId === undefined) return { kind: 'nowhere' };
		if (canvasId === here && cards.has(node)) return { kind: 'card', node };
		// A canvas nested in this one is drawn as a card of its own.
		let current = graph.canvases.find(canvas => canvas.id === canvasId);
		for (let depth = 0; current && depth < 64; depth += 1) {
			if (current.parentId === here && cards.has(canvasNode(current.id))) {
				return { kind: 'card', node: canvasNode(current.id) };
			}
			const parent = current.parentId;
			current = parent === null ? undefined : graph.canvases.find(canvas => canvas.id === parent);
		}
		return { kind: 'canvas', canvasId };
	};
	if ('server' in subject) {
		const server = graph.servers.find(s => s.id === subject.server);
		if (server && cards.has(serverNode(server.id))) {
			return { kind: 'card', node: serverNode(server.id) };
		}
		return onCanvas(server?.canvasId, serverNode(subject.server));
	}
	if ('pod' in subject) {
		const pod = graph.pods.find(p => p.id === subject.pod);
		return onCanvas(pod?.canvasId, serverNode(pod?.serverId ?? ''));
	}
	if ('exit' in subject) {
		const exit = graph.exits.find(e => e.id === subject.exit);
		return onCanvas(exit?.canvasId, exitNode(subject.exit));
	}
	if ('edge' in subject) {
		const bus = drawing.buses.find(b => b.edges.includes(subject.edge));
		if (bus) return { kind: 'bus', bus: bus.id };
		const edge = graph.edges.find(e => e.id === subject.edge);
		const source = graph.pods.find(p => p.id === edge?.sourcePodId);
		if (source) return onCanvas(source.canvasId, serverNode(source.serverId));
		const targetPod = edge ? edgeTargetPod(edge) : null;
		if (targetPod !== null) return locate(graph, drawing, { pod: targetPod });
		const targetExit = edge ? edgeTargetExit(edge) : null;
		if (targetExit !== null) return locate(graph, drawing, { exit: targetExit });
		return { kind: 'nowhere' };
	}
	if ('group' in subject) {
		const group = graph.groups.find(g => g.id === subject.group);
		return group ? { kind: 'canvas', canvasId: group.canvasId } : { kind: 'nowhere' };
	}
	if (subject.canvas === here) return { kind: 'canvas', canvasId: here };
	return onCanvas(subject.canvas, canvasNode(subject.canvas));
}

/**
 * The positions of every card only the drawing has, as drawn now, under what the
 * layout group already holds: what a first layout group starts from, so writing
 * one never moves a card that an older drawing or a fresh placement put down.
 */
export function drawnPositions<S extends Server>(
	graph: Graph<S>,
	canvasId: Id
): Map<string, Point> {
	const positions = positionsOf(layoutGroup(graph, canvasId));
	for (const card of draw(graph, canvasId).cards) {
		if (drawnOnly(card) && !positions.has(card.id)) positions.set(card.id, card.position);
	}
	return positions;
}
