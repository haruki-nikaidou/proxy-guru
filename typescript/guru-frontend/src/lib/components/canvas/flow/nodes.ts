import type { Node } from '@xyflow/svelte';
import type { Canvas, Drawing, Exit, Handle, Id, Pod, SplitterCard } from 'guru-graph';
import { locate } from 'guru-graph';
import type { CanvasGraph, DiagnosticDto, ServerDto } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * The drawing of one canvas as the cards Svelte Flow renders. Everything a card
 * shows is computed here, so the components stay dumb and a refresh that
 * changes nothing a card shows leaves the card alone.
 */

export type ProblemLevel = 'none' | 'warning' | 'error';

export type DrawnPodData = {
	pod: Pod;
	rules: Id[];
	dials: number;
	dialedBy: number;
	problem: ProblemLevel;
};

/** One way out of a splitter or an aggregator: where its bus leads. */
export type WayOut = { handle: string; label: string; weight: number | null };

export type FlowNodeData =
	| {
			kind: 'server';
			server: ServerDto;
			ghost: boolean;
			pods: DrawnPodData[];
			rules: Id[];
			problem: ProblemLevel;
	  }
	| { kind: 'exit'; exit: Exit; rules: Id[]; dialedBy: number; problem: ProblemLevel }
	| {
			kind: 'canvas';
			canvas: Canvas;
			servers: number;
			pods: number;
			problem: ProblemLevel;
	  }
	| {
			kind: 'portal';
			canvas: Canvas | null;
			pods: { id: Id; name: string }[];
			exits: { id: Id; name: string }[];
			problem: ProblemLevel;
	  }
	| {
			kind: 'splitter';
			policy: SplitterCard['policy'];
			sticky: boolean;
			/** How many route nodes (one per pod) the card stands for. */
			stands: number;
			ways: WayOut[];
			rules: Id[];
			problem: ProblemLevel;
	  }
	| { kind: 'aggregator'; sources: number; ways: WayOut[]; rules: Id[]; problem: ProblemLevel };

export type FlowNode = Node<FlowNodeData>;

/**
 * What the side panel is showing, in the ids of the graph and the drawing. The
 * cards only the drawing has are named by their shape, so an edit can rename
 * one under the panel: `anchor` is what the panel finds it again by — a route
 * node a splitter stands for, a card an aggregator feeds, an edge a bus carries.
 */
export type PanelTarget =
	| { kind: 'server'; id: Id }
	| { kind: 'pod'; id: Id }
	| { kind: 'exit'; id: Id }
	| { kind: 'canvas'; id: Id }
	| { kind: 'portal'; id: string }
	| { kind: 'splitter'; id: string; anchor?: { podId: Id; path: number[] } }
	| { kind: 'aggregator'; id: string; anchor?: string }
	| { kind: 'bus'; id: string; anchor?: Id };

/** A panel target whose card was renamed by an edit, found again; `null` when it is gone. */
export function refind(drawing: Drawing<ServerDto>, target: PanelTarget): PanelTarget | null {
	switch (target.kind) {
		case 'splitter': {
			const anchor = target.anchor;
			if (!anchor) return null;
			const card = drawing.cards.find(
				entry =>
					entry.kind === 'splitter' &&
					entry.members.some(
						member =>
							member.podId === anchor.podId && member.path.join('.') === anchor.path.join('.')
					)
			);
			return card ? { ...target, id: card.id } : null;
		}
		case 'aggregator': {
			const card = drawing.cards.find(
				entry => entry.kind === 'aggregator' && entry.targets.includes(target.anchor ?? '')
			);
			return card ? { ...target, id: card.id } : null;
		}
		case 'bus': {
			const bus = drawing.buses.find(entry => entry.edges.includes(target.anchor ?? ''));
			return bus ? { ...target, id: bus.id } : null;
		}
		default:
			return null;
	}
}

const worse = (a: ProblemLevel, b: ProblemLevel): ProblemLevel =>
	a === 'error' || b === 'error'
		? 'error'
		: a === 'warning' || b === 'warning'
			? 'warning'
			: 'none';

/** The worst diagnostic level of every card, bus and pod of the drawing. */
export type ProblemIndex = {
	cards: Map<string, ProblemLevel>;
	buses: Map<string, ProblemLevel>;
	pods: Map<Id, ProblemLevel>;
};

export function problemIndex(graph: CanvasGraph, drawing: Drawing<ServerDto>): ProblemIndex {
	const index: ProblemIndex = { cards: new Map(), buses: new Map(), pods: new Map() };
	const raise = <K>(map: Map<K, ProblemLevel>, key: K, level: ProblemLevel) =>
		map.set(key, worse(map.get(key) ?? 'none', level));
	for (const diagnostic of graph.diagnostics) {
		const level: ProblemLevel = diagnostic.error ? 'error' : 'warning';
		for (const subject of diagnostic.subjects) {
			if ('pod' in subject) raise(index.pods, subject.pod, level);
			const where = locate(graph, drawing, subject);
			if (where.kind === 'card') raise(index.cards, where.node, level);
			else if (where.kind === 'bus') raise(index.buses, where.bus, level);
		}
	}
	return index;
}

/**
 * One end of a bus as the bus panel names it: a pod with its server, a
 * splitter's member, an aggregator's way out, or else the card itself.
 */
export function handleLabel(
	graph: CanvasGraph,
	drawing: Drawing<ServerDto>,
	handle: Handle
): string {
	for (const prefix of ['pod-out:', 'pod-in:']) {
		if (!handle.handle.startsWith(prefix)) continue;
		const podId = handle.handle.slice(prefix.length);
		const pod = graph.pods.find(entry => entry.id === podId);
		if (!pod) return podId;
		const server = graph.servers.find(entry => entry.id === pod.serverId);
		return server ? `${pod.name} · ${server.name}` : pod.name;
	}
	const card = cardLabel(graph, drawing, handle.node);
	if (handle.handle.startsWith('out:')) {
		const rest = handle.handle.slice('out:'.length);
		if (handle.node.startsWith('split:') && /^\d+$/.test(rest)) {
			return m.editor_splitter_member_of({ splitter: card, index: Number(rest) + 1 });
		}
		if (handle.node.startsWith('agg:')) {
			return m.editor_aggregator_way_to({
				aggregator: card,
				target: cardLabel(graph, drawing, rest)
			});
		}
	}
	return card;
}

/** How a card names what a bus leads into. */
export function cardLabel(graph: CanvasGraph, drawing: Drawing<ServerDto>, node: string): string {
	const card = drawing.cards.find(entry => entry.id === node);
	switch (card?.kind) {
		case 'server':
			return card.server.name;
		case 'exit':
			return card.exit.name;
		case 'canvas':
			return card.canvas.name;
		case 'portal':
			return card.canvas?.name ?? '';
		case 'splitter':
			return card.policy === 'failover' ? m.editor_policy_failover() : m.editor_policy_balance();
		case 'aggregator':
			return m.editor_kind_aggregator();
		default:
			return graph.canvases.find(canvas => `canvas:${canvas.id}` === node)?.name ?? node;
	}
}

export function buildFlowNodes(
	graph: CanvasGraph,
	drawing: Drawing<ServerDto>,
	problems: ProblemIndex
): FlowNode[] {
	const level = (id: string): ProblemLevel => problems.cards.get(id) ?? 'none';
	const wayOut = (node: string, handle: string) =>
		drawing.buses.find(bus => bus.source.node === node && bus.source.handle === handle);
	const positions = new Map(drawing.cards.map(card => [card.id, card.position]));
	const yOf = (node: string) => positions.get(node)?.y ?? 0;
	const nodes: FlowNode[] = [];
	for (const card of drawing.cards) {
		const base = { id: card.id, position: card.position };
		switch (card.kind) {
			case 'server':
				nodes.push({
					...base,
					type: 'server',
					deletable: !card.ghost,
					data: {
						kind: 'server',
						server: card.server,
						ghost: card.ghost,
						pods: card.pods.map(drawn => ({
							pod: drawn.pod,
							rules: drawn.rules,
							dials: drawn.dials,
							dialedBy: drawn.dialedBy,
							problem: problems.pods.get(drawn.pod.id) ?? 'none'
						})),
						rules: card.rules,
						problem: level(card.id)
					}
				});
				break;
			case 'exit':
				nodes.push({
					...base,
					type: 'exit',
					deletable: true,
					data: {
						kind: 'exit',
						exit: card.exit,
						rules: card.rules,
						dialedBy: card.dialedBy,
						problem: level(card.id)
					}
				});
				break;
			case 'canvas': {
				const inside = new Set([card.canvas.id]);
				for (let grew = true; grew; ) {
					grew = false;
					for (const canvas of graph.canvases) {
						if (canvas.parentId && inside.has(canvas.parentId) && !inside.has(canvas.id)) {
							inside.add(canvas.id);
							grew = true;
						}
					}
				}
				nodes.push({
					...base,
					type: 'canvas',
					deletable: true,
					data: {
						kind: 'canvas',
						canvas: card.canvas,
						servers: graph.servers.filter(server => inside.has(server.canvasId)).length,
						pods: graph.pods.filter(pod => inside.has(pod.canvasId)).length,
						problem: level(card.id)
					}
				});
				break;
			}
			case 'portal':
				nodes.push({
					...base,
					type: 'portal',
					deletable: false,
					data: {
						kind: 'portal',
						canvas: card.canvas,
						pods: card.pods.map(pod => ({ id: pod.id, name: pod.name })),
						exits: card.exits.map(exit => ({ id: exit.id, name: exit.name })),
						problem: level(card.id)
					}
				});
				break;
			case 'splitter':
				nodes.push({
					...base,
					type: 'splitter',
					deletable: true,
					data: {
						kind: 'splitter',
						policy: card.policy,
						sticky: card.sticky,
						stands: card.members.length,
						ways: card.weights.map((weight, i) => {
							const bus = wayOut(card.id, `out:${i}`);
							return {
								handle: `out:${i}`,
								label: bus ? cardLabel(graph, drawing, bus.target.node) : '',
								weight: card.policy === 'balance' ? weight : null
							};
						}),
						rules: card.rules,
						problem: level(card.id)
					}
				});
				break;
			case 'aggregator':
				nodes.push({
					...base,
					type: 'aggregator',
					deletable: false,
					data: {
						kind: 'aggregator',
						sources: card.sources.length,
						// Top to bottom as the targets are drawn, so the lines out do not cross.
						ways: [...card.targets]
							.sort((a, b) => yOf(a) - yOf(b))
							.map(target => ({
								handle: `out:${target}`,
								label: cardLabel(graph, drawing, target),
								weight: null
							})),
						rules: card.rules,
						problem: level(card.id)
					}
				});
				break;
		}
	}
	return nodes;
}

/** The card a panel target is drawn on, for the focus ring. */
export function focusedCard(graph: CanvasGraph, target: PanelTarget | null): string | null {
	if (!target) return null;
	switch (target.kind) {
		case 'server':
			return `server:${target.id}`;
		case 'pod': {
			const pod = graph.pods.find(entry => entry.id === target.id);
			return pod ? `server:${pod.serverId}` : null;
		}
		case 'exit':
			return `exit:${target.id}`;
		case 'canvas':
			return `canvas:${target.id}`;
		case 'bus':
			return null;
		default:
			return target.id;
	}
}

/** Every name visible on a canvas tree, so a fresh default never duplicates one. */
export function usedNames(graph: CanvasGraph | undefined): ReadonlySet<string> {
	const names = new Set<string>();
	if (!graph) return names;
	for (const server of graph.servers) names.add(server.name);
	for (const pod of graph.pods) names.add(pod.name);
	for (const exit of graph.exits) names.add(exit.name);
	for (const canvas of graph.canvases) names.add(canvas.name);
	return names;
}

/** Diagnostics that name something drawn on this canvas come first. */
export function diagnosticsFor(
	graph: CanvasGraph,
	drawing: Drawing<ServerDto>
): { here: DiagnosticDto[]; elsewhere: DiagnosticDto[] } {
	const here: DiagnosticDto[] = [];
	const elsewhere: DiagnosticDto[] = [];
	for (const diagnostic of graph.diagnostics) {
		const local =
			diagnostic.subjects.length === 0 ||
			diagnostic.subjects.some(subject => {
				const where = locate(graph, drawing, subject);
				return where.kind === 'card' || where.kind === 'bus';
			});
		(local ? here : elsewhere).push(diagnostic);
	}
	const order = (list: DiagnosticDto[]) =>
		list.sort((a, b) => Number(b.error) - Number(a.error) || a.message.localeCompare(b.message));
	return { here: order(here), elsewhere: order(elsewhere) };
}
