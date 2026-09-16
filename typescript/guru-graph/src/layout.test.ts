import { describe, expect, test } from 'bun:test';
import type { SplitterCard } from './drawing.js';
import { draw, LAYOUT_KIND, layoutGroup, positionsOf } from './drawing.js';
import { addSplitterMember, cutEdges, moveCards, putPod, setSplitterPolicy } from './edit.js';
import { applied, consistent, fanOut, pod, server } from './fixture.test-util.js';
import { carryLayout, locate, subtreeOf } from './layout.js';
import type { Graph } from './model.js';
import * as routes from './route.js';

const splitterOf = (graph: Graph): SplitterCard => {
	const card = draw(graph, 'root').cards.find(c => c.kind === 'splitter');
	if (card?.kind !== 'splitter') throw new Error('no splitter');
	return card;
};

/** The fan-out with its splitter and aggregator placed by hand. */
function placed(): Graph {
	const graph = fanOut();
	const drawing = draw(graph, 'root');
	const moved = drawing.cards
		.filter(c => c.kind === 'splitter' || c.kind === 'aggregator')
		.map(c => ({
			node: c.id,
			position: c.kind === 'splitter' ? { x: 1000, y: 2000 } : { x: 1500, y: 2500 }
		}));
	const { layout } = moveCards(graph, 'root', moved);
	if (!layout) throw new Error('no layout');
	return applied(graph, layout);
}

describe('positions across an edit', () => {
	test('a splitter that gains a member stays where it was', () => {
		const graph: Graph = { ...placed(), servers: [...placed().servers, server('gcore6')] };
		const before = splitterOf(graph);
		expect(before.position).toEqual({ x: 1000, y: 2000 });
		const change = addSplitterMember(graph, draw(graph, 'root'), before.id, {
			server: 'gcore6',
			ingress: 'relay_quic',
			canvasId: 'root'
		});
		// Without carrying, the new shape is placed afresh.
		const naive = splitterOf(applied(graph, change));
		expect(naive.id).not.toBe(before.id);
		expect(naive.position).not.toEqual(before.position);

		const carried = carryLayout(graph, 'root', change);
		const after = applied(graph, carried);
		expect(consistent(after)).toEqual([]);
		const splitter = splitterOf(after);
		expect(splitter.position).toEqual({ x: 1000, y: 2000 });
		// The layout group was updated, not duplicated, and forgot the old name.
		const groups = after.groups.filter(g => g.kind === LAYOUT_KIND);
		expect(groups).toHaveLength(1);
		const positions = positionsOf(layoutGroup(after, 'root'));
		expect(positions.has(before.id)).toBe(false);
		expect(positions.has(splitter.id)).toBe(true);
	});

	test('a policy change keeps the splitter too', () => {
		const graph = placed();
		const before = splitterOf(graph);
		const change = setSplitterPolicy(graph, draw(graph, 'root'), before.id, {
			kind: 'failover',
			sticky: false,
			weights: [1, 1, 1, 1, 1],
			order: [0, 1, 2, 3, 4]
		});
		const after = applied(graph, carryLayout(graph, 'root', change));
		expect(splitterOf(after).policy).toBe('failover');
		expect(splitterOf(after).position).toEqual(before.position);
	});

	test('a change that moves nothing is left alone', () => {
		const graph = placed();
		const change = cutEdges(graph, [], true);
		expect(carryLayout(graph, 'root', change)).toEqual(change);
	});

	test('an edit pins what was placed afresh, so nothing else moves', () => {
		const graph = fanOut();
		const before = draw(graph, 'root');
		const change = carryLayout(graph, 'root', cutEdges(graph, ['web>g1'], true));
		const after = draw(applied(graph, change), 'root');
		for (const card of after.cards) {
			if (card.kind !== 'aggregator') continue;
			const old = before.cards.find(c => c.id === card.id);
			if (old) expect(card.position).toEqual(old.position);
		}
		expect(positionsOf(layoutGroup(applied(graph, change), 'root')).size).toBeGreaterThan(0);
	});

	test('a ghost server moves in the layout group, not its row', () => {
		const graph: Graph = {
			...fanOut(),
			canvases: [
				...fanOut().canvases,
				{ id: 'child', name: 'child', description: '', parentId: 'root', x: 0, y: 0 }
			],
			servers: [...fanOut().servers, server('far', 'child', 5, 5)],
			pods: [...fanOut().pods, pod('visitor', 'far', 'relay_tcp')]
		};
		const moves = moveCards(graph, 'root', [{ node: 'server:far', position: { x: 70, y: 80 } }]);
		expect(moves.servers).toEqual([]);
		if (!moves.layout) throw new Error('no layout');
		const card = draw(applied(graph, moves.layout), 'root').cards.find(c => c.id === 'server:far');
		expect(card?.kind === 'server' && card.ghost).toBe(true);
		expect(card?.position).toEqual({ x: 70, y: 80 });
	});
});

describe('finding things on a drawing', () => {
	const graph: Graph = {
		...fanOut(),
		canvases: [
			...fanOut().canvases,
			{ id: 'child', name: 'child', description: '', parentId: 'root', x: 0, y: 0 },
			{ id: 'grandchild', name: 'grandchild', description: '', parentId: 'child', x: 0, y: 0 },
			{ id: 'other', name: 'other', description: '', parentId: null, x: 0, y: 0 }
		],
		servers: [...fanOut().servers, server('deep', 'grandchild')],
		pods: [...fanOut().pods, pod('deep-pod', 'deep', 'relay_tcp', null, 'grandchild')]
	};
	const drawing = draw(graph, 'root');

	test('a pod is on its server card, an edge on its bus', () => {
		expect(locate(graph, drawing, { pod: 'web-g3' })).toEqual({
			kind: 'card',
			node: 'server:gcore3'
		});
		const bus = locate(graph, drawing, { edge: 'web>g2' });
		expect(bus.kind).toBe('bus');
		expect(locate(graph, drawing, { exit: 'exit-b' })).toEqual({
			kind: 'card',
			node: 'exit:exit-b'
		});
	});

	test('something nested is on the child canvas card; elsewhere is a canvas to open', () => {
		expect(locate(graph, drawing, { pod: 'deep-pod' })).toEqual({
			kind: 'card',
			node: 'canvas:child'
		});
		expect(locate(graph, drawing, { canvas: 'grandchild' })).toEqual({
			kind: 'card',
			node: 'canvas:child'
		});
		const inside = draw(graph, 'grandchild');
		expect(locate(graph, inside, { exit: 'exit-a' })).toEqual({ kind: 'canvas', canvasId: 'root' });
		expect(locate(graph, drawing, { pod: 'missing' })).toEqual({ kind: 'nowhere' });
	});

	test('a subtree holds its descendants only', () => {
		expect([...subtreeOf(graph, 'child')].sort()).toEqual(['child', 'grandchild']);
		expect([...subtreeOf(graph, 'root')].sort()).toEqual(['child', 'grandchild', 'root']);
	});
});

describe('an older drawing', () => {
	const withLegacy = (): Graph => {
		const graph = fanOut();
		const legacy = {
			id: 'old-splitter',
			canvasId: 'root',
			kind: 'splitter',
			name: '',
			props: { x: 500, y: 400 },
			members: graph.edges.filter(e => 'pod' in e.target).map(e => ({ edge: e.id }))
		};
		return { ...graph, groups: [legacy] };
	};

	test('seeds a canvas until the dashboard lays it out, and never moves a card after', () => {
		const graph = withLegacy();
		expect(splitterOf(graph).position).toEqual({ x: 500, y: 400 });
		// Moving the aggregator writes a layout group that keeps the splitter too.
		const aggregator = draw(graph, 'root').cards.find(c => c.kind === 'aggregator');
		if (!aggregator) throw new Error('aggregator');
		const { layout } = moveCards(graph, 'root', [
			{ node: aggregator.id, position: { x: 900, y: 900 } }
		]);
		if (!layout) throw new Error('layout');
		const moved = applied(graph, layout);
		expect(splitterOf(moved).position).toEqual({ x: 500, y: 400 });
	});

	test('does not place a card the dashboard has not drawn before', () => {
		const graph = withLegacy();
		// `web` nests two of its ways: a new splitter appears inside the old one.
		const web = graph.pods.find(p => p.id === 'web');
		if (!web?.route || !('balance' in web.route)) throw new Error('fixture');
		const nested = { ...web, route: routes.nest(web.route, [], [0, 1], 'failover') };
		const change = carryLayout(graph, 'root', putPod(graph, nested));
		const after = draw(applied(graph, change), 'root');
		const at = after.cards.filter(
			c => c.kind === 'splitter' && c.position.x === 500 && c.position.y === 400
		);
		expect(at.length).toBeLessThanOrEqual(1);
	});
});
