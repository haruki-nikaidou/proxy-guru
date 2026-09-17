import { describe, expect, test } from 'bun:test';
import type { AggregatorCard, SplitterCard } from './drawing.js';
import { clearSpot, draw } from './drawing.js';
import { connect } from './edit.js';
import { applied, exit, fanOut, leaf, pod, server, toExit, toPod } from './fixture.test-util.js';
import type { Graph } from './model.js';

describe('drawing the production fan-out', () => {
	const graph = fanOut();
	const drawing = draw(graph, 'root');
	const splitters = drawing.cards.filter((c): c is SplitterCard => c.kind === 'splitter');
	const aggregators = drawing.cards.filter((c): c is AggregatorCard => c.kind === 'aggregator');

	test('two rules over the same five servers are one splitter', () => {
		expect(splitters).toHaveLength(1);
		const [splitter] = splitters;
		expect(splitter?.policy).toBe('balance');
		expect(splitter?.arity).toBe(5);
		expect(splitter?.members.map(m => m.podId).sort()).toEqual(['api', 'web']);
		expect(splitter?.rules).toEqual(['api', 'web']);
	});

	test('each pod draws its own line, from its own dot to the relay pods it dials', () => {
		const splitter = splitters[0] as SplitterCard;
		const byHandle = (a: { handle: string }, b: { handle: string }) =>
			a.handle.localeCompare(b.handle);
		const into = drawing.buses.filter(b => b.target.node === splitter.id);
		expect(into.map(b => b.source).sort(byHandle)).toEqual([
			{ node: 'server:mobile', handle: 'pod-out:api' },
			{ node: 'server:mobile', handle: 'pod-out:web' }
		]);
		for (const bus of into) {
			expect(bus.target.handle).toBe('in');
			expect(bus.edges).toHaveLength(5);
		}
		for (let i = 0; i < 5; i += 1) {
			const out = drawing.buses.filter(
				b => b.source.node === splitter.id && b.source.handle === `out:${i}`
			);
			expect(out.map(b => b.target).sort(byHandle)).toEqual([
				{ node: `server:gcore${i + 1}`, handle: `pod-in:api-g${i + 1}` },
				{ node: `server:gcore${i + 1}`, handle: `pod-in:web-g${i + 1}` }
			]);
			expect(out.map(b => b.edges).sort()).toEqual([[`api>g${i + 1}`], [`web>g${i + 1}`]]);
			expect(out.map(b => b.rules).sort()).toEqual([['api'], ['web']]);
		}
		// Nothing is drawn from or to a server card's own handles.
		expect(
			drawing.buses.some(
				b =>
					(b.source.node.startsWith('server:') && !b.source.handle.startsWith('pod-out:')) ||
					(b.target.node.startsWith('server:') && !b.target.handle.startsWith('pod-in:'))
			)
		).toBe(false);
	});

	test('five servers handing on to two exits meet in one aggregator', () => {
		expect(aggregators).toHaveLength(1);
		const [aggregator] = aggregators;
		expect(aggregator?.sources).toEqual([1, 2, 3, 4, 5].map(i => `server:gcore${i}`));
		expect(aggregator?.targets).toEqual(['exit:exit-a', 'exit:exit-b']);
		const toA = drawing.buses.find(
			b => b.source.node === aggregator?.id && b.target.node === 'exit:exit-a'
		);
		expect(toA?.edges).toHaveLength(5);
		expect(toA?.rules).toEqual(['web']);
		// Into it, one line per relay pod, each from the pod's own dot.
		const inward = drawing.buses.filter(b => b.target.node === aggregator?.id);
		expect(inward).toHaveLength(10);
		for (const bus of inward) {
			expect(bus.source.handle.startsWith('pod-out:')).toBe(true);
			expect(bus.edges).toHaveLength(1);
		}
		// No server bus bypasses it.
		expect(
			drawing.buses.some(
				b => b.source.node.startsWith('server:gcore') && b.target.node.startsWith('exit:')
			)
		).toBe(false);
	});

	test('cards carry their pods and rules', () => {
		const gcore1 = drawing.cards.find(c => c.id === 'server:gcore1');
		expect(gcore1?.kind === 'server' && gcore1.pods.map(p => p.pod.id)).toEqual([
			'api-g1',
			'web-g1'
		]);
		expect(gcore1?.kind === 'server' && gcore1.rules).toEqual(['api', 'web']);
		const exitA = drawing.cards.find(c => c.id === 'exit:exit-a');
		expect(exitA?.kind === 'exit' && exitA.rules).toEqual(['web']);
		expect(drawing.rules.colors.get('web')).toBeNumber();
	});

	test('a drawing is stable', () => {
		expect(draw(graph, 'root')).toEqual(drawing);
	});

	test('what only the drawing has is placed between its ends', () => {
		const splitter = splitters[0] as SplitterCard;
		expect(splitter.position.x).toBeGreaterThan(0);
		expect(splitter.position.x).toBeLessThan(600);
	});
});

describe('drawing across canvases', () => {
	test('a pod in a subcanvas is reached through the subcanvas card, an outside one through a portal', () => {
		const graph: Graph = {
			canvases: [
				{ id: 'root', name: 'root', description: '', parentId: null, x: 0, y: 0 },
				{ id: 'sub', name: 'sub', description: '', parentId: 'root', x: 50, y: 50 },
				{ id: 'deep', name: 'deep', description: '', parentId: 'sub', x: 0, y: 0 }
			],
			servers: [server('a'), server('b', 'deep')],
			pods: [
				pod('entry', 'a', 'client_raw', { failover: [leaf('in'), leaf('out')] }),
				pod('hop', 'b', 'relay_tcp', leaf('back'), 'deep')
			],
			exits: [exit('origin')],
			edges: [
				toPod('in', 'entry', 'hop'),
				toExit('out', 'entry', 'origin'),
				toExit('back', 'hop', 'origin')
			],
			groups: [],
			generation: 1
		};
		const root = draw(graph, 'root');
		const splitter = root.cards.find(c => c.kind === 'splitter');
		expect(
			root.buses.map(b => `${b.source.node}#${b.source.handle}>${b.target.node}`).sort()
		).toEqual(
			[
				`${splitter?.id}#out:0>canvas:sub`,
				`${splitter?.id}#out:1>exit:origin`,
				'canvas:sub#out>exit:origin',
				`server:a#pod-out:entry>${splitter?.id}`
			].sort()
		);

		const deep = draw(graph, 'deep');
		const ids = deep.cards.map(c => c.id).sort();
		expect(ids).toEqual(['portal:root', 'server:b']);
		const portal = deep.cards.find(c => c.id === 'portal:root');
		expect(portal?.kind === 'portal' && portal.exits.map(e => e.id)).toEqual(['origin']);
		expect(portal?.kind === 'portal' && portal.pods.map(p => p.id)).toEqual(['entry']);
	});
});

describe('where a line lands', () => {
	test('an edge into a client pod, which the check refuses, lands on its server card', () => {
		const graph: Graph = {
			canvases: [{ id: 'root', name: 'root', description: '', parentId: null, x: 0, y: 0 }],
			servers: [server('a'), server('b')],
			pods: [pod('x', 'a', 'client_raw', leaf('bad')), pod('y', 'b', 'client_raw')],
			exits: [],
			edges: [toPod('bad', 'x', 'y')],
			groups: [],
			generation: 1
		};
		const bus = draw(graph, 'root').buses.find(b => b.edges.includes('bad'));
		expect(bus?.source).toEqual({ node: 'server:a', handle: 'pod-out:x' });
		expect(bus?.target).toEqual({ node: 'server:b', handle: 'in' });
	});

	test('two pods of one server into one relay pod are two lines', () => {
		const graph: Graph = {
			canvases: [{ id: 'root', name: 'root', description: '', parentId: null, x: 0, y: 0 }],
			servers: [server('a'), server('b')],
			pods: [
				pod('p', 'a', 'client_raw', leaf('p>hop')),
				pod('q', 'a', 'client_raw', leaf('q>hop')),
				pod('hop', 'b', 'relay_tcp')
			],
			exits: [],
			edges: [toPod('p>hop', 'p', 'hop'), toPod('q>hop', 'q', 'hop')],
			groups: [],
			generation: 1
		};
		const buses = draw(graph, 'root').buses;
		expect(buses.map(b => b.id).sort()).toEqual([
			'server:a#pod-out:p>server:b#pod-in:hop',
			'server:a#pod-out:q>server:b#pod-in:hop'
		]);
	});
});

describe('rule colours', () => {
	test('rules of a tree get different colours while the palette lasts', () => {
		const pods = Array.from({ length: 12 }, (_, i) => pod(`rule-${i}`, 'mobile', 'client_raw'));
		const graph: Graph = { ...fanOut(), pods: [...fanOut().pods, ...pods] };
		const colors = draw(graph, 'root').rules.colors;
		expect(new Set([...colors.values()]).size).toBe(12);
		// A thirteenth rule has to share, and still gets a slot.
		const more: Graph = { ...graph, pods: [...graph.pods, pod('rule-12', 'mobile', 'client_raw')] };
		expect(draw(more, 'root').rules.colors.get('rule-12')).toBeGreaterThanOrEqual(0);
	});

	test('two rules are far apart on the palette', () => {
		const colors = [...draw(fanOut(), 'root').rules.colors.values()];
		const [a = 0, b = 0] = colors;
		const apart = Math.abs(a - b);
		expect(Math.min(apart, 12 - apart)).toBe(6);
	});
});

describe('placing what only the drawing has', () => {
	test('a new splitter is not drawn on top of another', () => {
		const graph = fanOut();
		// Give `web` a sixth way on: its route no longer matches `api`'s.
		const after = applied(graph, connect(graph, 'web', { exit: 'exit-a' }));
		const splitters = draw(after, 'root').cards.filter(c => c.kind === 'splitter');
		expect(splitters).toHaveLength(2);
		const [a, b] = splitters;
		if (!a || !b) throw new Error('two splitters');
		const apart = Math.abs(a.position.y - b.position.y);
		expect(apart).toBeGreaterThanOrEqual(40);
	});

	test('a converted drawing places each of its splitters once', () => {
		const graph = fanOut();
		const legacy = {
			id: 'old-splitter',
			canvasId: 'root',
			kind: 'splitter',
			name: '',
			props: { x: 500, y: 400 },
			members: graph.edges.filter(e => e.target && 'pod' in e.target).map(e => ({ edge: e.id }))
		};
		const withLegacy: Graph = { ...graph, groups: [legacy] };
		const after = applied(withLegacy, connect(withLegacy, 'web', { exit: 'exit-a' }));
		const splitters = draw(after, 'root').cards.filter(c => c.kind === 'splitter');
		const at = splitters.filter(c => c.position.x === 500 && c.position.y === 400);
		expect(at).toHaveLength(1);
	});
});

describe('a clear spot for a new card', () => {
	test('is where asked when nothing is there, and off every card otherwise', () => {
		const drawing = draw(fanOut(), 'root');
		expect(clearSpot(drawing, { x: -5000, y: -5000 }, 'exit')).toEqual({ x: -5000, y: -5000 });
		const server = drawing.cards.find(c => c.id === 'server:gcore1');
		if (!server) throw new Error('fixture');
		const spot = clearSpot(drawing, server.position, 'exit');
		expect(spot).not.toEqual(server.position);
	});
});
