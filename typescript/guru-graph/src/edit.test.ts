import { describe, expect, test } from 'bun:test';
import type { AggregatorCard, SplitterCard } from './drawing.js';
import { draw } from './drawing.js';
import {
	addSplitterMember,
	addWayOn,
	canDropWayOn,
	connect,
	connectEach,
	cutEdges,
	EditError,
	fanOutSiblings,
	joinSplitter,
	moveCards,
	newPod,
	putPod,
	removeAll,
	removeExits,
	removePods,
	removeSplitter,
	setSplitterPolicy,
	targetOf
} from './edit.js';
import {
	applied,
	consistent,
	exit,
	fanOut,
	leaf,
	pod,
	server,
	toExit,
	toPod
} from './fixture.test-util.js';
import { isRecordKey } from './ids.js';
import type { Graph, Route } from './model.js';
import { leaves } from './route.js';

const splitterOf = (graph: Graph): SplitterCard => {
	const card = draw(graph, 'root').cards.find(c => c.kind === 'splitter');
	if (card?.kind !== 'splitter') throw new Error('no splitter');
	return card;
};

/** The aggregator whose ways out include `target`. */
const aggregatorTo = (graph: Graph, target: string): AggregatorCard => {
	const card = draw(graph, 'root').cards.find(
		c => c.kind === 'aggregator' && c.targets.includes(target)
	);
	if (card?.kind !== 'aggregator') throw new Error('no aggregator');
	return card;
};

const routeOf = (graph: Graph, id: string): Route | null =>
	graph.pods.find(p => p.id === id)?.route ?? null;

/** The code of the `EditError` a gesture throws, `other` or `none`. */
const reasonOf = (fn: () => unknown) => {
	try {
		fn();
	} catch (err) {
		return err instanceof EditError ? err.code : 'other';
	}
	return 'none';
};

describe('connecting', () => {
	test('a first way on is the route; a second makes it a balance', () => {
		const graph: Graph = {
			...fanOut(),
			pods: [...fanOut().pods, pod('new', 'mobile', 'client_raw')]
		};
		const first = applied(graph, connect(graph, 'new', { exit: 'exit-a' }));
		const created = first.pods.find(p => p.id === 'new');
		expect(created?.route && 'edge' in created.route).toBe(true);
		expect(consistent(first)).toEqual([]);

		const second = applied(first, connect(first, 'new', { pod: 'web-g1' }));
		const route = second.pods.find(p => p.id === 'new')?.route;
		expect(route && 'balance' in route && route.balance).toHaveLength(2);
		expect(consistent(second)).toEqual([]);
	});

	test('a way on can join a group inside the route', () => {
		const tiered = {
			failover: [
				{ balance: [1, 2, 3, 4].map(i => ({ to: { edge: `web>g${i}` } })) },
				{ edge: 'web>g5' }
			]
		};
		const graph: Graph = {
			...fanOut(),
			pods: fanOut().pods.map(p => (p.id === 'web' ? { ...p, route: tiered } : p))
		};
		expect(consistent(graph)).toEqual([]);
		const after = applied(graph, connect(graph, 'web', { exit: 'exit-a' }, [0]));
		expect(consistent(after)).toEqual([]);
		const route = after.pods.find(p => p.id === 'web')?.route;
		const first = route && 'failover' in route ? route.failover[0] : null;
		expect(first && 'balance' in first && first.balance).toHaveLength(5);
		expect(route && 'failover' in route && route.failover[1]).toEqual({ edge: 'web>g5' });
	});

	test('a server target lands on a new relay pod there', () => {
		const graph = fanOut();
		const change = connect(graph, 'web', {
			server: 'gcore1',
			ingress: 'relay_tls',
			canvasId: 'root'
		});
		const [landing] = change.putPods.filter(p => p.id !== 'web');
		expect(landing?.serverId).toBe('gcore1');
		expect(landing?.ingress).toEqual({ kind: 'relay_tls' });
		expect(landing?.port).toBe(0);
		expect(isRecordKey(landing?.id ?? '')).toBe(true);
		expect(consistent(applied(graph, change))).toEqual([]);
	});

	test('nonsense is refused with a reason', () => {
		const graph = fanOut();
		expect(reasonOf(() => connect(graph, 'web', { pod: 'web' }))).toBe('self_dial');
		expect(reasonOf(() => connect(graph, 'web', { pod: 'api' }))).toBe('client_pod_dialed');
		expect(reasonOf(() => connect(graph, 'ghost', { exit: 'exit-a' }))).toBe('pod_not_found');
		expect(reasonOf(() => connect(graph, 'web', { exit: 'nope' }))).toBe('exit_not_found');
	});
});

describe('splitters', () => {
	test('a pod joining a splitter gets relay pods of its own on every server', () => {
		const base = fanOut();
		const graph: Graph = { ...base, pods: [...base.pods, pod('ssh', 'mobile', 'client_raw')] };
		const splitter = splitterOf(graph);
		const change = joinSplitter(graph, draw(graph, 'root'), 'ssh', splitter.id);
		const after = applied(graph, change);
		expect(consistent(after)).toEqual([]);
		const landings = change.putPods.filter(p => p.id !== 'ssh');
		expect(landings.map(p => p.serverId).sort()).toEqual([
			'gcore1',
			'gcore2',
			'gcore3',
			'gcore4',
			'gcore5'
		]);
		// Each landing pod goes on where the template's did: the splitter's first
		// member, whose relays exit to its exit.
		const template = splitter.members[0]?.podId;
		const templateExit = template === 'web' ? 'exit-a' : 'exit-b';
		for (const landing of landings) {
			const out = after.edges.filter(e => e.sourcePodId === landing.id);
			expect(out).toHaveLength(1);
			expect(out[0]?.target).toEqual({ exit: templateExit });
		}
		// The joiner is drawn into the same splitter, a third member.
		expect(
			splitterOf(after)
				.members.map(m => m.podId)
				.sort()
		).toEqual(['api', 'ssh', 'web']);
	});

	test('a member added to a splitter is added for every pod it stands for', () => {
		const graph: Graph = { ...fanOut(), servers: [...fanOut().servers, server('gcore6')] };
		const splitter = splitterOf(graph);
		const change = addSplitterMember(graph, draw(graph, 'root'), splitter.id, {
			server: 'gcore6',
			ingress: 'relay_quic',
			canvasId: 'root'
		});
		const after = applied(graph, change);
		expect(consistent(after)).toEqual([]);
		expect(splitterOf(after).arity).toBe(6);
		expect(change.putPods.filter(p => p.serverId === 'gcore6')).toHaveLength(2);
	});

	test('a policy change rewrites every pod the splitter stands for', () => {
		const graph = fanOut();
		const splitter = splitterOf(graph);
		const change = setSplitterPolicy(graph, draw(graph, 'root'), splitter.id, {
			kind: 'balance',
			sticky: true,
			weights: [5, 1, 1, 1, 1],
			order: [4, 3, 2, 1, 0]
		});
		const after = applied(graph, change);
		expect(consistent(after)).toEqual([]);
		for (const id of ['web', 'api']) {
			const route = after.pods.find(p => p.id === id)?.route;
			if (!route || !('balance' in route)) throw new Error('not a balance');
			expect(route.sticky).toBe('client_ip');
			expect(route.balance[0]?.weight).toBe(5);
			expect(leaves(route)[0]).toBe(`${id}>g5`);
		}
		// Both pods still share one splitter.
		expect(splitterOf(after).members).toHaveLength(2);

		const failover = applied(
			graph,
			setSplitterPolicy(graph, draw(graph, 'root'), splitter.id, {
				kind: 'failover',
				sticky: false,
				weights: [1, 1, 1, 1, 1],
				order: [0, 1, 2, 3, 4]
			})
		);
		expect(splitterOf(failover).policy).toBe('failover');
	});

	test('removing a splitter prunes the relay pods only it dialed', () => {
		const graph = fanOut();
		const splitter = splitterOf(graph);
		const change = removeSplitter(graph, draw(graph, 'root'), splitter.id, true);
		const after = applied(graph, change);
		expect(consistent(after)).toEqual([]);
		expect(after.pods.map(p => p.id).sort()).toEqual(['api', 'web']);
		expect(after.edges).toEqual([]);
		expect(after.pods.every(p => p.route === null)).toBe(true);

		const kept = applied(graph, removeSplitter(graph, draw(graph, 'root'), splitter.id, false));
		expect(kept.pods).toHaveLength(12);
		expect(consistent(kept)).toEqual([]);
	});
});

describe('a way on for a whole row', () => {
	const withExits = (graph: Graph, ...ids: string[]): Graph => ({
		...graph,
		exits: [...graph.exits, ...ids.map((id, i) => exit(id, 'root', 1200, 1000 + i * 200))]
	});

	test("an aggregator's way out gives every pod on it a way on, and one splitter appears", () => {
		const graph = withExits(fanOut(), 'exit-c', 'exit-d');
		const aggregator = aggregatorTo(graph, 'exit:exit-a');
		const change = addWayOn(
			graph,
			draw(graph, 'root'),
			{ node: aggregator.id, handle: 'out:exit:exit-a' },
			{ exit: 'exit-c' }
		);
		expect(change.putEdges).toHaveLength(5);
		const after = applied(graph, change);
		expect(consistent(after)).toEqual([]);
		for (let i = 1; i <= 5; i += 1) {
			const route = routeOf(after, `web-g${i}`);
			if (!route || !('balance' in route)) throw new Error('not a balance');
			expect(route.balance[0]?.to).toEqual(leaf(`web-g${i}>out`));
			expect(route.balance).toHaveLength(2);
			expect(routeOf(after, `api-g${i}`)).toEqual(leaf(`api-g${i}>out`));
		}
		// Drawn: one new splitter for the five routes, still behind the aggregator.
		const redrawn = draw(after, 'root');
		const added = redrawn.cards.find(
			(c): c is SplitterCard => c.kind === 'splitter' && c.members.some(m => m.podId === 'web-g1')
		);
		expect(added?.members).toHaveLength(5);
		expect(added?.arity).toBe(2);
		const gathered = aggregatorTo(after, 'exit:exit-b');
		expect(gathered.id).toBe(aggregator.id);
		expect(gathered.targets).toEqual(['exit:exit-b', added?.id ?? '']);

		// The way out into that splitter adds a member to each of those routes.
		const again = applied(
			after,
			addWayOn(
				after,
				redrawn,
				{ node: gathered.id, handle: `out:${added?.id}` },
				{ exit: 'exit-d' }
			)
		);
		expect(consistent(again)).toEqual([]);
		const route = routeOf(again, 'web-g3');
		expect(route && 'balance' in route && route.balance).toHaveLength(3);
	});

	test('a new relay pod on a server is shared by the pods of one rule, not across rules', () => {
		const base = fanOut();
		const graph: Graph = { ...base, servers: [...base.servers, server('gcore6')] };
		const rowA = aggregatorTo(graph, 'exit:exit-a');
		const change = addWayOn(
			graph,
			draw(graph, 'root'),
			{ node: rowA.id, handle: 'out:exit:exit-a' },
			{ server: 'gcore6', ingress: 'relay_quic', canvasId: 'root' }
		);
		// web-g1..5 have five different names and one rule: one landing pod.
		const [landing, ...more] = change.putPods.filter(p => p.serverId === 'gcore6');
		expect(more).toHaveLength(0);
		expect(
			change.putEdges.filter(e => 'pod' in e.target && e.target.pod === landing?.id)
		).toHaveLength(5);
		expect(consistent(applied(graph, change))).toEqual([]);

		// Both rules on one way out, every relay pod named alike: one pod per rule.
		const shared: Graph = {
			...graph,
			pods: graph.pods.map(p => (p.id.includes('-g') ? { ...p, name: 'relay' } : p)),
			edges: graph.edges.map(e =>
				e.id.startsWith('api-g') ? { ...e, target: { exit: 'exit-a' } } : e
			)
		};
		const both = addWayOn(
			shared,
			draw(shared, 'root'),
			{ node: aggregatorTo(shared, 'exit:exit-a').id, handle: 'out:exit:exit-a' },
			{ server: 'gcore6', ingress: 'relay_quic', canvasId: 'root' }
		);
		expect(both.putPods.filter(p => p.serverId === 'gcore6')).toHaveLength(2);
		expect(consistent(applied(shared, both))).toEqual([]);
	});

	test("a splitter's member row gives that member of every route a way on", () => {
		const base = withExits(fanOut(), 'exit-c');
		const weighted = applied(
			base,
			setSplitterPolicy(base, draw(base, 'root'), splitterOf(base).id, {
				kind: 'balance',
				sticky: true,
				weights: [5, 1, 1, 1, 1],
				order: [0, 1, 2, 3, 4]
			})
		);
		const change = addWayOn(
			weighted,
			draw(weighted, 'root'),
			{ node: splitterOf(weighted).id, handle: 'out:2' },
			{ exit: 'exit-c' }
		);
		const after = applied(weighted, change);
		expect(consistent(after)).toEqual([]);
		for (const id of ['web', 'api']) {
			const route = routeOf(after, id);
			if (!route || !('balance' in route)) throw new Error('not a balance');
			expect(route.sticky).toBe('client_ip');
			expect(route.balance.map(m => m.weight ?? 1)).toEqual([5, 1, 1, 1, 1]);
			const member = route.balance[2]?.to;
			if (!member || !('balance' in member)) throw new Error('member not a balance');
			expect(member.sticky).toBe('client_ip');
			expect(member.balance[0]?.to).toEqual(leaf(`${id}>g3`));
		}
	});

	test('a pod holding one splitter twice gains a way on at both', () => {
		const base = fanOut();
		const graph: Graph = {
			...base,
			pods: [
				...base.pods.map(p =>
					p.id === 'web'
						? {
								...p,
								route: {
									failover: [
										{ balance: [{ to: leaf('web>g1') }, { to: leaf('web>g2') }] },
										{ balance: [{ to: leaf('web>h1') }, { to: leaf('web>h2') }] },
										{ balance: ['web>g3', 'web>g4', 'web>g5'].map(id => ({ to: leaf(id) })) }
									]
								}
							}
						: p
				),
				pod('web-h1', 'gcore1', 'relay_quic', leaf('web-h1>out')),
				pod('web-h2', 'gcore2', 'relay_quic', leaf('web-h2>out'))
			],
			edges: [
				...base.edges,
				toPod('web>h1', 'web', 'web-h1'),
				toPod('web>h2', 'web', 'web-h2'),
				toExit('web-h1>out', 'web-h1', 'exit-a'),
				toExit('web-h2>out', 'web-h2', 'exit-a')
			],
			exits: [...base.exits, exit('exit-c')]
		};
		expect(consistent(graph)).toEqual([]);
		const drawing = draw(graph, 'root');
		const twice = drawing.cards.find(
			(c): c is SplitterCard =>
				c.kind === 'splitter' && c.members.filter(m => m.podId === 'web').length === 2
		);
		if (!twice) throw new Error('no splitter held twice');
		const after = applied(
			graph,
			addWayOn(graph, drawing, { node: twice.id, handle: 'out:1' }, { exit: 'exit-c' })
		);
		expect(consistent(after)).toEqual([]);
		const route = routeOf(after, 'web');
		if (!route || !('failover' in route)) throw new Error('not a failover');
		for (const group of route.failover.slice(0, 2)) {
			if (!('balance' in group)) throw new Error('not a balance');
			const second = group.balance[1]?.to;
			expect(second && 'balance' in second && second.balance).toHaveLength(2);
		}
	});

	test('a row may not dial its own pods, lead back into itself, or use a stale drawing', () => {
		const base = withExits(fanOut(), 'exit-c');
		// api-g1 also dials web-g2, so anything leading to api-g1 leads to web-g2.
		const graph: Graph = {
			...base,
			pods: base.pods.map(p =>
				p.id === 'api-g1'
					? { ...p, route: { balance: [{ to: leaf('api-g1>out') }, { to: leaf('api-g1>web') }] } }
					: p
			),
			edges: [...base.edges, toPod('api-g1>web', 'api-g1', 'web-g2')]
		};
		const drawing = draw(graph, 'root');
		const row = { node: aggregatorTo(graph, 'exit:exit-a').id, handle: 'out:exit:exit-a' };
		expect(reasonOf(() => addWayOn(graph, drawing, row, { pod: 'web-g1' }))).toBe('self_dial');
		expect(reasonOf(() => addWayOn(graph, drawing, row, { pod: 'api-g1' }))).toBe('cycle');
		expect(reasonOf(() => addWayOn(graph, drawing, row, { pod: 'web' }))).toBe('client_pod_dialed');
		expect(
			reasonOf(() =>
				addWayOn(graph, drawing, { node: 'agg:gone', handle: row.handle }, { exit: 'exit-c' })
			)
		).toBe('aggregator_not_found');

		// A drawing of routes that have changed since: nothing half-made.
		const fan = withExits(fanOut(), 'exit-c');
		const old = draw(fan, 'root');
		const reshaped: Graph = {
			...fan,
			pods: fan.pods.map(p => (p.id === 'web' ? { ...p, route: leaf('web>g1') } : p))
		};
		const splitter = splitterOf(fan);
		expect(
			reasonOf(() =>
				addWayOn(reshaped, old, { node: splitter.id, handle: 'out:3' }, { exit: 'exit-c' })
			)
		).toBe('splitter_not_found');
		expect(reasonOf(() => addSplitterMember(fan, old, 'split:gone', { exit: 'exit-c' }))).toBe(
			'splitter_not_found'
		);
	});

	test('a row is only offered drops it can use', () => {
		const graph = withExits(fanOut(), 'exit-c');
		const drawing = draw(graph, 'root');
		const splitter = splitterOf(graph);
		const row = { node: aggregatorTo(graph, 'exit:exit-a').id, handle: 'out:exit:exit-a' };
		const drop = (from: { node: string; handle: string }, node: string, handle: string) =>
			canDropWayOn(graph, drawing, from, { node, handle });
		expect(drop(row, 'exit:exit-c', 'in')).toBe(true);
		expect(drop(row, 'server:gcore1', 'in')).toBe(true);
		expect(drop(row, 'server:gcore1', 'pod-in:api-g1')).toBe(true);
		// Where it already runs, a splitter, its own pods.
		expect(drop(row, 'exit:exit-a', 'in')).toBe(false);
		expect(drop(row, splitter.id, 'in')).toBe(false);
		expect(drop(row, 'server:gcore1', 'pod-in:web-g1')).toBe(false);
		const member = { node: splitter.id, handle: 'out:0' };
		expect(drop(member, 'server:gcore1', 'pod-in:web-g1')).toBe(false);
		expect(drop(member, 'exit:exit-c', 'in')).toBe(true);
		expect(drop({ node: 'server:mobile', handle: 'pod-out:web' }, 'exit:exit-c', 'in')).toBe(false);
	});
});

describe('connecting the rest of a fan-out', () => {
	/** The fan-out right after it landed: relay pods with no way on yet. */
	const landed = (): Graph => {
		const base = fanOut();
		return {
			...base,
			servers: [...base.servers, server('gcore6')],
			pods: base.pods.map(p => (p.id.includes('-g') ? { ...p, route: null } : p)),
			edges: base.edges.filter(e => !e.id.endsWith('>out'))
		};
	};

	test('the relay pods landed with one are found, and connected in one batch', () => {
		const graph = landed();
		const first = connect(graph, 'web-g1', { exit: 'exit-a' });
		const target = targetOf(first, 'web-g1');
		expect(target).toEqual({ exit: 'exit-a' });
		if (!target) throw new Error('no target');
		const after = applied(graph, first);
		const siblings = fanOutSiblings(after, 'web-g1', target);
		expect(siblings).toEqual(['web-g2', 'web-g3', 'web-g4', 'web-g5']);
		const rest = applied(after, connectEach(after, siblings, target));
		expect(consistent(rest)).toEqual([]);
		expect(fanOutSiblings(rest, 'web-g1', target)).toEqual([]);
	});

	test('a relay pod made for the first is where the rest go', () => {
		const graph = landed();
		const first = connect(graph, 'web-g1', {
			server: 'gcore6',
			ingress: 'relay_tls',
			canvasId: 'root'
		});
		const target = targetOf(first, 'web-g1');
		if (!target || !('pod' in target)) throw new Error('not a pod');
		const after = applied(graph, first);
		const siblings = fanOutSiblings(after, 'web-g1', target);
		expect(siblings).toHaveLength(4);
		const rest = applied(after, connectEach(after, siblings, target));
		expect(consistent(rest)).toEqual([]);
		expect(rest.edges.filter(e => 'pod' in e.target && e.target.pod === target.pod)).toHaveLength(
			5
		);
	});

	test('pods with a way on, the target and pods dialed twice are not siblings', () => {
		const graph = landed();
		const withG2 = applied(graph, connect(graph, 'web-g2', { exit: 'exit-a' }));
		const first = connect(withG2, 'web-g1', { pod: 'web-g3' });
		expect(targetOf(first, 'web-g1')).toEqual({ pod: 'web-g3' });
		const after = applied(withG2, first);
		expect(fanOutSiblings(after, 'web-g1', { pod: 'web-g3' })).toEqual(['web-g4', 'web-g5']);
		// web-g3 is dialed by web and by web-g1 now.
		expect(fanOutSiblings(after, 'web-g3', { exit: 'exit-a' })).toEqual([]);
		// A client pod has no dialer at all.
		expect(fanOutSiblings(after, 'web', { exit: 'exit-a' })).toEqual([]);
	});
});

describe('removing', () => {
	test('cutting one edge of a balance leaves a balance over the rest', () => {
		const graph = fanOut();
		const after = applied(graph, cutEdges(graph, ['web>g1'], true));
		expect(consistent(after)).toEqual([]);
		expect(after.pods.some(p => p.id === 'web-g1')).toBe(false);
		const route = after.pods.find(p => p.id === 'web')?.route;
		expect(route && 'balance' in route && route.balance).toHaveLength(4);
	});

	test('a pod goes with every edge into and out of it', () => {
		const graph = fanOut();
		const after = applied(graph, removePods(graph, ['api-g3'], false));
		expect(consistent(after)).toEqual([]);
		expect(after.edges.some(e => e.id === 'api>g3' || e.id === 'api-g3>out')).toBe(false);

		const whole = applied(graph, removePods(graph, ['web'], true));
		expect(consistent(whole)).toEqual([]);
		expect(whole.pods.filter(p => p.id.startsWith('web'))).toEqual([]);
	});

	test('an exit goes with the edges into it', () => {
		const graph = fanOut();
		const after = applied(graph, removeExits(graph, ['exit-a']));
		expect(consistent(after)).toEqual([]);
		expect(after.pods.filter(p => p.id.startsWith('web-g')).every(p => p.route === null)).toBe(
			true
		);
	});

	test('a dialed relay pod cannot become a client pod', () => {
		const graph = fanOut();
		const hop = graph.pods.find(p => p.id === 'web-g1');
		if (!hop) throw new Error('fixture');
		expect(() =>
			putPod(graph, { ...hop, ingress: { kind: 'client_raw', receiveProxyProtocol: null } })
		).toThrow(EditError);
		expect(putPod(graph, { ...hop, ingress: { kind: 'relay_tcp' } }).putPods).toHaveLength(1);
		expect(newPod('root', 'mobile', 'x', { kind: 'relay_tcp' }).port).toBe(0);
	});
});

describe('removing a selection', () => {
	test('a splitter and a bus into it go as one batch', () => {
		const graph = fanOut();
		const drawing = draw(graph, 'root');
		const splitter = splitterOf(graph);
		const into = drawing.buses.find(b => b.target.node === splitter.id);
		const change = removeAll(
			graph,
			drawing,
			{ splitterIds: [splitter.id], edgeIds: into?.edges ?? [] },
			true
		);
		const after = applied(graph, change);
		expect(consistent(after)).toEqual([]);
		expect(after.pods.map(p => p.id).sort()).toEqual(['api', 'web']);
	});

	test('the pods of a server and an exit together', () => {
		const graph = fanOut();
		const change = removeAll(
			graph,
			draw(graph, 'root'),
			{ podIds: ['web-g1', 'api-g1'], exitIds: ['exit-a'] },
			false
		);
		const after = applied(graph, change);
		expect(consistent(after)).toEqual([]);
		expect(after.exits.map(e => e.id)).toEqual(['exit-b']);
		expect(after.pods.some(p => p.serverId === 'gcore1')).toBe(false);
		// Pods that exited to exit-a stay, with nowhere to go.
		expect(after.pods.find(p => p.id === 'web-g2')?.route).toBeNull();
	});
});

describe('moving', () => {
	test('rows move as rows, drawn cards in the layout group', () => {
		const graph = fanOut();
		const splitter = splitterOf(graph);
		const moves = moveCards(graph, 'root', [
			{ node: 'server:gcore1', position: { x: 10.4, y: 20.6 } },
			{ node: 'exit:exit-a', position: { x: 1, y: 2 } },
			{ node: splitter.id, position: { x: 333, y: 444 } }
		]);
		expect(moves.servers).toEqual([{ id: 'gcore1', position: { x: 10, y: 21 } }]);
		expect(moves.exits).toEqual([{ id: 'exit-a', position: { x: 1, y: 2 } }]);
		const layout = moves.layout?.putGroups[0];
		expect(layout?.kind).toBe('layout');
		if (!moves.layout) throw new Error('no layout change');
		const after = applied(graph, moves.layout);
		expect(splitterOf(after).position).toEqual({ x: 333, y: 444 });
		// A second move keeps the group.
		const again = moveCards(after, 'root', [{ node: splitter.id, position: { x: 1, y: 1 } }]);
		expect(again.layout?.putGroups[0]?.id).toBe(layout?.id);
	});

	test('an exit card is placed where its row says', () => {
		const graph: Graph = { ...fanOut(), exits: [exit('exit-a', 'root', 7, 8), exit('exit-b')] };
		const card = draw(graph, 'root').cards.find(c => c.id === 'exit:exit-a');
		expect(card?.position).toEqual({ x: 7, y: 8 });
	});
});
