import { describe, expect, test } from 'bun:test';
import type { SplitterCard } from './drawing.js';
import { draw } from './drawing.js';
import {
	addSplitterMember,
	connect,
	cutEdges,
	EditError,
	joinSplitter,
	moveCards,
	newPod,
	putPod,
	removeAll,
	removeExits,
	removePods,
	removeSplitter,
	setSplitterPolicy
} from './edit.js';
import { applied, consistent, exit, fanOut, pod, server } from './fixture.test-util.js';
import { isRecordKey } from './ids.js';
import type { Graph } from './model.js';
import { leaves } from './route.js';

const splitterOf = (graph: Graph): SplitterCard => {
	const card = draw(graph, 'root').cards.find(c => c.kind === 'splitter');
	if (card?.kind !== 'splitter') throw new Error('no splitter');
	return card;
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
		const reason = (fn: () => unknown) => {
			try {
				fn();
			} catch (err) {
				return err instanceof EditError ? err.code : 'other';
			}
			return 'none';
		};
		expect(reason(() => connect(graph, 'web', { pod: 'web' }))).toBe('self_dial');
		expect(reason(() => connect(graph, 'web', { pod: 'api' }))).toBe('client_pod_dialed');
		expect(reason(() => connect(graph, 'ghost', { exit: 'exit-a' }))).toBe('pod_not_found');
		expect(reason(() => connect(graph, 'web', { exit: 'nope' }))).toBe('exit_not_found');
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
