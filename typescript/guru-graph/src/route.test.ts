import { describe, expect, test } from 'bun:test';
import type { Route } from './model.js';
import {
	appendMember,
	at,
	leaves,
	mapLeaves,
	nest,
	nodes,
	removeEdges,
	reordered,
	replaceAt,
	signature,
	unnest,
	withPolicy
} from './route.js';

const e = (edge: string): Route => ({ edge });

describe('routes', () => {
	const route: Route = {
		failover: [
			{ balance: [{ weight: 2, to: e('a') }, { to: e('b') }], sticky: 'client_ip' },
			e('c')
		]
	};

	test('leaves are the edges in order', () => {
		expect(leaves(route)).toEqual(['a', 'b', 'c']);
		expect(leaves(null)).toEqual([]);
	});

	test('a path names a node', () => {
		expect(at(route, [0, 1])).toEqual(e('b'));
		expect(at(route, [3])).toBeNull();
		expect(nodes(route).map(n => n.path)).toEqual([[], [0], [0, 0], [0, 1], [1]]);
	});

	test('removing edges collapses groups of one and drops empty ones', () => {
		expect(removeEdges(route, new Set(['b']))).toEqual({ failover: [e('a'), e('c')] });
		expect(removeEdges(route, new Set(['c']))).toEqual({
			balance: [{ weight: 2, to: e('a') }, { to: e('b') }],
			sticky: 'client_ip'
		});
		expect(removeEdges(route, new Set(['a', 'b', 'c']))).toBeNull();
	});

	test('a new member extends the route', () => {
		expect(appendMember(null, e('x'))).toEqual(e('x'));
		expect(appendMember(e('a'), e('x'))).toEqual({ balance: [{ to: e('a') }, { to: e('x') }] });
		expect(appendMember(route, e('x'))).toEqual({
			failover: [...(route as { failover: Route[] }).failover, e('x')]
		});
	});

	test('replacing a node keeps the rest', () => {
		expect(replaceAt(route, [0, 0], e('z'))).toEqual({
			failover: [
				{ balance: [{ weight: 2, to: e('z') }, { to: e('b') }], sticky: 'client_ip' },
				e('c')
			]
		});
		expect(replaceAt(route, [1], null)).toEqual({
			balance: [{ weight: 2, to: e('a') }, { to: e('b') }],
			sticky: 'client_ip'
		});
	});

	test('policy and order change a group, not its members', () => {
		const balance = at(route, [0]) as Route;
		expect(withPolicy(balance, { kind: 'failover' })).toEqual({ failover: [e('a'), e('b')] });
		expect(reordered(balance, [1, 0])).toEqual({
			balance: [{ to: e('b') }, { weight: 2, to: e('a') }],
			sticky: 'client_ip'
		});
		expect(withPolicy(route, { kind: 'balance', sticky: false, weights: [3, 0] })).toEqual({
			balance: [{ weight: 3, to: at(route, [0]) as Route }, { to: e('c') }]
		});
	});

	test('signatures name leaves by where they go', () => {
		const where = (edge: string) => (edge === 'a' || edge === 'b' ? 'same' : edge);
		const other: Route = {
			balance: [{ weight: 2, to: e('b') }, { to: e('a') }],
			sticky: 'client_ip'
		};
		expect(signature(at(route, [0]) as Route, where)).toBe(signature(other, where));
		expect(mapLeaves(route, edge => e(edge.toUpperCase()))).toEqual({
			failover: [
				{ balance: [{ weight: 2, to: e('A') }, { to: e('B') }], sticky: 'client_ip' },
				e('C')
			]
		});
	});

	test('members nest into a group of their own, and back out', () => {
		const flat: Route = {
			balance: [{ weight: 2, to: e('a') }, { to: e('b') }, { weight: 3, to: e('c') }]
		};
		const nested = nest(flat, [], [0, 2], 'failover');
		expect(nested).toEqual({
			balance: [{ weight: 5, to: { failover: [e('a'), e('c')] } }, { to: e('b') }]
		});
		expect(unnest(nested, [0])).toEqual({
			balance: [{ weight: 5, to: e('a') }, { weight: 5, to: e('c') }, { to: e('b') }]
		});
		expect(nest(flat, [], [1, 0], 'balance')).toEqual({
			balance: [
				{ weight: 3, to: { balance: [{ weight: 2, to: e('a') }, { to: e('b') }] } },
				{ weight: 3, to: e('c') }
			]
		});
		// Every member nested is the group itself under another policy.
		expect(nest(flat, [], [0, 1, 2], 'failover')).toEqual({
			failover: [e('a'), e('b'), e('c')]
		});
		expect(unnest(route, [0])).toEqual({ failover: [e('a'), e('b'), e('c')] });
	});
});
