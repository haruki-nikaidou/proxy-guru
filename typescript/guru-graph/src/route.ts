/**
 * Working with route trees. Every function here is pure and returns a new
 * route; a path names a node by the member indexes leading to it from the root.
 */

import type { Id, Route, Weighted } from './model.js';

export type Path = readonly number[];

export type RouteKind = 'edge' | 'balance' | 'failover';

export const kindOf = (route: Route): RouteKind =>
	'edge' in route ? 'edge' : 'balance' in route ? 'balance' : 'failover';

/** The members of a group, as routes, in a fresh array; empty for a leaf. */
export function children(route: Route): Route[] {
	if ('balance' in route) return route.balance.map(member => member.to);
	if ('failover' in route) return [...route.failover];
	return [];
}

/** The weight of each member of a balance, 1 for anything else. */
export function weightsOf(route: Route): number[] {
	if ('balance' in route) return route.balance.map(member => member.weight ?? 1);
	return children(route).map(() => 1);
}

/** The edges the route uses, in order. */
export function leaves(route: Route | null): Id[] {
	if (!route) return [];
	if ('edge' in route) return [route.edge];
	return children(route).flatMap(child => leaves(child));
}

/** The node at `path`, or `null` when the path leads nowhere. */
export function at(route: Route | null, path: Path): Route | null {
	let node: Route | null = route;
	for (const index of path) {
		if (!node) return null;
		node = children(node)[index] ?? null;
	}
	return node;
}

/** A group with its members replaced, keeping its policy. */
export function withChildren(route: Route, members: Route[], weights?: number[]): Route {
	if ('balance' in route) {
		const balance: Weighted[] = members.map((to, i) => {
			const weight = weights?.[i] ?? route.balance[i]?.weight ?? 1;
			return weight === 1 ? { to } : { weight, to };
		});
		return route.sticky ? { balance, sticky: route.sticky } : { balance };
	}
	if ('failover' in route) return { failover: members };
	return route;
}

/** The route with the node at `path` replaced; `null` removes it. */
export function replaceAt(route: Route, path: Path, next: Route | null): Route | null {
	const [head, ...rest] = path;
	if (head === undefined) return next;
	const members = children(route);
	const child = members[head];
	if (!child) return route;
	const replaced = replaceAt(child, rest, next);
	const weights = weightsOf(route);
	if (replaced === null) {
		members.splice(head, 1);
		weights.splice(head, 1);
	} else {
		members[head] = replaced;
	}
	return collapse(withChildren(route, members, weights));
}

/**
 * A group left with no members is gone, and one left with a single member is
 * that member: a policy over one choice chooses nothing.
 */
function collapse(route: Route): Route | null {
	const members = children(route);
	if (kindOf(route) === 'edge') return route;
	if (members.length === 0) return null;
	if (members.length === 1) return members[0] ?? null;
	return route;
}

/** The route without the given edges, groups collapsing as they empty out. */
export function removeEdges(route: Route | null, edges: ReadonlySet<Id>): Route | null {
	if (!route) return null;
	if ('edge' in route) return edges.has(route.edge) ? null : route;
	const members: Route[] = [];
	const weights: number[] = [];
	const original = weightsOf(route);
	for (const [i, child] of children(route).entries()) {
		const kept = removeEdges(child, edges);
		if (kept) {
			members.push(kept);
			weights.push(original[i] ?? 1);
		}
	}
	return collapse(withChildren(route, members, weights));
}

/**
 * `member` added to the route: a first route is just the member, a single edge
 * becomes a balance over the old and the new, and a group gains a member (a
 * balance one of weight 1, a failover its last tier).
 */
export function appendMember(route: Route | null, member: Route): Route {
	if (!route) return member;
	if ('edge' in route) return { balance: [{ to: route }, { to: member }] };
	if ('balance' in route) {
		return withChildren(route, [...children(route), member], [...weightsOf(route), 1]);
	}
	return { failover: [...route.failover, member] };
}

/** The route with every leaf replaced by what `leaf` makes of it. */
export function mapLeaves(route: Route, leaf: (edge: Id) => Route): Route {
	if ('edge' in route) return leaf(route.edge);
	return withChildren(
		route,
		children(route).map(child => mapLeaves(child, leaf)),
		weightsOf(route)
	);
}

/** Every node of the route with its path, parents first. */
export function nodes(route: Route | null): { path: number[]; node: Route }[] {
	if (!route) return [];
	const out: { path: number[]; node: Route }[] = [];
	const walk = (node: Route, path: number[]) => {
		out.push({ path, node });
		for (const [i, child] of children(node).entries()) walk(child, [...path, i]);
	};
	walk(route, []);
	return out;
}

/**
 * The route's shape with every leaf named by `leafKey`: two nodes with the same
 * signature balance or fail over the same way between the same places.
 */
export function signature(route: Route, leafKey: (edge: Id) => string): string {
	if ('edge' in route) return `(${leafKey(route.edge)})`;
	if ('balance' in route) {
		const members = route.balance
			.map(member => `${member.weight ?? 1}*${signature(member.to, leafKey)}`)
			.join(',');
		return `B${route.sticky ? '!' : ''}[${members}]`;
	}
	return `F[${route.failover.map(member => signature(member, leafKey)).join(',')}]`;
}

/** A group turned into another policy, keeping its members and their order. */
export function withPolicy(
	route: Route,
	policy: { kind: 'balance' | 'failover'; sticky?: boolean; weights?: number[] }
): Route {
	const members = children(route);
	if (members.length === 0) return route;
	if (policy.kind === 'failover') return { failover: members };
	const weights = policy.weights ?? weightsOf(route);
	const balance: Weighted[] = members.map((to, i) => {
		const weight = Math.max(1, Math.round(weights[i] ?? 1));
		return weight === 1 ? { to } : { weight, to };
	});
	return policy.sticky ? { balance, sticky: 'client_ip' } : { balance };
}

/** A group with its members put in the order `order` lists their indexes. */
export function reordered(route: Route, order: readonly number[]): Route {
	const members = children(route);
	const weights = weightsOf(route);
	if (order.length !== members.length) return route;
	return withChildren(
		route,
		order.map(i => members[i] as Route),
		order.map(i => weights[i] ?? 1)
	);
}

/**
 * The members at `indexes` of the group at `path`, wrapped into a new group of
 * `kind` that takes the place of the first of them. In a balance the new group
 * weighs what its members weighed together, and each member keeps its weight.
 */
export function nest(
	route: Route,
	path: Path,
	indexes: readonly number[],
	kind: 'balance' | 'failover'
): Route {
	const group = at(route, path);
	if (!group || 'edge' in group) return route;
	const members = children(group);
	const weights = weightsOf(group);
	const chosen = [...new Set(indexes)]
		.filter(i => i >= 0 && i < members.length)
		.sort((a, b) => a - b);
	if (chosen.length < 2) return route;
	const [first] = chosen;
	if (first === undefined) return route;
	const inner: Route =
		kind === 'failover'
			? { failover: chosen.map(i => members[i] as Route) }
			: {
					balance: chosen.map(i => {
						const weight = weights[i] ?? 1;
						const to = members[i] as Route;
						return weight === 1 ? { to } : { weight, to };
					})
				};
	const nextMembers: Route[] = [];
	const nextWeights: number[] = [];
	for (const [i, member] of members.entries()) {
		if (i === first) {
			nextMembers.push(inner);
			nextWeights.push(chosen.reduce((sum, j) => sum + (weights[j] ?? 1), 0));
		} else if (!chosen.includes(i)) {
			nextMembers.push(member);
			nextWeights.push(weights[i] ?? 1);
		}
	}
	const replaced = collapse(withChildren(group, nextMembers, nextWeights)) ?? inner;
	return replaceAt(route, path, replaced) ?? replaced;
}

/**
 * The group at `path` replaced in its parent by its own members. In a balance
 * parent, members of a balance keep their weights, and members of a failover
 * take the weight the group had.
 */
export function unnest(route: Route, path: Path): Route {
	if (path.length === 0) return route;
	const parentPath = path.slice(0, -1);
	const index = path[path.length - 1] as number;
	const parent = at(route, parentPath);
	const group = at(route, path);
	if (!parent || !group || 'edge' in parent || 'edge' in group) return route;
	const members = children(parent);
	const weights = weightsOf(parent);
	const own = children(group);
	const ownWeights = 'balance' in group ? weightsOf(group) : own.map(() => weights[index] ?? 1);
	const nextMembers = [...members.slice(0, index), ...own, ...members.slice(index + 1)];
	const nextWeights = [...weights.slice(0, index), ...ownWeights, ...weights.slice(index + 1)];
	const replaced = withChildren(parent, nextMembers, nextWeights);
	return replaceAt(route, parentPath, replaced) ?? replaced;
}
