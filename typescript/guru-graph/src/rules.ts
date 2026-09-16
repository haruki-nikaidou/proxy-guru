/**
 * Rules: what the dashboard colours traffic by. A rule is a client pod — where
 * traffic enters the fabric — and everything that traffic can pass through
 * carries it: the pod's out-edges, the relay pods they dial, their edges, and so
 * on down to the exits.
 */

import { edgeTargetExit, edgeTargetPod, type Graph, type Id, isRelay } from './model.js';

/** How many colours the dashboard's palette has. */
export const PALETTE_SIZE = 12;

export type RuleIndex = {
	/** The client pods, by name. */
	rules: Id[];
	/** Rules reaching each pod, by name of the rule. */
	pods: Map<Id, Id[]>;
	/** Rules each edge carries. */
	edges: Map<Id, Id[]>;
	/** Rules reaching each exit. */
	exits: Map<Id, Id[]>;
	/** The palette slot of each rule. */
	colors: Map<Id, number>;
};

export function ruleIndex(graph: Graph): RuleIndex {
	const podById = new Map(graph.pods.map(pod => [pod.id, pod]));
	const outOf = new Map<Id, Id[]>();
	const indegree = new Map<Id, number>();
	for (const pod of graph.pods) {
		outOf.set(pod.id, []);
		indegree.set(pod.id, 0);
	}
	for (const edge of graph.edges) {
		outOf.get(edge.sourcePodId)?.push(edge.id);
		const target = edgeTargetPod(edge);
		if (target !== null && indegree.has(target)) {
			indegree.set(target, (indegree.get(target) ?? 0) + 1);
		}
	}
	const edgeById = new Map(graph.edges.map(edge => [edge.id, edge]));
	const reaching = new Map<Id, Set<Id>>();
	for (const pod of graph.pods) {
		reaching.set(pod.id, new Set(isRelay(pod.ingress) ? [] : [pod.id]));
	}

	// Kahn's order; whatever a cycle leaves over (a graph the control plane
	// would refuse) is walked once in id order, so this always ends.
	const order: Id[] = [];
	const ready = graph.pods.filter(pod => (indegree.get(pod.id) ?? 0) === 0).map(pod => pod.id);
	ready.sort();
	const done = new Set<Id>();
	while (ready.length > 0) {
		const id = ready.shift() as Id;
		order.push(id);
		done.add(id);
		for (const edgeId of outOf.get(id) ?? []) {
			const edge = edgeById.get(edgeId);
			const target = edge ? edgeTargetPod(edge) : null;
			if (target === null || !indegree.has(target)) continue;
			const left = (indegree.get(target) ?? 0) - 1;
			indegree.set(target, left);
			if (left === 0) ready.push(target);
		}
	}
	for (const pod of [...graph.pods].sort((a, b) => a.id.localeCompare(b.id))) {
		if (!done.has(pod.id)) order.push(pod.id);
	}

	const edges = new Map<Id, Id[]>();
	const exits = new Map<Id, Set<Id>>();
	for (const id of order) {
		const rules = reaching.get(id) ?? new Set<Id>();
		for (const edgeId of outOf.get(id) ?? []) {
			const edge = edgeById.get(edgeId);
			if (!edge) continue;
			edges.set(edgeId, [...rules]);
			const pod = edgeTargetPod(edge);
			if (pod !== null) {
				const into = reaching.get(pod);
				if (into) for (const rule of rules) into.add(rule);
			}
			const exit = edgeTargetExit(edge);
			if (exit !== null) {
				const into = exits.get(exit) ?? new Set<Id>();
				for (const rule of rules) into.add(rule);
				exits.set(exit, into);
			}
		}
	}

	const clients = graph.pods.filter(pod => !isRelay(pod.ingress));
	const nameOf = (id: Id) => podById.get(id)?.name ?? id;
	const byName = (a: Id, b: Id) => nameOf(a).localeCompare(nameOf(b)) || a.localeCompare(b);
	const sorted = (set: Iterable<Id>) => [...set].sort(byName);

	return {
		rules: sorted(clients.map(pod => pod.id)),
		pods: new Map([...reaching].map(([id, set]) => [id, sorted(set)])),
		edges: new Map([...edges].map(([id, list]) => [id, sorted(list)])),
		exits: new Map([...exits].map(([id, set]) => [id, sorted(set)])),
		colors: ruleColors(
			graph,
			clients.map(pod => pod.id)
		)
	};
}

/**
 * The palette slot of each rule: the one a `rule` group pinned for it
 * (`props.color`), else one taken from the rule's id — so it does not move when
 * other rules come and go — moved on to the next free slot while the palette
 * still has one, so two rules of a tree never share a colour by chance.
 */
function ruleColors(graph: Graph, rules: Id[]): Map<Id, number> {
	const pinned = new Map<Id, number>();
	for (const group of graph.groups) {
		if (group.kind !== 'rule') continue;
		const color = group.props.color;
		if (typeof color !== 'number' || !Number.isInteger(color)) continue;
		for (const member of group.members) {
			if ('pod' in member)
				pinned.set(member.pod, ((color % PALETTE_SIZE) + PALETTE_SIZE) % PALETTE_SIZE);
		}
	}
	const colors = new Map<Id, number>();
	const taken = new Set<number>();
	for (const rule of rules) {
		const slot = pinned.get(rule);
		if (slot === undefined) continue;
		colors.set(rule, slot);
		taken.add(slot);
	}
	for (const rule of [...rules].sort()) {
		if (colors.has(rule)) continue;
		const slot = freeSlot(slotOf(rule), taken);
		colors.set(rule, slot);
		taken.add(slot);
	}
	return colors;
}

/**
 * The slot for a rule that hashes to `wanted`: the free slot farthest in hue
 * from every slot taken — neighbouring slots are neighbouring hues, too alike to
 * tell two rules apart — and `wanted` itself whenever it is as far as any.
 */
function freeSlot(wanted: number, taken: Set<number>): number {
	const distanceOf = (slot: number) => {
		let distance = PALETTE_SIZE;
		for (const other of taken) {
			const apart = Math.abs(slot - other);
			distance = Math.min(distance, apart, PALETTE_SIZE - apart);
		}
		return distance;
	};
	let best = { slot: wanted, distance: taken.has(wanted) ? -1 : distanceOf(wanted) };
	for (let step = 1; step < PALETTE_SIZE; step += 1) {
		const slot = (wanted + step) % PALETTE_SIZE;
		if (taken.has(slot)) continue;
		const distance = distanceOf(slot);
		if (distance > best.distance) best = { slot, distance };
	}
	return best.slot;
}

function slotOf(id: Id): number {
	let hash = 0;
	for (let i = 0; i < id.length; i += 1) hash = (hash * 31 + id.charCodeAt(i)) >>> 0;
	return hash % PALETTE_SIZE;
}
