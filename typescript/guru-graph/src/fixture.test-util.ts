/**
 * Graphs the tests share. Ids are readable names: nothing in the library cares
 * about their shape.
 */

import { applyChange } from './layout.js';
import type { Edge, Exit, Graph, GraphChange, Ingress, Pod, Route, Server } from './model.js';

export const server = (id: string, canvasId = 'root', x = 0, y = 0): Server => ({
	id,
	canvasId,
	name: id,
	x,
	y
});

export const pod = (
	id: string,
	serverId: string,
	ingress: Ingress['kind'],
	route: Route | null = null,
	canvasId = 'root'
): Pod => ({
	id,
	canvasId,
	serverId,
	name: id,
	comment: '',
	port: 1000,
	bindIp: null,
	advertiseIp: null,
	ingress:
		ingress === 'client_raw'
			? { kind: 'client_raw', receiveProxyProtocol: null }
			: ingress === 'client_tls'
				? {
						kind: 'client_tls',
						receiveProxyProtocol: null,
						tls: { sni: 'a.example.com', dnsProviderId: 'cf', domainId: 'z', acmeDirectory: '' }
					}
				: { kind: ingress },
	route
});

export const exit = (id: string, canvasId = 'root', x = 0, y = 0): Exit => ({
	id,
	canvasId,
	name: id,
	comment: '',
	destination: `${id}.example.com:443`,
	sendProxyProtocol: null,
	x,
	y
});

export const toPod = (id: string, source: string, target: string): Edge => ({
	id,
	sourcePodId: source,
	target: { pod: target },
	overrideIp: null,
	overridePort: null
});

export const toExit = (id: string, source: string, target: string): Edge => ({
	id,
	sourcePodId: source,
	target: { exit: target },
	overrideIp: null,
	overridePort: null
});

export const leaf = (edge: string): Route => ({ edge });

/**
 * The production shape: two client pods on `mobile` (rules `web` and `api`)
 * each balance over their own relay pod on five `gcore` servers; every relay
 * pod exits, `web`'s to `exit-a`, `api`'s to `exit-b`.
 */
export function fanOut(): Graph {
	const servers = [server('mobile', 'root', 0, 0)];
	const pods: Pod[] = [];
	const edges: Edge[] = [];
	for (let i = 1; i <= 5; i += 1) servers.push(server(`gcore${i}`, 'root', 600, i * 200));
	for (const [rule, target] of [
		['web', 'exit-a'],
		['api', 'exit-b']
	] as const) {
		const members: Route[] = [];
		for (let i = 1; i <= 5; i += 1) {
			const relay = `${rule}-g${i}`;
			edges.push(toPod(`${rule}>g${i}`, rule, relay));
			edges.push(toExit(`${relay}>out`, relay, target));
			pods.push(pod(relay, `gcore${i}`, 'relay_quic', leaf(`${relay}>out`)));
			members.push(leaf(`${rule}>g${i}`));
		}
		pods.push(pod(rule, 'mobile', 'client_raw', { balance: members.map(to => ({ to })) }));
	}
	return {
		canvases: [{ id: 'root', name: 'root', description: '', parentId: null, x: 0, y: 0 }],
		servers,
		pods,
		exits: [exit('exit-a', 'root', 1200, 200), exit('exit-b', 'root', 1200, 600)],
		edges,
		groups: [],
		generation: 1
	};
}

/** The graph after a change, applied the way the control plane would. */
export const applied = (graph: Graph, change: GraphChange): Graph => applyChange(graph, change);

/**
 * What the control plane checks about routes, enough for the tests: every pod's
 * route names exactly its own out-edges, each once, and every edge's ends exist.
 */
export function consistent(graph: Graph): string[] {
	const problems: string[] = [];
	const pods = new Map(graph.pods.map(p => [p.id, p]));
	const exits = new Set(graph.exits.map(e => e.id));
	const walk = (route: Route | null): string[] => {
		if (!route) return [];
		if ('edge' in route) return [route.edge];
		if ('balance' in route) {
			if (route.balance.length === 0) problems.push('empty balance');
			return route.balance.flatMap(m => walk(m.to));
		}
		if (route.failover.length === 0) problems.push('empty failover');
		return route.failover.flatMap(walk);
	};
	for (const p of graph.pods) {
		const named = walk(p.route).sort();
		const own = graph.edges
			.filter(e => e.sourcePodId === p.id)
			.map(e => e.id)
			.sort();
		if (named.join() !== own.join()) problems.push(`${p.id}: route ${named} vs edges ${own}`);
	}
	for (const e of graph.edges) {
		if (!pods.has(e.sourcePodId)) problems.push(`${e.id}: no source`);
		if ('pod' in e.target && !pods.has(e.target.pod)) problems.push(`${e.id}: no target pod`);
		if ('exit' in e.target && !exits.has(e.target.exit)) problems.push(`${e.id}: no target exit`);
	}
	return problems;
}
