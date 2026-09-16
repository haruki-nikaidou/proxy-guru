import type { UniversalGroupName } from '#lib/dto/topology.js';

/**
 * The id spaces the flow mirror runs on. Every other module here builds on
 * these, and nothing here builds on anything: servers and nodes share one id
 * space in Svelte Flow but not in the backend, and a bundle-capable node's
 * "add" handles need ids no port can collide with.
 */

/**
 * A bundle-capable node's "add" handles are Svelte Flow handles of their own,
 * with ids no port can collide with: one per group, taking a new connection
 * each time. The ports the control plane creates behind them are ordinary
 * one-edge handles of their own.
 */
export const groupHandleId = (flowId: string, group: UniversalGroupName): string =>
	`u:${flowId}:${group}`;

export function parseGroupHandle(
	handle: string
): { flowId: string; group: UniversalGroupName } | null {
	if (!handle.startsWith('u:')) return null;
	const separator = handle.lastIndexOf(':');
	const group = handle.slice(separator + 1);
	if (group !== 'channel_out' && group !== 'bundle_in') return null;
	return { flowId: handle.slice(2, separator), group };
}

/** Servers and nodes share one id space in Svelte Flow but not in the backend. */
export const flowNodeId = (kind: 'server' | 'node', id: string): string => `${kind}:${id}`;

export function parseFlowNodeId(flowId: string): { kind: 'server' | 'node'; id: string } {
	const separator = flowId.indexOf(':');
	const prefix = flowId.slice(0, separator);
	return { kind: prefix === 'server' ? 'server' : 'node', id: flowId.slice(separator + 1) };
}
