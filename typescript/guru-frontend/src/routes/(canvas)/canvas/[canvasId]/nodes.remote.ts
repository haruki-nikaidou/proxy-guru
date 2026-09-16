/**
 * The nodes an operator draws on a canvas: creating them, renaming and moving
 * them, replacing a spec, retiring one. The boundary nodes — import and export
 * — live in `./subcanvas.remote.js`, because they change two canvases at once.
 */

import type { NodeSpec } from 'app-protobuf/orchestration/orchestration';
import {
	LoadBalanceMode,
	ProxyProtocolVersion,
	RelayProtocol
} from 'app-protobuf/orchestration/orchestration';
import * as v from 'valibot';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { fromBalanceMode, fromProxy, fromRelayProtocol } from '#lib/server/topology/enums.js';
import {
	balanceModeSchema,
	commentSchema,
	coordSchema,
	membersSchema,
	nameSchema,
	optionalIpSchema,
	portSchema,
	proxySchema,
	relayProtocolSchema,
	standaloneKindSchema,
	tlsSchema
} from '#lib/server/topology/schemas.js';
import { command } from '$app/server';
import { refreshAcrossBoundary, refreshSubcanvasViews } from './refresh.js';
import { getCanvasGraph } from './topology.remote.js';

/**
 * A new load-balance node starts with two members named `1` and `2`: the
 * operator renames them in the inspector and draws a bundle on each.
 */
const DEFAULT_MEMBERS = [
	{ slot: 1, name: '1' },
	{ slot: 2, name: '2' }
];

/**
 * Replaces one node's spec, then re-reads the canvas. Every command below is
 * this call with a different `spec`: the control plane validates the new shape,
 * reshapes the node's ports, and drops whatever no longer fits — so the graph
 * is stale either way and the reply carries nothing worth threading back.
 */
const replaceSpec = async (canvasId: string, nodeId: string, spec: NodeSpec) => {
	const metadata = sessionMetadata(requireSessionId());
	await callGrpc(() =>
		orchestrationClient().replaceNodeSpec({ nodeId, spec, itemCount: 0 }, { metadata })
	);
	await getCanvasGraph({ canvasId }).refresh();
	return { ok: true as const };
};

export const createStandaloneNode = command(
	v.object({
		canvasId: idSchema,
		kind: standaloneKindSchema,
		name: nameSchema,
		x: coordSchema,
		y: coordSchema
	}),
	async ({ canvasId, kind, name, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		// A half-drawn chain is deliberately storable (see `services/topology.rs`):
		// an exit with no destination yet is a warning in the problems panel, not a
		// write rejection, so the palette never invents a placeholder value.
		const spec =
			kind === 'entry'
				? { entry: { receiveProxyProtocol: ProxyProtocolVersion.UNSPECIFIED, tls: undefined } }
				: kind === 'relay'
					? {
							relay: {
								protocol: RelayProtocol.RELAY_TCP_RAW,
								overrideIpAddress: '',
								overridePort: 0
							}
						}
					: kind === 'exit'
						? { exit: { destination: '', passProxyProtocol: ProxyProtocolVersion.UNSPECIFIED } }
						: kind === 'load_balance_distribute'
							? {
									loadBalanceDistribute: {
										mode: LoadBalanceMode.ROUND_ROBIN,
										protocol: RelayProtocol.RELAY_TCP_RAW,
										members: DEFAULT_MEMBERS
									}
								}
							: { loadBalanceAggregate: { members: DEFAULT_MEMBERS } };

		await callGrpc(() =>
			orchestrationClient().createNode(
				{ canvasId, name, comment: '', spec, position: { x, y }, itemCount: 0 },
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

export const createPodNode = command(
	v.object({
		canvasId: idSchema,
		name: nameSchema,
		serverId: idSchema,
		port: portSchema,
		bindIp: optionalIpSchema,
		advertiseIp: optionalIpSchema,
		x: coordSchema,
		y: coordSchema
	}),
	async ({ canvasId, name, serverId, port, bindIp, advertiseIp, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().createNode(
				{
					canvasId,
					name,
					comment: '',
					spec: { pod: { serverId, port, bindIp, advertiseIp } },
					position: { x, y },
					itemCount: 0
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

export const updateNodeText = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		name: nameSchema,
		comment: commentSchema,
		/**
		 * Set for an export node: the parent labels its mirrored port with this
		 * name, so a rename here changes what the parent draws.
		 */
		boundary: v.optional(v.boolean(), false)
	}),
	async ({ canvasId, nodeId, name, comment, boundary }) => {
		const metadata = sessionMetadata(requireSessionId());
		// An unset `position` means "do not move".
		await callGrpc(() =>
			orchestrationClient().updateNodeMeta(
				{ nodeId, name, comment, position: undefined },
				{ metadata }
			)
		);
		if (boundary) await refreshAcrossBoundary(canvasId, metadata);
		else await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

/**
 * `UpdateNodeMeta` replaces name and comment wholesale, so a move has to resend
 * them. Deliberately does not refresh: the dragged position already matches —
 * except for an export node, whose `position.y` orders the mirrored ports on
 * the importing node, so the parent has to be re-read.
 */
export const moveNode = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		name: nameSchema,
		comment: commentSchema,
		x: coordSchema,
		y: coordSchema,
		boundary: v.optional(v.boolean(), false)
	}),
	async ({ canvasId, nodeId, name, comment, x, y, boundary }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().updateNodeMeta(
				{ nodeId, name, comment, position: { x, y } },
				{ metadata }
			)
		);
		if (boundary) await refreshAcrossBoundary(canvasId, metadata);
		return { ok: true as const };
	}
);

/**
 * A `null` `tls` drops TLS termination from the spec, which clears it. The
 * certificate itself is not touched: the derivation pass creates a certificate
 * row from this config, and `/tls` manages the result.
 */
export const replaceEntrySpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		receiveProxyProtocol: proxySchema,
		tls: tlsSchema
	}),
	({ canvasId, nodeId, receiveProxyProtocol, tls }) =>
		replaceSpec(canvasId, nodeId, {
			// `tls: undefined` is the wire form of "no TLS on this entry".
			entry: { receiveProxyProtocol: fromProxy(receiveProxyProtocol), tls: tls ?? undefined }
		})
);

export const replaceRelaySpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		protocol: relayProtocolSchema,
		overrideIpAddress: v.optional(v.pipe(v.string(), v.trim()), ''),
		overridePort: v.pipe(v.number(), v.integer(), v.minValue(0), v.maxValue(65535))
	}),
	({ canvasId, nodeId, protocol, overrideIpAddress, overridePort }) =>
		replaceSpec(canvasId, nodeId, {
			relay: { protocol: fromRelayProtocol(protocol), overrideIpAddress, overridePort }
		})
);

export const replaceExitSpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		destination: v.optional(v.pipe(v.string(), v.trim()), ''),
		passProxyProtocol: proxySchema
	}),
	({ canvasId, nodeId, destination, passProxyProtocol }) =>
		replaceSpec(canvasId, nodeId, {
			exit: { destination, passProxyProtocol: fromProxy(passProxyProtocol) }
		})
);

/**
 * A distribute node's mode and protocol apply to every channel at once. A
 * protocol change re-rolls the ports of every landing pod its channels reach,
 * since a listener cannot change protocol in place; the control plane does that
 * in the same write. The members are the operator's rule: a member keeps its
 * bundle as long as its slot stays in the list, whatever its name or place;
 * dropping a wired member is refused (`Conflict`) until its bundle is cut.
 */
export const replaceLoadBalanceSpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		mode: v.picklist(['distribute', 'aggregate'] as const),
		balanceMode: balanceModeSchema,
		protocol: v.optional(relayProtocolSchema, 'tcp_raw'),
		members: membersSchema
	}),
	({ canvasId, nodeId, mode, balanceMode, protocol, members }) =>
		// The spec kind cannot change, so `mode` only picks which config to resend.
		replaceSpec(
			canvasId,
			nodeId,
			mode === 'distribute'
				? {
						loadBalanceDistribute: {
							mode: fromBalanceMode(balanceMode),
							protocol: fromRelayProtocol(protocol),
							members
						}
					}
				: { loadBalanceAggregate: { members } }
		)
);

export const replacePodSpec = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		serverId: idSchema,
		port: portSchema,
		bindIp: optionalIpSchema,
		advertiseIp: optionalIpSchema
	}),
	({ canvasId, nodeId, serverId, port, bindIp, advertiseIp }) =>
		replaceSpec(canvasId, nodeId, { pod: { serverId, port, bindIp, advertiseIp } })
);

export const deleteNode = command(
	v.object({
		canvasId: idSchema,
		nodeId: idSchema,
		force: v.optional(v.boolean(), false),
		/**
		 * The canvas an import node embeds. Retiring it hands that canvas back to
		 * the root listing, which every listing and both trails have to be told
		 * about. The caller supplies it because the node is gone by the time the
		 * graph is re-read.
		 */
		subcanvasTarget: v.optional(v.string(), ''),
		/** Set for an export node: its mirrored port leaves the parent's graph. */
		boundary: v.optional(v.boolean(), false)
	}),
	async ({ canvasId, nodeId, force, subcanvasTarget, boundary }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			force
				? orchestrationClient().forceDeleteNode({ nodeId }, { metadata })
				: orchestrationClient().retireNode({ nodeId }, { metadata })
		);
		if (subcanvasTarget) await refreshSubcanvasViews(canvasId, subcanvasTarget);
		else if (boundary) await refreshAcrossBoundary(canvasId, metadata);
		else await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);
