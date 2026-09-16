/**
 * The server node: its row on the canvas, the agent that runs on it, and where
 * it stands in a rollout. Reads live in `./topology.remote.js`.
 */
import { Ipv6Resolve } from 'app-protobuf/orchestration/orchestration';
import * as v from 'valibot';
import type {
	AgentInstallDto,
	AgentReleaseDto,
	ServerConfigTomlDto,
	ServerRolloutDto
} from '#lib/dto/topology.js';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { toSnapshot } from '#lib/server/topology/decode.js';
import { fromIpv6, fromQuicCongestion } from '#lib/server/topology/enums.js';
import {
	agentUnitSchema,
	commentSchema,
	coordSchema,
	extraAddressesSchema,
	ipv6Schema,
	logLevelSchema,
	nameSchema,
	optionalIpSchema,
	serverQuicSchema
} from '#lib/server/topology/schemas.js';
import { command, query } from '$app/server';
import { getCanvasGraph } from './topology.remote.js';

export const createServerNode = command(
	v.object({ canvasId: idSchema, name: nameSchema, x: coordSchema, y: coordSchema }),
	async ({ canvasId, name, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().createServer(
				{
					canvasId,
					name,
					icon: '',
					comment: '',
					position: { x, y },
					ipv6Resolve: Ipv6Resolve.IPV6_TOLERATED,
					logLevel: 'info',
					overrideV4: '',
					overrideV6: '',
					extraAddresses: []
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

export const updateServerNode = command(
	v.object({
		canvasId: idSchema,
		serverId: idSchema,
		name: nameSchema,
		icon: v.optional(v.string(), ''),
		comment: commentSchema,
		ipv6Resolve: ipv6Schema,
		logLevel: logLevelSchema,
		overrideV4: optionalIpSchema,
		overrideV6: optionalIpSchema,
		extraAddresses: extraAddressesSchema,
		agentUnit: agentUnitSchema,
		quic: serverQuicSchema
	}),
	async ({
		canvasId,
		serverId,
		name,
		icon,
		comment,
		ipv6Resolve,
		logLevel,
		overrideV4,
		overrideV6,
		extraAddresses,
		agentUnit,
		quic
	}) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().updateServer(
				{
					serverId,
					name,
					icon,
					comment,
					ipv6Resolve: fromIpv6(ipv6Resolve),
					logLevel,
					overrideV4,
					overrideV6,
					extraAddresses,
					agentUnit,
					quic: {
						congestion: fromQuicCongestion(quic.congestion),
						upMbps: quic.upMbps,
						downMbps: quic.downMbps,
						streamReceiveWindow: BigInt(quic.streamReceiveWindow),
						connReceiveWindow: BigInt(quic.connReceiveWindow)
					}
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

/**
 * Issues the server's agent key and renders the install command carrying it.
 * The key is in the reply exactly once; issuing again replaces it.
 */
export const issueServerAgentInstall = command(
	v.object({ canvasId: idSchema, serverId: idSchema, unit: agentUnitSchema }),
	async ({ canvasId, serverId, unit }): Promise<AgentInstallDto> => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			orchestrationClient().issueServerAgentInstall({ serverId, unit }, { metadata })
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { command: reply.command, unit: reply.unit, version: reply.version };
	}
);

/** Asks the server's worker to move to the published release at its next poll. */
export const requestAgentUpdate = command(
	v.object({ canvasId: idSchema, serverId: idSchema }),
	async ({ canvasId, serverId }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() => orchestrationClient().requestAgentUpdate({ serverId }, { metadata }));
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

/** The published worker release, which the panel offers to servers running another one. */
export const getAgentRelease = query(async (): Promise<AgentReleaseDto> => {
	const metadata = sessionMetadata(requireSessionId());
	const reply = await callGrpc(() => orchestrationClient().getAgentRelease({}, { metadata }));
	return {
		version: reply.version,
		sha256: reply.sha256,
		arch: reply.arch,
		publishedAt: reply.publishedAt,
		baseUrlConfigured: reply.baseUrlConfigured
	};
});

/** Deliberately does not refresh: the dragged position already matches locally. */
export const moveServerNode = command(
	v.object({ canvasId: idSchema, serverId: idSchema, x: coordSchema, y: coordSchema }),
	async ({ serverId, x, y }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().moveServer({ serverId, position: { x, y } }, { metadata })
		);
		return { ok: true as const };
	}
);

export const deleteServerNode = command(
	v.object({
		canvasId: idSchema,
		serverId: idSchema,
		force: v.optional(v.boolean(), false)
	}),
	async ({ canvasId, serverId, force }) => {
		const metadata = sessionMetadata(requireSessionId());
		// `DeleteServer` refuses while any pod is still placed on it, so the pods go
		// first. The list is re-read here rather than taken from the client: a pod
		// added since the last refresh would otherwise block the delete.
		const detail = await callGrpc(() =>
			orchestrationClient().getCanvas({ canvasId }, { metadata })
		);
		for (const node of detail.nodes) {
			const pod = node.spec?.pod;
			// A landing lane is not retirable by hand and the universal pod goes
			// with the server: both are left to `DeleteServer`, which refuses with
			// the channels still landing here.
			if (!pod || pod.serverId !== serverId || node.lane) continue;
			const nodeId = node.id;
			await callGrpc(() =>
				force
					? orchestrationClient().forceDeleteNode({ nodeId }, { metadata })
					: orchestrationClient().retireNode({ nodeId }, { metadata })
			);
		}
		await callGrpc(() => orchestrationClient().deleteServer({ serverId }, { metadata }));
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

/**
 * Where one server stands between the config the control plane derived and the
 * config its worker confirmed. Read by the server panel while it is open.
 */
export const getServerRollout = query(
	v.object({ serverId: idSchema }),
	async ({ serverId }): Promise<ServerRolloutDto> => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			orchestrationClient().getServerRolloutStatus({ serverId }, { metadata })
		);
		return {
			desired: toSnapshot(reply.desired),
			inFlight: toSnapshot(reply.inFlight),
			applied: toSnapshot(reply.applied),
			applyError: reply.applyError,
			deriveError: reply.deriveError,
			waitingForServerIds: [...reply.waitingForServerIds],
			derivationPending: reply.derivationPending,
			lastSeenAt: reply.lastSeenAt,
			invalidPods: reply.invalidPods.map(pod => ({
				nodeId: pod.nodeId,
				podName: pod.podName,
				listen: pod.listen,
				error: pod.error
			}))
		};
	}
);

/** The worker TOML as rendered for this server, fetched only when asked for. */
export const getServerConfigToml = query(
	v.object({ serverId: idSchema }),
	async ({ serverId }): Promise<ServerConfigTomlDto> => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			orchestrationClient().getServerConfig({ serverId }, { metadata })
		);
		return { revision: Number(reply.revision), toml: reply.toml };
	}
);

/**
 * Admin only, destructive: declares the server dead so its dependants may
 * switch away from listeners it might still be serving. The graph goes stale
 * with the rollout, because forgetting re-derives every dependant.
 */
export const forgetServerApplied = command(
	v.object({ canvasId: idSchema, serverId: idSchema }),
	async ({ canvasId, serverId }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() => orchestrationClient().forgetServerApplied({ serverId }, { metadata }));
		await Promise.all([
			getServerRollout({ serverId }).refresh(),
			getCanvasGraph({ canvasId }).refresh()
		]);
		return { ok: true as const };
	}
);
