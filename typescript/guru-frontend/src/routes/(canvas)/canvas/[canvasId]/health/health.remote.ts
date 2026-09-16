import type {
	NodeHealthRecord as ProtoNodeHealthRecord,
	ServerHealthRecord as ProtoServerHealthRecord
} from 'app-protobuf/orchestration/orchestration';
import { NodeHealthStatus, ServerHealthStatus } from 'app-protobuf/orchestration/orchestration';
import * as v from 'valibot';
import type {
	NodeHealthPoint,
	NodeHealthStatusName,
	ServerHealthPoint,
	ServerHealthSeries,
	ServerHealthStatusName
} from '#lib/dto/health.js';
import { HEALTH_WINDOWS } from '#lib/dto/health.js';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { query } from '$app/server';

// Health is readable by every role: the control plane is authoritative on
// permissions, so no role check happens here.
const windowSchema = v.optional(v.picklist(HEALTH_WINDOWS, 'health_window_invalid'), 60);

/** Every unknown / UNSPECIFIED value reads as `unknown`, never as a status. */
function toServerStatus(value: ServerHealthStatus): ServerHealthStatusName {
	switch (value) {
		case ServerHealthStatus.SERVER_ONLINE:
			return 'online';
		case ServerHealthStatus.SERVER_DEGRADED:
			return 'degraded';
		case ServerHealthStatus.SERVER_OFFLINE:
			return 'offline';
		default:
			return 'unknown';
	}
}

function toNodeStatus(value: NodeHealthStatus): NodeHealthStatusName {
	switch (value) {
		case NodeHealthStatus.NODE_READY:
			return 'ready';
		case NodeHealthStatus.NODE_DEPLOYING:
			return 'deploying';
		case NodeHealthStatus.NODE_FAILED:
			return 'failed';
		default:
			return 'unknown';
	}
}

/** The four counters are `int64`, so they arrive as `bigint`. */
const toServerPoint = (record: ProtoServerHealthRecord): ServerHealthPoint => ({
	id: record.id,
	reportTime: record.reportTime,
	status: toServerStatus(record.status),
	uploadBytes: Number(record.uploadBytes),
	downloadBytes: Number(record.downloadBytes),
	currentConnections: Number(record.currentConnections),
	maxConnections: Number(record.maxConnections)
});

const toNodePoint = (record: ProtoNodeHealthRecord): NodeHealthPoint => ({
	id: record.id,
	reportTime: record.reportTime,
	status: toNodeStatus(record.status),
	message: record.message
});

/**
 * Every server of the canvas with the reports it uploaded inside the window.
 * The status badge comes from the server row — a server that stopped reporting
 * is `offline` there while its series is empty.
 */
export const listServerHealth = query(
	v.object({ canvasId: idSchema, windowMinutes: windowSchema }),
	async ({ canvasId, windowMinutes }): Promise<ServerHealthSeries[]> => {
		const metadata = sessionMetadata(requireSessionId());
		const { servers } = await callGrpc(() =>
			orchestrationClient().getCanvas({ canvasId }, { metadata })
		);

		// An empty `end` means now, so only the lower bound is sent.
		const start = new Date(Date.now() - windowMinutes * 60_000).toISOString();
		const series = await Promise.all(
			servers.map(async server => {
				const { records } = await callGrpc(() =>
					orchestrationClient().listServerHealthHistory(
						{ serverId: server.id, start, end: '' },
						{ metadata }
					)
				);
				return {
					serverId: server.id,
					serverName: server.name,
					status: toServerStatus(server.healthStatus),
					// The control plane answers newest first; a chart plots along time.
					points: records.map(toServerPoint).reverse()
				};
			})
		);

		return series.sort((a, b) => a.serverName.localeCompare(b.serverName));
	}
);

/**
 * The deployment events of one pod, newest first as the control plane orders
 * them. Fetched only when a server's event feed is opened, so the page does not
 * fan out one call per pod on first paint.
 */
export const listNodeHealth = query(
	v.object({ nodeId: idSchema, windowMinutes: windowSchema }),
	async ({ nodeId, windowMinutes }): Promise<NodeHealthPoint[]> => {
		const metadata = sessionMetadata(requireSessionId());
		const start = new Date(Date.now() - windowMinutes * 60_000).toISOString();
		// `limit` 0 keeps the control plane's own page size.
		const { records } = await callGrpc(() =>
			orchestrationClient().listNodeHealthHistory(
				{ nodeId, start, end: '', limit: 0 },
				{ metadata }
			)
		);
		return records.map(toNodePoint);
	}
);
