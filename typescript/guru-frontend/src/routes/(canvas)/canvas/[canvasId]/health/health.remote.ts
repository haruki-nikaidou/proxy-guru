import type {
	PodHealthRecord as ProtoPodHealthRecord,
	ServerHealthRecord as ProtoServerHealthRecord
} from 'app-protobuf/orchestration/orchestration';
import { PodHealthStatus } from 'app-protobuf/orchestration/orchestration';
import * as v from 'valibot';
import type {
	PodHealthPoint,
	PodHealthStatusName,
	ServerHealthPoint,
	ServerHealthSeries
} from '#lib/dto/health.js';
import { HEALTH_WINDOWS } from '#lib/dto/health.js';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { toServerHealth } from '#lib/server/topology/enums.js';
import { query } from '$app/server';

// Health is readable by every role: the control plane is authoritative on
// permissions, so no role check happens here.
const windowSchema = v.optional(v.picklist(HEALTH_WINDOWS, 'health_window_invalid'), 60);

function toPodStatus(value: PodHealthStatus): PodHealthStatusName {
	switch (value) {
		case PodHealthStatus.POD_READY:
			return 'ready';
		case PodHealthStatus.POD_DEPLOYING:
			return 'deploying';
		case PodHealthStatus.POD_FAILED:
			return 'failed';
		default:
			return 'unknown';
	}
}

/** The four counters are `int64`, so they arrive as `bigint`. */
const toServerPoint = (record: ProtoServerHealthRecord): ServerHealthPoint => ({
	id: record.id,
	reportTime: record.reportTime,
	status: toServerHealth(record.status),
	uploadBytes: Number(record.uploadBytes),
	downloadBytes: Number(record.downloadBytes),
	currentConnections: Number(record.currentConnections),
	maxConnections: Number(record.maxConnections)
});

const toPodPoint = (record: ProtoPodHealthRecord): PodHealthPoint => ({
	id: record.id,
	reportTime: record.reportTime,
	status: toPodStatus(record.status),
	message: record.message
});

/**
 * Every server of the canvas with the reports it uploaded inside the window.
 * The status badge comes from the server row — a server that stopped reporting
 * is `offline` there while its series is empty. `GetGraph` answers the whole
 * tree; only the servers placed on this canvas are this page's.
 */
export const listServerHealth = query(
	v.object({ canvasId: idSchema, windowMinutes: windowSchema }),
	async ({ canvasId, windowMinutes }): Promise<ServerHealthSeries[]> => {
		const metadata = sessionMetadata(requireSessionId());
		const graph = await callGrpc(() => orchestrationClient().getGraph({ canvasId }, { metadata }));
		const servers = graph.servers.filter(server => server.canvasId === canvasId);

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
					status: toServerHealth(server.healthStatus),
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
export const listPodHealth = query(
	v.object({ podId: idSchema, windowMinutes: windowSchema }),
	async ({ podId, windowMinutes }): Promise<PodHealthPoint[]> => {
		const metadata = sessionMetadata(requireSessionId());
		const start = new Date(Date.now() - windowMinutes * 60_000).toISOString();
		// `limit` 0 keeps the control plane's own page size.
		const { records } = await callGrpc(() =>
			orchestrationClient().listPodHealthHistory({ podId, start, end: '', limit: 0 }, { metadata })
		);
		return records.map(toPodPoint);
	}
);
