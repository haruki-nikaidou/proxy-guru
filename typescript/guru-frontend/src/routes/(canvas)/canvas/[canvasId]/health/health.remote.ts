/**
 * The health page's two live views: every server of a canvas with its reports,
 * and every pod of a server with its deployment events. Each is one SvelteKit
 * live query fed by one `WatchGraph` stream (which servers or pods exist) plus
 * one record stream per member, so a page holds one connection to this server
 * whatever the fleet's size.
 */
import type {
	GetGraphReply,
	GraphEvent,
	PodHealthEvent,
	PodHealthRecord as ProtoPodHealthRecord,
	ServerHealthRecord as ProtoServerHealthRecord,
	ServerHealthEvent
} from 'app-protobuf/orchestration/orchestration';
import { PodHealthStatus } from 'app-protobuf/orchestration/orchestration';
import { type CallOptions, ClientError, Status } from 'nice-grpc';
import * as v from 'valibot';
import type {
	PodEventFeed,
	PodHealthPoint,
	PodHealthStatusName,
	ServerHealthPoint,
	ServerHealthSeries,
	ServerHealthStatusName
} from '#lib/dto/health.js';
import { HEALTH_WINDOWS } from '#lib/dto/health.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { GrpcStreams, liveSessionId, streamFailure } from '#lib/server/live.js';
import { idSchema } from '#lib/server/schemas.js';
import { sessionMetadata } from '#lib/server/session.js';
import { toServerHealth } from '#lib/server/topology/enums.js';
import { getRequestEvent, query } from '$app/server';

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

type TimedPoint = { id: string; reportTime: string };

/** One member of a feed as the graph lists it. */
type Member = { id: string; name: string; status: ServerHealthStatusName };

/** One member with the records inside the window, in the feed's order. */
type Feed<P> = Member & { points: P[] };

type FeedSpec<E, P extends TimedPoint> = {
	/** This page's members of a tree snapshot. */
	members: (graph: GetGraphReply) => Member[];
	/** Opens one member's record stream from `since` (RFC 3339). */
	watch: (id: string, since: string, options: CallOptions) => AsyncIterable<E>;
	/**
	 * What a record-stream event carries; `undefined` for a keep-alive. A
	 * snapshot may also name the member's current status.
	 */
	unpack: (event: E) => { points: P[]; status?: ServerHealthStatusName } | undefined;
	newestFirst: boolean;
};

/**
 * The live feeds of one page: a graph stream names the members, and each
 * member's records arrive on their own stream, opened from the newest record
 * this generator has seen so a reopen only fills the gap. Yields once the graph
 * and every member's opening snapshot are in, then after every change; a
 * member the graph stops listing is dropped, as is one whose stream answers
 * `NOT_FOUND` (deleted; the next graph snapshot agrees).
 */
async function* liveFeeds<E, P extends TimedPoint>(
	canvasId: string,
	windowMinutes: number,
	spec: FeedSpec<E, P>
): AsyncGenerator<Feed<P>[]> {
	const sessionId = liveSessionId();
	if (!sessionId) return;
	const metadata = sessionMetadata(sessionId);
	const streams = new GrpcStreams<GraphEvent | E>(getRequestEvent().request.signal);
	const feeds = new Map<string, Feed<P> & { last: string }>();
	const pending = new Set<string>();
	let graphSeen = false;
	const windowStart = () => Date.now() - windowMinutes * 60_000;

	const merge = (feed: Feed<P> & { last: string }, incoming: P[]) => {
		const replaced = new Set(incoming.map(point => point.id));
		const floor = windowStart();
		const points = feed.points
			.filter(point => !replaced.has(point.id))
			.concat(incoming)
			.filter(point => Date.parse(point.reportTime) >= floor)
			.sort((a, b) => Date.parse(a.reportTime) - Date.parse(b.reportTime));
		feed.last = points.at(-1)?.reportTime ?? feed.last;
		feed.points = spec.newestFirst ? points.reverse() : points;
	};

	const onGraph = (graph: GetGraphReply) => {
		const listed = new Set<string>();
		for (const member of spec.members(graph)) {
			listed.add(member.id);
			const feed = feeds.get(member.id);
			if (feed) {
				feed.name = member.name;
				feed.status = member.status;
				continue;
			}
			const fresh = { ...member, points: [] as P[], last: '' };
			feeds.set(member.id, fresh);
			pending.add(member.id);
			// `last` is read at open time, so a reopen after a transport loss
			// only fills the gap.
			streams.open(`member:${member.id}`, signal =>
				spec.watch(member.id, fresh.last || new Date(windowStart()).toISOString(), {
					metadata,
					signal
				})
			);
		}
		for (const id of [...feeds.keys()]) {
			if (listed.has(id)) continue;
			streams.close(`member:${id}`);
			feeds.delete(id);
			pending.delete(id);
		}
		graphSeen = true;
	};

	streams.open('graph', signal =>
		orchestrationClient().watchGraph({ canvasId }, { metadata, signal })
	);
	let yielded = false;
	try {
		for await (const item of streams) {
			if (item.key === 'graph') {
				if ('error' in item) {
					if (streamFailure(item.error, yielded) === 'end') return;
					continue;
				}
				const event = item.event as GraphEvent;
				if (!event.snapshot) continue;
				onGraph(event.snapshot);
			} else {
				const id = item.key.slice('member:'.length);
				const feed = feeds.get(id);
				if (!feed) continue;
				if ('error' in item) {
					// A deleted member: the next graph snapshot agrees.
					const gone = item.error instanceof ClientError && item.error.code === Status.NOT_FOUND;
					if (!gone && streamFailure(item.error, yielded) === 'end') return;
					streams.close(item.key);
					feeds.delete(id);
					pending.delete(id);
				} else {
					const unpacked = spec.unpack(item.event as E);
					if (!unpacked) continue;
					merge(feed, unpacked.points);
					if (unpacked.status) feed.status = unpacked.status;
					pending.delete(id);
				}
			}
			if (!graphSeen || pending.size > 0) continue;
			yield [...feeds.values()]
				.map(({ last: _, ...feed }) => feed)
				.sort((a, b) => a.name.localeCompare(b.name));
			yielded = true;
		}
	} finally {
		streams.closeAll();
	}
}

/**
 * Every server of the canvas with the reports it uploaded inside the window,
 * kept current: a point per report, and the status the server row carries — a
 * server that stopped reporting is `offline` there while its series is empty.
 * `WatchGraph` answers the whole tree; only the servers placed on this canvas
 * are this page's.
 */
export const watchServerHealth = query.live(
	v.object({ canvasId: idSchema, windowMinutes: windowSchema }),
	async function* ({ canvasId, windowMinutes }): AsyncGenerator<ServerHealthSeries[]> {
		const feeds = liveFeeds<ServerHealthEvent, ServerHealthPoint>(canvasId, windowMinutes, {
			members: graph =>
				graph.servers
					.filter(server => server.canvasId === canvasId)
					.map(server => ({
						id: server.id,
						name: server.name,
						status: toServerHealth(server.healthStatus)
					})),
			watch: (serverId, since, options) =>
				orchestrationClient().watchServerHealth({ serverId, since }, options),
			unpack: event => {
				if (event.snapshot) {
					return {
						points: event.snapshot.records.map(toServerPoint),
						status: toServerHealth(event.snapshot.status)
					};
				}
				// The server row's status is always its newest record's.
				if (event.record) {
					const point = toServerPoint(event.record);
					return { points: [point], status: point.status };
				}
				return undefined;
			},
			newestFirst: false
		});
		for await (const series of feeds) {
			yield series.map(feed => ({
				serverId: feed.id,
				serverName: feed.name,
				status: feed.status,
				points: feed.points
			}));
		}
	}
);

/**
 * The deployment events of every pod running on one server, newest first,
 * kept current. Every pod of the server is listed, whichever canvas of the tree
 * it is drawn on. Read only while a server's event feed is open, so the page
 * does not fan out one stream per pod on first paint.
 */
export const watchPodEvents = query.live(
	v.object({ canvasId: idSchema, serverId: idSchema, windowMinutes: windowSchema }),
	async function* ({ canvasId, serverId, windowMinutes }): AsyncGenerator<PodEventFeed[]> {
		const feeds = liveFeeds<PodHealthEvent, PodHealthPoint>(canvasId, windowMinutes, {
			members: graph =>
				graph.pods
					.filter(pod => pod.serverId === serverId)
					.map(pod => ({ id: pod.id, name: pod.name, status: 'unknown' })),
			watch: (podId, since, options) =>
				orchestrationClient().watchPodHealth({ podId, since }, options),
			unpack: event => {
				if (event.snapshot) return { points: event.snapshot.records.map(toPodPoint) };
				if (event.record) return { points: [toPodPoint(event.record)] };
				return undefined;
			},
			newestFirst: true
		});
		for await (const pods of feeds) {
			yield pods.map(feed => ({ podId: feed.id, podName: feed.name, points: feed.points }));
		}
	}
);
