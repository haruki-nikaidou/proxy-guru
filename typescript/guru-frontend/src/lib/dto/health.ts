/**
 * The health dashboard's view of the recorded worker reports. Protobuf never
 * reaches the client: the numeric enums become string unions and every `int64`
 * counter is narrowed to a number in the remote function.
 *
 * A server's status is the same union the canvas uses, re-exported so a page
 * that only deals in health need not reach into the topology DTO.
 */
import type { ServerHealthStatusName } from './topology.js';

export type { ServerHealthStatusName };

export type PodHealthStatusName = 'unknown' | 'ready' | 'deploying' | 'failed';

/**
 * How far back the dashboard may look, in minutes: one hour, six hours, a day,
 * a week. `watchServerHealth` validates against this list and defaults to the
 * first entry.
 */
export const HEALTH_WINDOWS = [60, 360, 1440, 10080] as const;
export type HealthWindowMinutes = (typeof HEALTH_WINDOWS)[number];

/**
 * One report a worker uploaded. `uploadBytes` / `downloadBytes` /
 * `currentConnections` / `maxConnections` are protobuf `int64`, so they decode
 * as `bigint` (`forceLong=bigint`) and are narrowed to `number` by
 * `watchServerHealth`: the byte counters are per-report deltas, not lifetime
 * totals, so a double holds them exactly at any plausible interval.
 */
export type ServerHealthPoint = {
	id: string;
	/** RFC3339, as the control plane stores it. */
	reportTime: string;
	status: ServerHealthStatusName;
	uploadBytes: number;
	downloadBytes: number;
	currentConnections: number;
	maxConnections: number;
};

/** One deployment event of a pod; `message` carries the derive / apply error. */
export type PodHealthPoint = {
	id: string;
	/** RFC3339, as the control plane stores it. */
	reportTime: string;
	status: PodHealthStatusName;
	message: string;
};

/**
 * Everything the page draws for one server: its current status (from the server
 * row, not from the last record) plus the window's reports, oldest first so a
 * chart can plot them directly.
 */
export type ServerHealthSeries = {
	serverId: string;
	serverName: string;
	status: ServerHealthStatusName;
	points: ServerHealthPoint[];
};

/** One pod's deployment events as the health page lists them: newest first. */
export type PodEventFeed = { podId: string; podName: string; points: PodHealthPoint[] };
