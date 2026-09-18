/**
 * What the canvas editor's remote functions accept. The server schemas mirror
 * rules the control plane enforces, so a doomed write fails in the browser
 * instead of round-tripping; a graph change is only checked for its shape here,
 * because what it means is the control plane's to judge (`ApplyGraph` answers
 * with diagnostics). The messages are codes, resolved by `#lib/i18n/codes.js`.
 */
import type { Route } from 'guru-graph';
import * as v from 'valibot';
import { SNI_PATTERN } from '../../tls.js';
import { idSchema } from '../schemas.js';

export const nameSchema = v.pipe(
	v.string(),
	v.trim(),
	v.minLength(1, 'node_name_required'),
	v.maxLength(128, 'node_name_too_long')
);
export const commentSchema = v.optional(
	v.pipe(v.string(), v.trim(), v.maxLength(1000, 'node_comment_too_long')),
	''
);
export const coordSchema = v.pipe(v.number(), v.integer(), v.safeInteger());
/** A pod's port; 0 asks the control plane for a free one. */
export const podPortSchema = v.pipe(
	v.number(),
	v.integer(),
	v.minValue(0, 'port_out_of_range'),
	v.maxValue(65535, 'port_out_of_range')
);
/** An optional IP literal: empty means unset. */
export const optionalIpSchema = v.optional(
	v.pipe(
		v.string(),
		v.trim(),
		v.check(
			value => value === '' || v.safeParse(v.pipe(v.string(), v.ip()), value).success,
			'ip_invalid'
		)
	),
	''
);
export const extraAddressesSchema = v.optional(
	v.array(v.pipe(v.string(), v.trim(), v.ip('ip_invalid'))),
	[]
);
export const logLevelSchema = v.picklist(['trace', 'debug', 'info', 'warn', 'error'] as const);
/** `guru-worker@<unit>`: what the control plane accepts, or empty for none. */
export const agentUnitSchema = v.optional(
	v.pipe(v.string(), v.trim(), v.regex(/^(?:[a-z0-9][a-z0-9-]{0,31})?$/, 'agent_unit_invalid')),
	''
);
export const ipv6Schema = v.picklist(['required', 'preferred', 'tolerated', 'forbidden'] as const);
export const ipFamilySchema = v.picklist(['auto', 'v4', 'v6'] as const);
export const quicCongestionSchema = v.picklist(['cubic', 'brutal'] as const);
/** A rate in Mbit/s; 0 is "unknown". */
export const mbpsSchema = v.pipe(v.number(), v.integer(), v.minValue(0), v.maxValue(1_000_000));
/** A window in bytes; 0 is "derived / unlimited". */
export const windowBytesSchema = v.pipe(
	v.number(),
	v.integer(),
	v.minValue(0),
	v.maxValue(Number.MAX_SAFE_INTEGER)
);
export const serverQuicSchema = v.object({
	congestion: quicCongestionSchema,
	upMbps: mbpsSchema,
	downMbps: mbpsSchema,
	streamReceiveWindow: windowBytesSchema,
	connReceiveWindow: windowBytesSchema
});

/** Every field is required except `acmeDirectory`, where empty means the default. */
const tlsSchema = v.object({
	sni: v.pipe(
		v.string(),
		v.trim(),
		v.minLength(1, 'sni_required'),
		v.maxLength(253, 'sni_too_long'),
		v.regex(SNI_PATTERN, 'sni_invalid')
	),
	dnsProviderId: v.pipe(v.string(), v.trim(), v.minLength(1, 'dns_provider_required')),
	domainId: v.pipe(v.string(), v.trim(), v.minLength(1, 'domain_id_required')),
	acmeDirectory: v.pipe(
		v.string(),
		v.trim(),
		v.check(value => value === '' || /^https:\/\/[^\s]+$/.test(value), 'acme_directory_invalid')
	)
});

const proxyVersionSchema = v.nullable(v.picklist(['v1', 'v2'] as const));
const optionalAddressSchema = v.nullable(v.pipe(v.string(), v.trim(), v.maxLength(253)));

const ingressSchema = v.variant('kind', [
	v.object({ kind: v.literal('client_raw'), receiveProxyProtocol: proxyVersionSchema }),
	v.object({
		kind: v.literal('client_tls'),
		receiveProxyProtocol: proxyVersionSchema,
		tls: tlsSchema
	}),
	v.object({ kind: v.literal('relay_tcp') }),
	v.object({ kind: v.literal('relay_tls') }),
	v.object({ kind: v.literal('relay_quic') })
]);

/** How deep a route may nest, as `guru_topology::MAX_ROUTE_DEPTH` says. */
const MAX_ROUTE_DEPTH = 32;
const routeDepth = (route: Route): number =>
	'edge' in route
		? 1
		: 1 +
			Math.max(
				0,
				...('balance' in route
					? route.balance.map(member => routeDepth(member.to))
					: route.failover.map(routeDepth))
			);

const routeSchema: v.GenericSchema<Route> = v.lazy(() =>
	v.union([
		v.strictObject({ edge: idSchema }),
		v.strictObject({
			balance: v.array(
				v.strictObject({
					weight: v.optional(v.pipe(v.number(), v.integer(), v.minValue(1))),
					to: routeSchema
				})
			),
			sticky: v.optional(v.literal('client_ip'))
		}),
		v.strictObject({ failover: v.array(routeSchema) })
	])
);

const podSchema = v.object({
	id: idSchema,
	canvasId: idSchema,
	serverId: idSchema,
	name: nameSchema,
	comment: commentSchema,
	port: podPortSchema,
	bindIp: optionalAddressSchema,
	advertiseIp: optionalAddressSchema,
	ingress: ingressSchema,
	route: v.nullable(
		v.pipe(
			routeSchema,
			v.check(route => routeDepth(route) <= MAX_ROUTE_DEPTH)
		)
	)
});

const exitSchema = v.object({
	id: idSchema,
	canvasId: idSchema,
	name: nameSchema,
	comment: commentSchema,
	destination: v.pipe(v.string(), v.trim(), v.maxLength(512)),
	sendProxyProtocol: proxyVersionSchema,
	x: coordSchema,
	y: coordSchema
});

const edgeSchema = v.object({
	id: idSchema,
	sourcePodId: idSchema,
	target: v.union([v.strictObject({ pod: idSchema }), v.strictObject({ exit: idSchema })]),
	overrideIp: optionalAddressSchema,
	overridePort: v.nullable(v.pipe(v.number(), v.integer(), v.minValue(1), v.maxValue(65535))),
	// Required: a page built before the field existed is refused rather than
	// allowed to put its edges back on auto.
	ipFamily: ipFamilySchema
});

const groupSchema = v.object({
	id: idSchema,
	canvasId: idSchema,
	kind: v.pipe(v.string(), v.minLength(1), v.maxLength(64)),
	name: v.pipe(v.string(), v.maxLength(128)),
	props: v.record(v.string(), v.unknown()),
	members: v.array(
		v.union([
			v.strictObject({ pod: idSchema }),
			v.strictObject({ edge: idSchema }),
			v.strictObject({ exit: idSchema }),
			v.strictObject({ server: idSchema })
		])
	)
});

export const graphChangeSchema = v.object({
	putPods: v.array(podSchema),
	putExits: v.array(exitSchema),
	putEdges: v.array(edgeSchema),
	putGroups: v.array(groupSchema),
	deletePodIds: v.array(idSchema),
	deleteExitIds: v.array(idSchema),
	deleteEdgeIds: v.array(idSchema),
	deleteGroupIds: v.array(idSchema)
});

export const positionsSchema = v.array(
	v.object({ id: idSchema, position: v.object({ x: coordSchema, y: coordSchema }) })
);
