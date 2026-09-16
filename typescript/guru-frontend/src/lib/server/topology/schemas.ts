/**
 * What the canvas editor's remote functions accept. Every schema mirrors a rule
 * the control plane enforces, so a doomed write fails in the browser instead of
 * round-tripping; the messages are codes, resolved by `#lib/i18n/codes.js`.
 */
import * as v from 'valibot';

export const idSchema = v.pipe(v.string(), v.minLength(1, 'id_required'));
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
export const portSchema = v.pipe(
	v.number(),
	v.integer(),
	v.minValue(1, 'port_out_of_range'),
	v.maxValue(65535, 'port_out_of_range')
);
/**
 * The operator's member list of a load-balance node: 1 to 256, each with a
 * trimmed name (unique, at most 64 characters) and a slot unique in the list.
 * Mirrors `members_ok` in the control plane.
 */
const memberSchema = v.object({
	slot: v.pipe(v.number(), v.integer(), v.minValue(1, 'members_invalid')),
	name: v.pipe(
		v.string(),
		v.trim(),
		v.minLength(1, 'members_invalid'),
		v.maxLength(64, 'members_invalid')
	)
});
export const membersSchema = v.pipe(
	v.array(memberSchema),
	v.minLength(1, 'members_invalid'),
	v.maxLength(256, 'members_invalid'),
	v.check(list => new Set(list.map(m => m.name)).size === list.length, 'members_invalid'),
	v.check(list => new Set(list.map(m => m.slot)).size === list.length, 'members_invalid')
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
export const logLevelSchema = v.pipe(v.string(), v.trim(), v.minLength(1, 'log_level_required'));
/** `guru-worker@<unit>`: what the control plane accepts, or empty for none. */
export const agentUnitSchema = v.optional(
	v.pipe(v.string(), v.trim(), v.regex(/^(?:[a-z0-9][a-z0-9-]{0,31})?$/, 'agent_unit_invalid')),
	''
);
export const proxySchema = v.picklist(['none', 'v1', 'v2'] as const);
export const relayProtocolSchema = v.picklist(['tcp_raw', 'tcp_tls', 'quic'] as const);
export const balanceModeSchema = v.picklist([
	'round_robin',
	'random',
	'ip_hash',
	'fallback'
] as const);
export const ipv6Schema = v.picklist(['required', 'preferred', 'tolerated', 'forbidden'] as const);
/**
 * One hostname, optionally with a leading `*.` wildcard label. Deliberately
 * narrower than the RFC: the ACME order is built from this verbatim.
 */
const SNI_PATTERN = /^(\*\.)?[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$/i;
/**
 * `null` clears TLS termination on the entry. Every field is required except
 * `acme_directory`, where empty means "use the installation default".
 */
export const tlsSchema = v.nullable(
	v.object({
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
	})
);

export const standaloneKindSchema = v.picklist([
	'entry',
	'relay',
	'exit',
	'load_balance_distribute',
	'load_balance_aggregate'
] as const);

export const portKindSchema = v.picklist(['derive_listen', 'derive_destination'] as const);
export const exportAsSchema = v.picklist(['input_into_canvas', 'output_out_of_canvas'] as const);

/** One end of a connect: a port id, or a universal node's handle group. */
const universalGroupSchema = v.picklist(['channel_out', 'bundle_in'] as const);
export const connectEndSchema = v.union([
	v.object({ portId: idSchema }),
	v.object({ nodeId: idSchema, group: universalGroupSchema })
]);
