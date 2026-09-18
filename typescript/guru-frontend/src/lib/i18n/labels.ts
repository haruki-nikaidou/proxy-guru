import type { IngressKind, IpFamily, ProxyVersion, RelayKind } from 'guru-graph';
import type { BadgeVariant } from '#lib/components/ui/badge/index.js';
import type {
	Ipv6ResolveName,
	LogLevelName,
	ProxyProtocolName,
	QuicCongestionName,
	ServerHealthStatusName
} from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * What each variant of an enum is called on screen, and the order a picker
 * offers them in. One place, because a card and the panel that edits it must
 * say the same word for the same value — they were drifting apart as separate
 * ternaries in a dozen components.
 *
 * Every unknown value falls back to the variant the control plane treats as the
 * default, matching how `#lib/server/topology/enums.js` decodes it.
 */

export const PROXY_OPTIONS: ProxyProtocolName[] = ['none', 'v1', 'v2'];
export const proxyLabel = (value: ProxyProtocolName | ProxyVersion | null): string =>
	value === 'v1'
		? m.editor_proxy_v1()
		: value === 'v2'
			? m.editor_proxy_v2()
			: m.editor_proxy_none();
/** The picker's name for a stored PROXY setting, and back. */
export const proxyName = (value: ProxyVersion | null): ProxyProtocolName => value ?? 'none';
export const proxyVersion = (value: ProxyProtocolName): ProxyVersion | null =>
	value === 'none' ? null : value;

export const INGRESS_KINDS: IngressKind[] = [
	'client_raw',
	'client_tls',
	'relay_tcp',
	'relay_tls',
	'relay_quic'
];
export const RELAY_KINDS: RelayKind[] = ['relay_quic', 'relay_tls', 'relay_tcp'];
export const isClientIngress = (kind: IngressKind): boolean =>
	kind === 'client_raw' || kind === 'client_tls';
/** How a pod listens, in full: who arrives there, and over what. */
export const ingressLabel = (kind: IngressKind): string => {
	switch (kind) {
		case 'client_raw':
			return m.editor_ingress_client_raw();
		case 'client_tls':
			return m.editor_ingress_client_tls();
		case 'relay_tcp':
			return m.editor_ingress_relay_tcp();
		case 'relay_tls':
			return m.editor_ingress_relay_tls();
		default:
			return m.editor_ingress_relay_quic();
	}
};
/** The protocol alone: what a pod's row shows next to its name. */
export const ingressProtocolLabel = (kind: IngressKind): string =>
	kind === 'client_tls' || kind === 'relay_tls' ? 'TLS' : kind === 'relay_quic' ? 'QUIC' : 'TCP';

/** `[::]:port` for a wildcard bind, `[v6]:port` for a literal IPv6. */
export const listenLabel = (bindIp: string | null, port: number): string =>
	bindIp === null
		? `[::]:${port}`
		: bindIp.includes(':')
			? `[${bindIp}]:${port}`
			: `${bindIp}:${port}`;

export const policyLabel = (kind: 'balance' | 'failover'): string =>
	kind === 'failover' ? m.editor_policy_failover() : m.editor_policy_balance();

export const QUIC_CONGESTION_OPTIONS: QuicCongestionName[] = ['cubic', 'brutal'];
export const quicCongestionLabel = (value: QuicCongestionName): string =>
	value === 'brutal' ? m.editor_quic_brutal() : m.editor_quic_cubic();
export const LOG_LEVEL_OPTIONS: LogLevelName[] = ['trace', 'debug', 'info', 'warn', 'error'];
export const logLevelLabel = (value: LogLevelName): string =>
	value === 'trace'
		? m.editor_log_level_trace()
		: value === 'debug'
			? m.editor_log_level_debug()
			: value === 'warn'
				? m.editor_log_level_warn()
				: value === 'error'
					? m.editor_log_level_error()
					: m.editor_log_level_info();
export const IPV6_OPTIONS: Ipv6ResolveName[] = ['required', 'preferred', 'tolerated', 'forbidden'];
export const ipv6Label = (value: Ipv6ResolveName): string =>
	value === 'required'
		? m.editor_ipv6_required()
		: value === 'preferred'
			? m.editor_ipv6_preferred()
			: value === 'forbidden'
				? m.editor_ipv6_forbidden()
				: m.editor_ipv6_tolerated();

export const IP_FAMILY_OPTIONS: IpFamily[] = ['auto', 'v4', 'v6'];
export const ipFamilyLabel = (value: IpFamily): string =>
	value === 'v4'
		? m.editor_ip_family_v4()
		: value === 'v6'
			? m.editor_ip_family_v6()
			: m.editor_ip_family_auto();

/** A pod binds every address by default; `0.0.0.0` is IPv4 only. */
export const BIND_ALL = '';
export const BIND_V4 = '0.0.0.0';
/** An empty advertise address means "whatever the server resolves to". */
export const ADVERTISE_AUTO = '';

export const podBindLabel = (value: string): string =>
	value === BIND_ALL ? m.editor_pod_bind_all() : value === BIND_V4 ? m.editor_pod_bind_v4() : value;
export const podAdvertiseLabel = (value: string): string =>
	value === ADVERTISE_AUTO ? m.editor_pod_advertise_auto() : value;

export function roleLabel(role: string): string {
	switch (role) {
		case 'admin':
			return m.role_admin();
		case 'maintainer':
			return m.role_maintainer();
		case 'observer':
			return m.role_observer();
		default:
			return m.role_unknown();
	}
}

/**
 * The shared health vocabulary: the node badge, the server panel and the
 * health page must read the same way — they used to carry two sets of message
 * keys and had already drifted apart. `unknown` is never dressed as healthy.
 */
export function serverHealthLabel(status: ServerHealthStatusName): string {
	switch (status) {
		case 'online':
			return m.editor_health_online();
		case 'degraded':
			return m.editor_health_degraded();
		case 'offline':
			return m.editor_health_offline();
		default:
			return m.editor_health_unknown();
	}
}

/**
 * Badge look per status: a filled traffic light for the three states a worker
 * reports, and `unknown` as an outline with muted text. The fills are fixed
 * colours, not theme tokens, so green, amber and red mean the same in both
 * themes; white text stays above 4.5:1 on green-700 and red-600 at `text-xs`.
 */
export const serverHealthBadge = (
	status: ServerHealthStatusName
): { variant: BadgeVariant; class: string } =>
	status === 'online'
		? { variant: 'default', class: 'bg-green-700 text-white' }
		: status === 'degraded'
			? { variant: 'default', class: 'bg-amber-400 text-amber-950' }
			: status === 'offline'
				? { variant: 'default', class: 'bg-red-600 text-white' }
				: { variant: 'outline', class: 'text-muted-foreground' };
