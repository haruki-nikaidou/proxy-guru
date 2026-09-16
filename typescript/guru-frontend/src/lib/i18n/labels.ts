import type { BadgeVariant } from '#lib/components/ui/badge/index.js';
import type {
	CanvasExportAsName,
	ExportPortKindName,
	Ipv6ResolveName,
	LoadBalanceModeName,
	ProxyProtocolName,
	QuicCongestionName,
	RelayProtocolName,
	ServerHealthStatusName
} from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * What each variant of an enum is called on screen, and the order a picker
 * offers them in. One place, because a node card and the panel that edits it
 * must say the same word for the same value — they were drifting apart as
 * separate ternaries in a dozen components.
 *
 * Every unknown value falls back to the variant the control plane treats as the
 * default, matching how `#lib/server/topology/enums.js` decodes it.
 */

export const PROXY_OPTIONS: ProxyProtocolName[] = ['none', 'v1', 'v2'];
export const proxyLabel = (value: ProxyProtocolName): string =>
	value === 'v1'
		? m.editor_proxy_v1()
		: value === 'v2'
			? m.editor_proxy_v2()
			: m.editor_proxy_none();

export const RELAY_PROTOCOLS: RelayProtocolName[] = ['tcp_raw', 'tcp_tls', 'quic'];
export const relayProtocolLabel = (value: RelayProtocolName): string =>
	value === 'tcp_tls'
		? m.editor_relay_tcp_tls()
		: value === 'quic'
			? m.editor_relay_quic()
			: m.editor_relay_tcp_raw();

export const BALANCE_MODES: LoadBalanceModeName[] = [
	'round_robin',
	'random',
	'ip_hash',
	'fallback'
];
export const balanceModeLabel = (value: LoadBalanceModeName): string =>
	value === 'random'
		? m.editor_balance_random()
		: value === 'ip_hash'
			? m.editor_balance_ip_hash()
			: value === 'fallback'
				? m.editor_balance_fallback()
				: m.editor_balance_round_robin();

export const QUIC_CONGESTION_OPTIONS: QuicCongestionName[] = ['cubic', 'brutal'];
export const quicCongestionLabel = (value: QuicCongestionName): string =>
	value === 'brutal' ? m.editor_quic_brutal() : m.editor_quic_cubic();
export const IPV6_OPTIONS: Ipv6ResolveName[] = ['required', 'preferred', 'tolerated', 'forbidden'];
export const ipv6Label = (value: Ipv6ResolveName): string =>
	value === 'required'
		? m.editor_ipv6_required()
		: value === 'preferred'
			? m.editor_ipv6_preferred()
			: value === 'forbidden'
				? m.editor_ipv6_forbidden()
				: m.editor_ipv6_tolerated();

export const EXPORT_PORT_KINDS: ExportPortKindName[] = ['derive_listen', 'derive_destination'];
export const exportKindLabel = (value: ExportPortKindName): string =>
	value === 'derive_listen' ? m.editor_port_listen() : m.editor_port_destination();

export const EXPORT_DIRECTIONS: CanvasExportAsName[] = [
	'input_into_canvas',
	'output_out_of_canvas'
];
export const exportAsLabel = (value: CanvasExportAsName): string =>
	value === 'input_into_canvas'
		? m.editor_export_input_into_canvas()
		: m.editor_export_output_out_of_canvas();

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

/** Badge variant per status; `unknown` is an outline badge with muted text. */
export const serverHealthBadge = (
	status: ServerHealthStatusName
): { variant: BadgeVariant; class: string } =>
	status === 'online'
		? { variant: 'secondary', class: '' }
		: status === 'degraded'
			? { variant: 'outline', class: '' }
			: status === 'offline'
				? { variant: 'destructive', class: '' }
				: { variant: 'outline', class: 'text-muted-foreground' };
