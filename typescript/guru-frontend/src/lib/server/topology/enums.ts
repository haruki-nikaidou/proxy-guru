/**
 * One function per protobuf enum the canvas API carries, in each direction the
 * editor needs. Decoding never fails: every unknown / UNSPECIFIED value reads
 * as the backend's own default, so a dashboard built against an older proto
 * still renders. Encoding is total by construction — the names come from the
 * picklists in `./schemas.js`.
 */
import {
	AddressSource,
	CanvasExportAs,
	Ipv6Resolve,
	LoadBalanceMode,
	PortDirection,
	PortKind,
	ProxyProtocolVersion,
	RelayProtocol,
	ServerHealthStatus,
	UniversalGroup
} from 'app-protobuf/orchestration/orchestration';
import type {
	AddressSourceName,
	CanvasExportAsName,
	ExportPortKindName,
	Ipv6ResolveName,
	LoadBalanceModeName,
	PortDirectionName,
	PortKindName,
	ProxyProtocolName,
	RelayProtocolName,
	ServerHealthStatusName,
	UniversalGroupName
} from '#lib/dto/topology.js';

export function toProxy(value: ProxyProtocolVersion): ProxyProtocolName {
	switch (value) {
		case ProxyProtocolVersion.PROXY_V1:
			return 'v1';
		case ProxyProtocolVersion.PROXY_V2:
			return 'v2';
		default:
			return 'none';
	}
}
export function fromProxy(value: ProxyProtocolName): ProxyProtocolVersion {
	switch (value) {
		case 'v1':
			return ProxyProtocolVersion.PROXY_V1;
		case 'v2':
			return ProxyProtocolVersion.PROXY_V2;
		default:
			return ProxyProtocolVersion.UNSPECIFIED;
	}
}
export function toRelayProtocol(value: RelayProtocol): RelayProtocolName {
	switch (value) {
		case RelayProtocol.RELAY_TCP_TLS:
			return 'tcp_tls';
		case RelayProtocol.RELAY_QUIC:
			return 'quic';
		default:
			return 'tcp_raw';
	}
}
export function fromRelayProtocol(value: RelayProtocolName): RelayProtocol {
	switch (value) {
		case 'tcp_tls':
			return RelayProtocol.RELAY_TCP_TLS;
		case 'quic':
			return RelayProtocol.RELAY_QUIC;
		default:
			return RelayProtocol.RELAY_TCP_RAW;
	}
}
export function toBalanceMode(value: LoadBalanceMode): LoadBalanceModeName {
	switch (value) {
		case LoadBalanceMode.RANDOM:
			return 'random';
		case LoadBalanceMode.IP_HASH:
			return 'ip_hash';
		case LoadBalanceMode.FALLBACK:
			return 'fallback';
		default:
			return 'round_robin';
	}
}
export function fromBalanceMode(value: LoadBalanceModeName): LoadBalanceMode {
	switch (value) {
		case 'random':
			return LoadBalanceMode.RANDOM;
		case 'ip_hash':
			return LoadBalanceMode.IP_HASH;
		case 'fallback':
			return LoadBalanceMode.FALLBACK;
		default:
			return LoadBalanceMode.ROUND_ROBIN;
	}
}
export function toIpv6(value: Ipv6Resolve): Ipv6ResolveName {
	switch (value) {
		case Ipv6Resolve.IPV6_REQUIRED:
			return 'required';
		case Ipv6Resolve.IPV6_PREFERRED:
			return 'preferred';
		case Ipv6Resolve.IPV6_FORBIDDEN:
			return 'forbidden';
		default:
			// `IPV6_RESOLVE_UNSPECIFIED` decodes to `Tolerated` in the control plane.
			return 'tolerated';
	}
}
export function fromIpv6(value: Ipv6ResolveName): Ipv6Resolve {
	switch (value) {
		case 'required':
			return Ipv6Resolve.IPV6_REQUIRED;
		case 'preferred':
			return Ipv6Resolve.IPV6_PREFERRED;
		case 'forbidden':
			return Ipv6Resolve.IPV6_FORBIDDEN;
		default:
			return Ipv6Resolve.IPV6_TOLERATED;
	}
}
/**
 * A server that never reported, and any status this build does not know, both
 * read as `unknown`: the dashboard must not claim a worker is online.
 */
export function toServerHealth(value: ServerHealthStatus): ServerHealthStatusName {
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
export const toPortKind = (value: PortKind): PortKindName =>
	value === PortKind.DERIVE_LISTEN
		? 'derive_listen'
		: value === PortKind.BUNDLE
			? 'bundle'
			: 'derive_destination';
export const toPortDirection = (value: PortDirection): PortDirectionName =>
	value === PortDirection.PORT_OUTPUT ? 'output' : 'input';

export const toAddressSource = (value: AddressSource): AddressSourceName => {
	switch (value) {
		case AddressSource.ADDRESS_OVERRIDE:
			return 'override';
		case AddressSource.ADDRESS_REPORTED:
			return 'reported';
		case AddressSource.ADDRESS_OBSERVED:
			return 'observed';
		default:
			return 'none';
	}
};

export const fromPortKind = (value: ExportPortKindName): PortKind =>
	value === 'derive_listen' ? PortKind.DERIVE_LISTEN : PortKind.DERIVE_DESTINATION;
export const fromExportAs = (value: CanvasExportAsName): CanvasExportAs =>
	value === 'input_into_canvas'
		? CanvasExportAs.INPUT_INTO_CANVAS
		: CanvasExportAs.OUTPUT_OUT_OF_CANVAS;

export const fromGroup = (value: UniversalGroupName): UniversalGroup =>
	value === 'channel_out' ? UniversalGroup.CHANNEL_OUT : UniversalGroup.BUNDLE_IN;
