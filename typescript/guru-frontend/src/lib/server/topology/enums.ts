/**
 * One function per protobuf enum the canvas API carries, in each direction the
 * editor needs. Decoding never fails: every unknown / UNSPECIFIED value reads
 * as the backend's own default, so a dashboard built against an older proto
 * still renders. Encoding is total by construction — the names come from the
 * picklists in `./schemas.js`.
 */
import {
	AddressSource,
	Ingress,
	Ipv6Resolve,
	ProxyProtocolVersion,
	QuicCongestion,
	ServerHealthStatus
} from 'app-protobuf/orchestration/orchestration';
import type { IngressKind, ProxyVersion } from 'guru-graph';
import type {
	AddressSourceName,
	Ipv6ResolveName,
	LogLevelName,
	ProxyProtocolName,
	QuicCongestionName,
	ServerHealthStatusName
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
/** The graph model's spelling: `null` is no PROXY header. */
export const toProxyVersion = (value: ProxyProtocolVersion): ProxyVersion | null => {
	const name = toProxy(value);
	return name === 'none' ? null : name;
};
export function fromProxy(value: ProxyProtocolName | ProxyVersion | null): ProxyProtocolVersion {
	switch (value) {
		case 'v1':
			return ProxyProtocolVersion.PROXY_V1;
		case 'v2':
			return ProxyProtocolVersion.PROXY_V2;
		default:
			return ProxyProtocolVersion.UNSPECIFIED;
	}
}
/**
 * A pod whose ingress this build does not know reads as a raw client listener:
 * it is drawn and can be edited, and the control plane stays the judge.
 */
export function toIngressKind(value: Ingress): IngressKind {
	switch (value) {
		case Ingress.CLIENT_TLS:
			return 'client_tls';
		case Ingress.RELAY_TCP:
			return 'relay_tcp';
		case Ingress.RELAY_TLS:
			return 'relay_tls';
		case Ingress.RELAY_QUIC:
			return 'relay_quic';
		default:
			return 'client_raw';
	}
}
export function fromIngressKind(value: IngressKind): Ingress {
	switch (value) {
		case 'client_tls':
			return Ingress.CLIENT_TLS;
		case 'relay_tcp':
			return Ingress.RELAY_TCP;
		case 'relay_tls':
			return Ingress.RELAY_TLS;
		case 'relay_quic':
			return Ingress.RELAY_QUIC;
		default:
			return Ingress.CLIENT_RAW;
	}
}
export function toQuicCongestion(value: QuicCongestion): QuicCongestionName {
	return value === QuicCongestion.QUIC_BRUTAL ? 'brutal' : 'cubic';
}
export function fromQuicCongestion(value: QuicCongestionName): QuicCongestion {
	return value === 'brutal' ? QuicCongestion.QUIC_BRUTAL : QuicCongestion.QUIC_CUBIC;
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
/**
 * The log level travels as a string; the control plane stores one of five, and
 * anything else reads as `info`, the worker's own default.
 */
export function toLogLevel(value: string): LogLevelName {
	switch (value) {
		case 'trace':
		case 'debug':
		case 'warn':
		case 'error':
			return value;
		default:
			return 'info';
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
