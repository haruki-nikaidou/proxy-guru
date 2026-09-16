import type { BadgeVariant } from '#lib/components/ui/badge/index.js';
import type { HealthWindowMinutes, PodHealthStatusName } from '#lib/dto/health.js';
import { m } from '#lib/paraglide/messages.js';
import { getLocale } from '#lib/paraglide/runtime.js';

/**
 * Presentation helpers of the health page. Timestamps are *not* formatted here:
 * `#lib/i18n/format.js` owns the only timestamp formatter.
 */

/** IEC symbols: they read the same in every locale, only the number is localised. */
const BYTE_UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'] as const;

export function formatBytes(value: number): string {
	const magnitude = Math.abs(value);
	const step =
		magnitude < 1024 ? 0 : Math.min(Math.floor(Math.log2(magnitude) / 10), BYTE_UNITS.length - 1);
	const scaled = value / 1024 ** step;
	const number = new Intl.NumberFormat(getLocale(), {
		maximumFractionDigits: step === 0 ? 0 : 1
	}).format(scaled);
	return `${number} ${BYTE_UNITS[step]}`;
}

export const formatCount = (value: number): string =>
	new Intl.NumberFormat(getLocale()).format(value);

export function podStatusLabel(status: PodHealthStatusName): string {
	switch (status) {
		case 'ready':
			return m.health_pod_status_ready();
		case 'deploying':
			return m.health_pod_status_deploying();
		case 'failed':
			return m.health_pod_status_failed();
		default:
			return m.health_pod_status_unknown();
	}
}

export function podStatusVariant(status: PodHealthStatusName): BadgeVariant {
	switch (status) {
		case 'ready':
			return 'secondary';
		case 'failed':
			return 'destructive';
		default:
			return 'outline';
	}
}

export function windowLabel(minutes: HealthWindowMinutes): string {
	switch (minutes) {
		case 60:
			return m.health_window_60();
		case 360:
			return m.health_window_360();
		case 1440:
			return m.health_window_1440();
		default:
			return m.health_window_10080();
	}
}
