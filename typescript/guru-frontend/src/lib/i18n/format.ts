import { m } from '#lib/paraglide/messages.js';

/**
 * Every time on the dashboard reads in UTC+8, wherever it is rendered. Pages are
 * server-rendered on a host in another zone and hydrated in browsers in any
 * zone, so neither local clock is the one the operators work by; a fixed zone
 * also makes the server's text and the browser's identical. `Asia/Shanghai` has
 * no daylight saving, so it is UTC+8 all year.
 */
const UTC8 = new Intl.DateTimeFormat('en-CA', {
	timeZone: 'Asia/Shanghai',
	year: 'numeric',
	month: '2-digit',
	day: '2-digit',
	hour: '2-digit',
	minute: '2-digit',
	second: '2-digit',
	hourCycle: 'h23'
});

type Fields = Record<'year' | 'month' | 'day' | 'hour' | 'minute' | 'second', string>;

/** The zero-padded UTC+8 calendar fields of an instant. */
function utc8(date: Date): Fields {
	const parts = new Map(UTC8.formatToParts(date).map(part => [part.type, part.value]));
	const field = (name: Intl.DateTimeFormatPartTypes) => parts.get(name) ?? '';
	return {
		year: field('year'),
		month: field('month'),
		day: field('day'),
		hour: field('hour'),
		minute: field('minute'),
		second: field('second')
	};
}

/**
 * Backend timestamps are RFC3339 with nanoseconds, or `''` when absent. Shown as
 * `2026-09-17 02:27:56`, in UTC+8.
 */
export function formatTimestamp(rfc3339: string): string {
	if (rfc3339 === '') return m.common_never();
	const parsed = new Date(rfc3339);
	if (Number.isNaN(parsed.getTime())) return rfc3339;
	const t = utc8(parsed);
	return `${t.year}-${t.month}-${t.day} ${t.hour}:${t.minute}:${t.second}`;
}

/**
 * A chart's time tick, in UTC+8: `02:15` within a window of a day or less, and
 * `09-17 02:15` over a longer one, where the clock alone would repeat.
 */
export function formatAxisTime(value: Date | number, windowMinutes: number): string {
	const date = new Date(value);
	if (Number.isNaN(date.getTime())) return '';
	const t = utc8(date);
	const clock = `${t.hour}:${t.minute}`;
	return windowMinutes > 24 * 60 ? `${t.month}-${t.day} ${clock}` : clock;
}
