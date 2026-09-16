<script lang="ts">
import ChartNoAxesColumnIcon from '@lucide/svelte/icons/chart-no-axes-column';
import { AreaChart } from 'layerchart';
import { Badge } from '#lib/components/ui/badge/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import * as Chart from '#lib/components/ui/chart/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import * as Tabs from '#lib/components/ui/tabs/index.js';
import type { HealthWindowMinutes, ServerHealthSeries } from '#lib/dto/health.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import NodeEventsPanel from './NodeEventsPanel.svelte';
import { formatBytes, formatCount } from './format.js';
import { serverHealthBadge, serverHealthLabel } from '#lib/i18n/labels.js';

let {
	series,
	canvasId,
	windowMinutes
}: { series: ServerHealthSeries; canvasId: string; windowMinutes: HealthWindowMinutes } = $props();

/** Both charts read the same rows, so they share one x axis. */
const rows = $derived(
	series.points.map(point => ({
		time: new Date(point.reportTime),
		upload: point.uploadBytes,
		download: point.downloadBytes,
		current: point.currentConnections,
		max: point.maxConnections
	}))
);
const latest = $derived(series.points.at(-1));
const peak = $derived(
	series.points.reduce((most, point) => Math.max(most, point.maxConnections), 0)
);

// The theme's chart ramp is monochromatic, so adjacent steps read as one band
// once the areas are filled: each pair takes the ends of the ramp instead.
const throughputConfig = $derived({
	upload: { label: m.health_series_upload(), color: 'var(--chart-1)' },
	download: { label: m.health_series_download(), color: 'var(--chart-4)' }
} satisfies Chart.ChartConfig);
const connectionsConfig = $derived({
	current: { label: m.health_series_current(), color: 'var(--chart-1)' },
	max: { label: m.health_series_max(), color: 'var(--chart-4)' }
} satisfies Chart.ChartConfig);

/** The x value is a `Date`; the one timestamp formatter takes RFC3339. */
const tooltipLabel = (value: unknown) =>
	formatTimestamp(value instanceof Date ? value.toISOString() : String(value));

/**
 * The chart measures its own gutters against the *default* tick format, so a
 * `190.7 MiB` label would be cut off, and the legend sits in the bottom gutter
 * with the x ticks. Both charts reserve the same room, which also lines their
 * plot areas up.
 */
const AXIS_PADDING = { top: 8, right: 8, bottom: 48, left: 64 };

// A pod feed costs one call per pod, so it is only mounted once asked for.
let tab = $state('metrics');
</script>

{#snippet valueRow(
	value: unknown,
	name: string,
	color: string | undefined,
	format: (value: number) => string
)}
	<div
		style="--color-bg: {color};"
		class="size-2.5 shrink-0 rounded-[2px] bg-(--color-bg)"
	></div>
	<div class="flex flex-1 shrink-0 items-center justify-between gap-2 leading-none">
		<span class="text-muted-foreground">{name}</span>
		<span class="font-mono font-medium text-foreground tabular-nums">
			{format(typeof value === 'number' ? value : Number(value))}
		</span>
	</div>
{/snippet}

<Card.Root>
	<Card.Header>
		<Card.Title class="flex items-center gap-2">
			{series.serverName}
			<Badge
				variant={serverHealthBadge(series.status).variant}
				class={series.status === 'unknown' ? 'text-muted-foreground' : undefined}
			>
				{serverHealthLabel(series.status)}
			</Badge>
		</Card.Title>
		<Card.Description class="flex flex-wrap gap-x-4 gap-y-1">
			<span>
				{m.health_connections_last()}:
				<span class="font-mono text-foreground tabular-nums">
					{formatCount(latest?.currentConnections ?? 0)}
				</span>
			</span>
			<span>
				{m.health_connections_peak()}:
				<span class="font-mono text-foreground tabular-nums">{formatCount(peak)}</span>
			</span>
			<span>
				{m.health_last_report()}:
				<span class="text-foreground">{formatTimestamp(latest?.reportTime ?? '')}</span>
			</span>
			<span>{m.health_reports_count({ count: series.points.length })}</span>
		</Card.Description>
	</Card.Header>

	<Card.Content>
		<Tabs.Root bind:value={tab}>
			<Tabs.List>
				<Tabs.Trigger value="metrics">{m.health_tab_metrics()}</Tabs.Trigger>
				<Tabs.Trigger value="events">{m.health_tab_events()}</Tabs.Trigger>
			</Tabs.List>

			<Tabs.Content value="metrics" class="pt-4">
				{#if rows.length === 0}
					<Empty.Root>
						<Empty.Header>
							<Empty.Media variant="icon"><ChartNoAxesColumnIcon /></Empty.Media>
							<Empty.Title>{m.health_no_records_title()}</Empty.Title>
							<Empty.Description>{m.health_no_records_description()}</Empty.Description>
						</Empty.Header>
					</Empty.Root>
				{:else}
					<!-- Stacked, not side by side: the two charts plot the same rows, so
					     keeping them full width lines their x axes up. -->
					<div class="flex flex-col gap-6">
						<div class="flex min-w-0 flex-1 flex-col gap-1">
							<span class="text-sm font-medium">{m.health_chart_throughput()}</span>
							<Chart.Container config={throughputConfig} class="h-52 w-full">
								<AreaChart
									data={rows}
									x="time"
									seriesLayout="stack"
									legend
									series={[
										{
											key: 'upload',
											label: throughputConfig.upload.label,
											color: 'var(--color-upload)'
										},
										{
											key: 'download',
											label: throughputConfig.download.label,
											color: 'var(--color-download)'
										}
									]}
									props={{ yAxis: { format: formatBytes } }}
									padding={AXIS_PADDING}
								>
									{#snippet tooltip()}
										<Chart.Tooltip labelFormatter={tooltipLabel}>
											{#snippet formatter({ value, name, item })}
												{@render valueRow(value, name, item.config?.color ?? item.color, formatBytes)}
											{/snippet}
										</Chart.Tooltip>
									{/snippet}
								</AreaChart>
							</Chart.Container>
						</div>

						<div class="flex min-w-0 flex-1 flex-col gap-1">
							<span class="text-sm font-medium">{m.health_chart_connections()}</span>
							<Chart.Container config={connectionsConfig} class="h-52 w-full">
								<AreaChart
									data={rows}
									x="time"
									seriesLayout="overlap"
									legend
									series={[
										{
											key: 'max',
											label: connectionsConfig.max.label,
											color: 'var(--color-max)'
										},
										{
											key: 'current',
											label: connectionsConfig.current.label,
											color: 'var(--color-current)'
										}
									]}
									props={{ yAxis: { format: formatCount } }}
									padding={AXIS_PADDING}
								>
									{#snippet tooltip()}
										<Chart.Tooltip labelFormatter={tooltipLabel}>
											{#snippet formatter({ value, name, item })}
												{@render valueRow(value, name, item.config?.color ?? item.color, formatCount)}
											{/snippet}
										</Chart.Tooltip>
									{/snippet}
								</AreaChart>
							</Chart.Container>
						</div>
					</div>
				{/if}
			</Tabs.Content>

			<Tabs.Content value="events" class="pt-4">
				{#if tab === 'events'}
					<NodeEventsPanel {canvasId} serverId={series.serverId} {windowMinutes} />
				{/if}
			</Tabs.Content>
		</Tabs.Root>
	</Card.Content>
</Card.Root>
