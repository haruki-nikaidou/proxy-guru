<script lang="ts">
import ServerIcon from '@lucide/svelte/icons/server';
import { page } from '$app/state';
import * as Card from '#lib/components/ui/card/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import * as ToggleGroup from '#lib/components/ui/toggle-group/index.js';
import { HEALTH_WINDOWS, type HealthWindowMinutes } from '#lib/dto/health.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import BoundaryError from '#lib/components/BoundaryError.svelte';
import CanvasNodeEvents from './CanvasNodeEvents.svelte';
import ServerHealthCard from './ServerHealthCard.svelte';
import { formatCount, windowLabel } from './format.js';
import { serverHealthLabel } from '#lib/i18n/labels.js';
import { listServerHealth } from './health.remote.js';

const canvasId = $derived(page.params.canvasId ?? '');
let windowMinutes = $state<HealthWindowMinutes>(60);
const health = $derived(listServerHealth({ canvasId, windowMinutes }));
const servers = $derived(health.current);

/** Every count the strip shows, walked once. */
const summary = $derived.by(() => {
	const tally = { online: 0, degraded: 0, offline: 0, reporting: 0, total: servers?.length ?? 0 };
	for (const series of servers ?? []) {
		if (series.status === 'online') tally.online += 1;
		else if (series.status === 'degraded') tally.degraded += 1;
		else if (series.status === 'offline') tally.offline += 1;
		if (series.points.length > 0) tally.reporting += 1;
	}
	return tally;
});

function pickWindow(value: string) {
	const minutes = Number(value);
	// An empty value means the pressed item was toggled off: keep the window.
	if (HEALTH_WINDOWS.some(candidate => candidate === minutes)) {
		windowMinutes = minutes as HealthWindowMinutes;
	}
}
</script>

<div class="flex flex-1 flex-col gap-6 p-4 md:p-8">
	<div class="flex flex-wrap items-end justify-between gap-4">
		<div>
			<h1 class="text-2xl font-semibold">{m.health_title()}</h1>
			<p class="text-sm text-muted-foreground">{m.health_description()}</p>
		</div>
		<ToggleGroup.Root
			type="single"
			variant="outline"
			size="sm"
			value={String(windowMinutes)}
			onValueChange={pickWindow}
			aria-label={m.health_window_label()}
		>
			{#each HEALTH_WINDOWS as minutes (minutes)}
				<ToggleGroup.Item value={String(minutes)}>{windowLabel(minutes)}</ToggleGroup.Item>
			{/each}
		</ToggleGroup.Root>
	</div>

	<svelte:boundary>
		{#if servers === undefined}
			<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
				{#each [0, 1, 2, 3] as slot (slot)}
					<Skeleton class="h-24 w-full rounded-xl" />
				{/each}
			</div>
			<Skeleton class="h-80 w-full rounded-xl" />
		{:else if servers.length === 0}
			<Empty.Root>
				<Empty.Header>
					<Empty.Media variant="icon"><ServerIcon /></Empty.Media>
					<Empty.Title>{m.health_no_servers_title()}</Empty.Title>
					<Empty.Description>{m.health_no_servers_description()}</Empty.Description>
				</Empty.Header>
			</Empty.Root>
		{:else}
			<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
				<Card.Root>
					<Card.Header>
						<Card.Description>{serverHealthLabel('online')}</Card.Description>
						<Card.Title class="text-3xl tabular-nums">{formatCount(summary.online)}</Card.Title>
					</Card.Header>
				</Card.Root>
				<Card.Root>
					<Card.Header>
						<Card.Description>{serverHealthLabel('degraded')}</Card.Description>
						<Card.Title class="text-3xl tabular-nums">{formatCount(summary.degraded)}</Card.Title>
					</Card.Header>
				</Card.Root>
				<Card.Root>
					<Card.Header>
						<Card.Description>{serverHealthLabel('offline')}</Card.Description>
						<Card.Title class="text-3xl tabular-nums">{formatCount(summary.offline)}</Card.Title>
					</Card.Header>
				</Card.Root>
				<Card.Root>
					<Card.Header>
						<Card.Description>{m.health_summary_reporting()}</Card.Description>
						<Card.Title class="text-3xl tabular-nums">{formatCount(summary.reporting)}</Card.Title>
						<p class="text-xs text-muted-foreground">
							{m.health_summary_reporting_hint({ total: summary.total })}
						</p>
					</Card.Header>
				</Card.Root>
			</div>

			{#each servers as series (series.serverId)}
				<ServerHealthCard {series} {canvasId} {windowMinutes} />
			{/each}

			<CanvasNodeEvents {canvasId} {windowMinutes} />
		{/if}

		{#snippet failed(error)}
			<BoundaryError {error} />
		{/snippet}
	</svelte:boundary>
</div>
