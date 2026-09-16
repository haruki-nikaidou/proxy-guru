<script lang="ts">
import { Badge } from '#lib/components/ui/badge/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import type { HealthWindowMinutes } from '#lib/dto/health.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import { podStatusLabel, podStatusVariant } from './format.js';
import { listPodHealth } from './health.remote.js';

let {
	podId,
	label,
	windowMinutes
}: { podId: string; label: string; windowMinutes: HealthWindowMinutes } = $props();

/** One pod's own history: newest first, exactly as the control plane orders it. */
const events = $derived(listPodHealth({ podId, windowMinutes }));
</script>

{#if events.current === undefined}
	<Table.Row>
		<Table.Cell class="font-medium">{label}</Table.Cell>
		<Table.Cell colspan={3}><Skeleton class="h-4 w-full" /></Table.Cell>
	</Table.Row>
{:else if events.current.length === 0}
	<Table.Row>
		<Table.Cell class="font-medium">{label}</Table.Cell>
		<Table.Cell colspan={3} class="text-muted-foreground">{m.health_events_empty()}</Table.Cell>
	</Table.Row>
{:else}
	{#each events.current as event, index (event.id)}
		<Table.Row>
			<Table.Cell class="font-medium">{index === 0 ? label : ''}</Table.Cell>
			<Table.Cell>
				<Badge
					variant={podStatusVariant(event.status)}
					class={event.status === 'unknown' ? 'text-muted-foreground' : undefined}
				>
					{podStatusLabel(event.status)}
				</Badge>
			</Table.Cell>
			<Table.Cell class="whitespace-nowrap">{formatTimestamp(event.reportTime)}</Table.Cell>
			<Table.Cell class="text-muted-foreground">
				{event.message.length > 0 ? event.message : '—'}
			</Table.Cell>
		</Table.Row>
	{/each}
{/if}
