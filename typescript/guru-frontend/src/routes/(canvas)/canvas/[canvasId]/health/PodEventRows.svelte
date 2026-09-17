<script lang="ts">
import { Badge } from '#lib/components/ui/badge/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import type { PodHealthPoint } from '#lib/dto/health.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import { podStatusLabel, podStatusVariant } from './format.js';

/** One pod's rows of the events table; `points` newest first. */
let { label, points }: { label: string; points: PodHealthPoint[] } = $props();
</script>

{#if points.length === 0}
	<Table.Row>
		<Table.Cell class="font-medium">{label}</Table.Cell>
		<Table.Cell colspan={3} class="text-muted-foreground">{m.health_events_empty()}</Table.Cell>
	</Table.Row>
{:else}
	{#each points as event, index (event.id)}
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
