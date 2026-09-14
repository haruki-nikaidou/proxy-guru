<script lang="ts">
import NetworkIcon from '@lucide/svelte/icons/network';
import * as Card from '#lib/components/ui/card/index.js';
import * as Collapsible from '#lib/components/ui/collapsible/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import type { HealthWindowMinutes } from '#lib/dto/health.js';
import type { StandaloneNode } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import { getCanvasGraph } from '../topology.remote.js';
import NodeEventRows from './NodeEventRows.svelte';

let { canvasId, windowMinutes }: { canvasId: string; windowMinutes: HealthWindowMinutes } =
	$props();

/**
 * A pod's health row is not the only one a report writes: the control plane also
 * records one for every node the pod's config was derived through — the Entry or
 * Relay on the listen side, the exits, relays and load balancers on the
 * destination side, with the worst status winning when several pods share a
 * node. Those nodes belong to the canvas rather than to one server, so their
 * feed lives here instead of being repeated under each server card.
 */
let open = $state(false);
const graph = $derived(open ? getCanvasGraph({ canvasId }) : undefined);

const kindLabel = (node: StandaloneNode): string => {
	switch (node.kind) {
		case 'entry':
			return m.editor_kind_entry();
		case 'relay':
			return m.editor_kind_relay();
		case 'exit':
			return m.editor_kind_exit();
		case 'load_balance':
			return node.mode === 'aggregate'
				? m.editor_kind_lb_aggregate()
				: m.editor_kind_lb_distribute();
		case 'canvas_import':
			return m.editor_kind_subcanvas();
		default:
			return m.editor_kind_export();
	}
};

// Import and export nodes are canvas boundaries, not derived hops: no report
// ever names one, so listing them would only ever show an empty row.
const nodes = $derived(
	(graph?.current?.nodes ?? []).filter(
		node => node.kind !== 'canvas_import' && node.kind !== 'canvas_export'
	)
);
</script>

<Card.Root>
	<Collapsible.Root bind:open>
		<Card.Header>
			<Card.Title>{m.health_nodes_title()}</Card.Title>
			<Card.Description>{m.health_nodes_description()}</Card.Description>
			<Card.Action>
				<Collapsible.Trigger>
					{#snippet child({ props })}
						<button {...props} class="text-sm underline-offset-4 hover:underline">
							{open ? m.health_nodes_hide() : m.health_nodes_show()}
						</button>
					{/snippet}
				</Collapsible.Trigger>
			</Card.Action>
		</Card.Header>
		<Collapsible.Content>
			<Card.Content>
				{#if graph?.current === undefined}
					<Skeleton class="h-32 w-full" />
				{:else if nodes.length === 0}
					<Empty.Root>
						<Empty.Header>
							<Empty.Media variant="icon"><NetworkIcon /></Empty.Media>
							<Empty.Title>{m.health_nodes_empty_title()}</Empty.Title>
							<Empty.Description>{m.health_nodes_empty_description()}</Empty.Description>
						</Empty.Header>
					</Empty.Root>
				{:else}
					<Table.Root>
						<Table.Header>
							<Table.Row>
								<Table.Head>{m.health_events_node()}</Table.Head>
								<Table.Head>{m.health_events_status()}</Table.Head>
								<Table.Head>{m.health_events_time()}</Table.Head>
								<Table.Head>{m.health_events_message()}</Table.Head>
							</Table.Row>
						</Table.Header>
						<Table.Body>
							{#each nodes as node (node.id)}
								<NodeEventRows
									nodeId={node.id}
									label="{node.name} · {kindLabel(node)}"
									{windowMinutes}
								/>
							{/each}
						</Table.Body>
					</Table.Root>
				{/if}
			</Card.Content>
		</Collapsible.Content>
	</Collapsible.Root>
</Card.Root>
