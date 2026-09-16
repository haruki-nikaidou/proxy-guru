<script lang="ts">
import EllipsisIcon from '@lucide/svelte/icons/ellipsis';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import * as DropdownMenu from '#lib/components/ui/dropdown-menu/index.js';
import * as Tooltip from '#lib/components/ui/tooltip/index.js';
import type { CanvasSummary } from '#lib/dto/canvas.js';
import { m } from '#lib/paraglide/messages.js';
import CanvasFormDialog from './CanvasFormDialog.svelte';
import DeleteCanvasDialog from './DeleteCanvasDialog.svelte';

let { canvas, editable }: { canvas: CanvasSummary; editable: boolean } = $props();

let editOpen = $state(false);
let deleteOpen = $state(false);
</script>

<Card.Root>
	<Card.Header>
		<Card.Title>
			<a href="/canvas/{canvas.id}" class="hover:underline">{canvas.name}</a>
		</Card.Title>
		<Card.Description>{canvas.description || m.canvas_no_description()}</Card.Description>
		{#if canvas.parent}
			{@const parent = canvas.parent}
			<p class="text-xs text-muted-foreground">
				{m.canvas_subcanvas_of()}
				<a href="/canvas/{parent.id}" class="hover:underline">{parent.name}</a>
			</p>
		{/if}
		{#if editable}
			<Card.Action>
				<DropdownMenu.Root>
					<DropdownMenu.Trigger>
						{#snippet child({ props })}
							<Button {...props} variant="ghost" size="icon" aria-label={m.common_actions()}>
								<EllipsisIcon />
							</Button>
						{/snippet}
					</DropdownMenu.Trigger>
					<DropdownMenu.Content align="end">
						<DropdownMenu.Item onSelect={() => (editOpen = true)}>
							{m.canvas_rename()}
						</DropdownMenu.Item>
						<DropdownMenu.Item variant="destructive" onSelect={() => (deleteOpen = true)}>
							{m.canvas_delete()}
						</DropdownMenu.Item>
					</DropdownMenu.Content>
				</DropdownMenu.Root>
			</Card.Action>
		{/if}
	</Card.Header>

	<Card.Content class="flex flex-wrap items-center gap-2">
		{#if canvas.stats}
			<Badge variant="secondary">{m.canvas_servers_count({ count: canvas.stats.servers })}</Badge>
			<Badge variant="secondary">{m.canvas_pods_count({ count: canvas.stats.pods })}</Badge>
			<Badge variant="secondary">{m.canvas_edges_count({ count: canvas.stats.edges })}</Badge>
		{:else}
			<Badge variant="outline">{m.canvas_stats_unavailable()}</Badge>
		{/if}

		{#if canvas.health}
			{@const health = canvas.health}
			{#if health.errors === 0 && health.warnings === 0}
				<Badge variant="secondary">{m.canvas_health_ok()}</Badge>
			{:else}
				<Tooltip.Root>
					<Tooltip.Trigger>
						{#snippet child({ props })}
							<span {...props} class="flex gap-2">
								{#if health.errors > 0}
									<Badge variant="destructive">
										{m.canvas_health_errors({ count: health.errors })}
									</Badge>
								{/if}
								{#if health.warnings > 0}
									<Badge variant="outline">
										{m.canvas_health_warnings({ count: health.warnings })}
									</Badge>
								{/if}
							</span>
						{/snippet}
					</Tooltip.Trigger>
					<Tooltip.Content>
						<ul class="list-disc ps-4">
							{#each health.problems as problem (problem.message)}
								<li>{problem.message}</li>
							{/each}
						</ul>
					</Tooltip.Content>
				</Tooltip.Root>
			{/if}
		{/if}
	</Card.Content>
</Card.Root>

{#if editable}
	<CanvasFormDialog mode="edit" {canvas} bind:open={editOpen} />
	<DeleteCanvasDialog {canvas} stats={canvas.stats} bind:open={deleteOpen} />
{/if}
