<script lang="ts">
import LayoutDashboardIcon from '@lucide/svelte/icons/layout-dashboard';
import PlusIcon from '@lucide/svelte/icons/plus';
import { Button } from '#lib/components/ui/button/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import { Label } from '#lib/components/ui/label/index.js';
import { Switch } from '#lib/components/ui/switch/index.js';
import { canEditWorkspace } from '#lib/permissions.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import BoundaryError from '#lib/components/BoundaryError.svelte';
import CanvasCard from './CanvasCard.svelte';
import CanvasFormDialog from './CanvasFormDialog.svelte';
import { listCanvases } from './canvases.remote.js';
import type { PageProps } from './$types.js';

let { data }: PageProps = $props();

const editable = $derived(canEditWorkspace(data.identity.role));
let createOpen = $state(false);
/** Subcanvases are reached by drilling into their parent unless this is on. */
let showSubcanvases = $state(false);

const canvases = $derived(listCanvases({ includeSubcanvases: showSubcanvases }));
</script>

<div class="flex items-center justify-between gap-4">
	<div>
		<h1 class="text-2xl font-semibold">{m.canvas_list_title()}</h1>
		<p class="text-sm text-muted-foreground">{m.canvas_list_description()}</p>
	</div>
	<div class="flex items-center gap-4">
		<div class="flex items-center gap-2">
			<Switch id="show-subcanvases" bind:checked={showSubcanvases} />
			<Label for="show-subcanvases" class="text-sm font-normal">
				{m.canvas_list_show_subcanvases()}
			</Label>
		</div>
		{#if editable}
			<Button onclick={() => (createOpen = true)}>
				<PlusIcon />
				{m.canvas_create()}
			</Button>
		{/if}
	</div>
</div>

<svelte:boundary>
	{#if canvases.current === undefined && canvases.error}
		<BoundaryError error={canvases.error} retry />
	{:else if canvases.loading}
		<div class="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
			{#each { length: 3 }}
				<Skeleton class="h-40 w-full rounded-xl" />
			{/each}
		</div>
	{:else if (canvases.current ?? []).length === 0}
		<Empty.Root>
			<Empty.Header>
				<Empty.Media variant="icon"><LayoutDashboardIcon /></Empty.Media>
				<Empty.Title>{m.canvas_list_empty_title()}</Empty.Title>
				<Empty.Description>{m.canvas_list_empty_description()}</Empty.Description>
			</Empty.Header>
			{#if editable}
				<Empty.Content>
					<Button onclick={() => (createOpen = true)}>
						<PlusIcon />
						{m.canvas_create()}
					</Button>
				</Empty.Content>
			{/if}
		</Empty.Root>
	{:else}
		<div class="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
			{#each canvases.current ?? [] as canvas (canvas.id)}
				<CanvasCard {canvas} {editable} />
			{/each}
		</div>
	{/if}

	{#snippet failed(error, reset)}
		<BoundaryError {error} {reset} />
	{/snippet}
</svelte:boundary>

{#if editable}
	<CanvasFormDialog mode="create" bind:open={createOpen} />
{/if}
