<script lang="ts">
import * as Menubar from '#lib/components/ui/menubar/index.js';
import type { CanvasViewport } from '#lib/components/canvas/viewport.svelte.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * The bar floating over the grid: what an operator can add to the canvas, and
 * how they look at it. It owns no state — the palette hands every gesture back
 * to the flow, which is where the canvas and its dialogs live.
 */
let {
	editable,
	view,
	onAddServer,
	onAddNode,
	onSubcanvas,
	onExport
}: {
	editable: boolean;
	view: CanvasViewport;
	onAddServer: () => void;
	onAddNode: (
		kind: 'entry' | 'relay' | 'exit' | 'load_balance_distribute' | 'load_balance_aggregate',
		typeLabel: string
	) => void;
	onSubcanvas: (mode: 'create' | 'import') => void;
	onExport: () => void;
} = $props();
</script>

<Menubar.Root class="bg-background shadow-sm">
	{#if editable}
		<Menubar.Menu>
			<Menubar.Trigger>{m.editor_menu_nodes()}</Menubar.Trigger>
			<Menubar.Content>
				<Menubar.Group>
					<Menubar.Item onSelect={onAddServer}>{m.editor_new_server()}</Menubar.Item>
					<Menubar.Item onSelect={() => onAddNode('entry', m.editor_add_entry())}>
						{m.editor_new_entry()}
					</Menubar.Item>
					<Menubar.Item onSelect={() => onAddNode('relay', m.editor_add_relay())}>
						{m.editor_new_relay()}
					</Menubar.Item>
					<Menubar.Item onSelect={() => onAddNode('exit', m.editor_add_exit())}>
						{m.editor_new_exit()}
					</Menubar.Item>
				</Menubar.Group>
				<Menubar.Separator />
				<Menubar.Group>
					<Menubar.Item
						onSelect={() =>
							onAddNode('load_balance_distribute', m.editor_add_lb_distribute())}
					>
						{m.editor_new_lb_distribute()}
					</Menubar.Item>
					<Menubar.Item
						onSelect={() =>
							onAddNode('load_balance_aggregate', m.editor_add_lb_aggregate())}
					>
						{m.editor_new_lb_aggregate()}
					</Menubar.Item>
				</Menubar.Group>
			</Menubar.Content>
		</Menubar.Menu>

		<Menubar.Menu>
			<Menubar.Trigger>{m.editor_kind_subcanvas()}</Menubar.Trigger>
			<Menubar.Content>
				<Menubar.Group>
					<Menubar.Item onSelect={() => onSubcanvas('create')}>
						{m.editor_subcanvas_create_title()}
					</Menubar.Item>
					<Menubar.Item onSelect={() => onSubcanvas('import')}>
						{m.editor_menu_subcanvas_import()}
					</Menubar.Item>
					<Menubar.Item onSelect={onExport}>
						{m.editor_kind_export()}
					</Menubar.Item>
				</Menubar.Group>
			</Menubar.Content>
		</Menubar.Menu>
	{/if}

	<!-- Opening the menu re-reads the zoom: `fitView` and the node panel move
	     the viewport without a move event. -->
	<Menubar.Menu
		onOpenChange={open => {
			if (open) view.syncZoom();
		}}
	>
		<Menubar.Trigger>{m.editor_menu_view()}</Menubar.Trigger>
		<Menubar.Content>
			<Menubar.Group>
				<Menubar.CheckboxItem
					checked={view.fullscreen}
					onCheckedChange={view.toggleFullscreen}
				>
					{m.editor_view_fullscreen()}
				</Menubar.CheckboxItem>
				<Menubar.CheckboxItem bind:checked={view.interactive}>
					{m.editor_view_interactive()}
				</Menubar.CheckboxItem>
			</Menubar.Group>
			<Menubar.Separator />
			<Menubar.Group>
				<Menubar.GroupHeading>
					{m.editor_view_zoom({ percent: Math.round(view.zoom * 100) })}
				</Menubar.GroupHeading>
				<Menubar.Item closeOnSelect={false} onSelect={() => view.rescale('in')}>
					{m.editor_view_zoom_in()}
				</Menubar.Item>
				<Menubar.Item closeOnSelect={false} onSelect={() => view.rescale('out')}>
					{m.editor_view_zoom_out()}
				</Menubar.Item>
				<Menubar.Item onSelect={() => view.rescale('fit')}>
					{m.editor_view_zoom_fit()}
				</Menubar.Item>
			</Menubar.Group>
		</Menubar.Content>
	</Menubar.Menu>
</Menubar.Root>
