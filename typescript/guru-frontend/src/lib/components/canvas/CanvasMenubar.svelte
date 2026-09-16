<script lang="ts">
import type { CanvasViewport } from '#lib/components/canvas/viewport.svelte.js';
import * as Menubar from '#lib/components/ui/menubar/index.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * The bar floating over the grid: what an operator can add to the canvas, and
 * how they look at it. It owns no state — the palette hands every gesture back
 * to the flow, which is where the canvas and its dialogs live. Pods are added
 * on their server, and every way on is drawn from a pod.
 */
let {
	editable,
	view,
	onAddServer,
	onAddExit,
	onAddSubcanvas
}: {
	editable: boolean;
	view: CanvasViewport;
	onAddServer: () => void;
	onAddExit: () => void;
	onAddSubcanvas: () => void;
} = $props();
</script>

<Menubar.Root class="bg-background shadow-sm">
	{#if editable}
		<Menubar.Menu>
			<Menubar.Trigger>{m.editor_menu_add()}</Menubar.Trigger>
			<Menubar.Content>
				<Menubar.Group>
					<Menubar.Item onSelect={onAddServer}>{m.editor_new_server()}</Menubar.Item>
					<Menubar.Item onSelect={onAddExit}>{m.editor_new_exit()}</Menubar.Item>
					<Menubar.Item onSelect={onAddSubcanvas}>{m.editor_subcanvas_create_title()}</Menubar.Item>
				</Menubar.Group>
			</Menubar.Content>
		</Menubar.Menu>
	{/if}

	<!-- Opening the menu re-reads the zoom: `fitView` and the side panel move
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
