<script lang="ts">
import MonitorSmartphoneIcon from '@lucide/svelte/icons/monitor-smartphone';
import { SvelteFlowProvider } from '@xyflow/svelte';
import { page } from '$app/state';
import CanvasFlow from '#lib/components/canvas/CanvasFlow.svelte';
import * as Empty from '#lib/components/ui/empty/index.js';
import { IsMobile } from '#lib/hooks/is-mobile.svelte.js';
import { m } from '#lib/paraglide/messages.js';
import { canEditWorkspace, canUseRemoteShell } from '#lib/permissions.js';
import type { PageProps } from './$types.js';

let { data }: PageProps = $props();

const canvasId = $derived(page.params.canvasId ?? '');
const editable = $derived(canEditWorkspace(data.identity.role));
const admin = $derived(data.identity.role === 'admin');
const remoteShell = $derived(canUseRemoteShell(data.identity.role));

// The flow plus its side panel do not fit a phone; nothing is gained by trying.
const narrow = new IsMobile();
</script>

{#if narrow.current}
	<div class="flex flex-1 items-center justify-center p-4">
		<Empty.Root>
			<Empty.Header>
				<Empty.Media variant="icon"><MonitorSmartphoneIcon /></Empty.Media>
				<Empty.Title>{m.editor_narrow_title()}</Empty.Title>
				<Empty.Description>{m.editor_narrow_description()}</Empty.Description>
			</Empty.Header>
		</Empty.Root>
	</div>
{:else}
	<!-- `SvelteFlowProvider` is required: `CanvasFlow` calls `useSvelteFlow()`. -->
	<div class="min-h-0 flex-1">
		<SvelteFlowProvider>
			<CanvasFlow {canvasId} {editable} {admin} {remoteShell} />
		</SvelteFlowProvider>
	</div>
{/if}
