<script lang="ts">
import LockIcon from '@lucide/svelte/icons/lock';
import LogInIcon from '@lucide/svelte/icons/log-in';
import type { NodeProps } from '@xyflow/svelte';
import { portLabel, type FlowNodeData } from '#lib/components/canvas/graph.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import * as Tooltip from '#lib/components/ui/tooltip/index.js';
import { m } from '#lib/paraglide/messages.js';
import NodeShell from './NodeShell.svelte';
import PortHandle from './PortHandle.svelte';

let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'entry' }> } = $props();

const proxy = $derived(
	data.node.receiveProxyProtocol === 'v1'
		? m.editor_proxy_v1()
		: data.node.receiveProxyProtocol === 'v2'
			? m.editor_proxy_v2()
			: m.editor_proxy_none()
);
</script>

<NodeShell
	{id}
	title={data.node.name}
	kindLabel={m.editor_kind_entry()}
	comment={data.node.comment}
	problem={data.problem}
	background="bg-canvas-entry"
>
	{#snippet icon()}<LogInIcon class="size-4 shrink-0" />{/snippet}
	<p class="px-3 pt-1 text-xs text-muted-foreground">
		{m.editor_receive_proxy()}: {proxy}
	</p>
	{#if data.node.tls}
		{@const tls = data.node.tls}
		<!-- The certificate itself lives on `/tls`; the node only reports the SNI
		     this entry terminates. -->
		<div class="px-3 pt-1">
			<Tooltip.Root>
				<Tooltip.Trigger>
					{#snippet child({ props })}
						<Badge {...props} variant="secondary" class="max-w-full">
							<LockIcon />
							<span class="truncate">{tls.sni}</span>
						</Badge>
					{/snippet}
				</Tooltip.Trigger>
				<Tooltip.Content>
					{m.editor_entry_tls_node_hint({ sni: tls.sni })}
				</Tooltip.Content>
			</Tooltip.Root>
		</div>
	{/if}
	<div class="mt-1">
		{#each data.node.ports as port (port.id)}
			<PortHandle {port} label={portLabel(port)} />
		{/each}
	</div>
</NodeShell>
