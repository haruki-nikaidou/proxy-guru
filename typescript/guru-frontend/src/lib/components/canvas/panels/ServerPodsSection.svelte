<script lang="ts">
import PlusIcon from '@lucide/svelte/icons/plus';
import { newPod, putPod, type RelayKind } from 'guru-graph';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import RuleDots from '#lib/components/canvas/nodes/RuleDots.svelte';
import { Button } from '#lib/components/ui/button/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import type { ServerDto } from '#lib/dto/topology.js';
import { ingressLabel, listenLabel } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import { seedOn } from '#lib/seed.svelte.js';

/**
 * The pods that run on this server, wherever in the tree they are drawn, and a
 * row to add one. A new pod listens on a port the control plane picks unless one
 * is typed; how it listens — and everything else — is edited on the pod.
 */
let { server }: { server: ServerDto } = $props();

const editor = useEditor();

const pods = $derived(
	editor.graph.pods
		.filter(pod => pod.serverId === server.id)
		.sort((a, b) => a.name.localeCompare(b.name))
);
const canvasName = (id: string) =>
	editor.graph.canvases.find(canvas => canvas.id === id)?.name ?? id;
const rulesOf = (podId: string) => editor.drawing.rules.pods.get(podId) ?? [];

type NewIngress = 'client_raw' | RelayKind;
const NEW_INGRESS: NewIngress[] = ['client_raw', 'relay_quic', 'relay_tls', 'relay_tcp'];

let name = $state('');
let ingress = $state<NewIngress>('client_raw');
// A text field, not `type="number"`: Svelte binds a number input as a number,
// and as null for anything it cannot parse (`80e`), which would read as auto.
let port = $state('');
let pending = $state(false);

seedOn(
	() => server.id,
	() => {
		name = '';
		ingress = 'client_raw';
		port = '';
	}
);

const portValue = $derived(port.trim() === '' ? 0 : Number(port));
const portValid = $derived(Number.isInteger(portValue) && portValue >= 0 && portValue <= 65535);

async function add() {
	pending = true;
	const pod = {
		...newPod(
			editor.canvasId,
			server.id,
			name.trim(),
			ingress === 'client_raw'
				? { kind: 'client_raw', receiveProxyProtocol: null }
				: { kind: ingress }
		),
		port: portValue
	};
	const written = await editor.commit(() => putPod(editor.graph, pod), m.editor_pod_added());
	pending = false;
	if (!written) return;
	name = '';
	port = '';
	editor.open({ kind: 'pod', id: pod.id });
}
</script>

<h3 class="text-sm font-medium">{m.editor_pods()}</h3>
{#if pods.length === 0}
	<p class="mt-2 text-sm text-muted-foreground">{m.editor_pod_none()}</p>
{:else}
	<ul class="mt-2 grid gap-1">
		{#each pods as pod (pod.id)}
			<li>
				<button
					type="button"
					class="flex w-full items-center gap-2 rounded px-2 py-1 text-start text-sm hover:bg-accent"
					onclick={() => editor.open({ kind: 'pod', id: pod.id })}
				>
					<RuleDots rules={rulesOf(pod.id)} max={4} />
					<span class="min-w-0 truncate">{pod.name}</span>
					<span class="shrink-0 text-xs text-muted-foreground">{ingressLabel(pod.ingress.kind)}</span>
					<span class="ms-auto shrink-0 font-mono text-xs text-muted-foreground">
						{listenLabel(pod.bindIp, pod.port)}
					</span>
				</button>
				{#if pod.canvasId !== editor.canvasId}
					<p class="px-2 text-xs text-muted-foreground">
						{m.editor_pod_drawn_on({ canvas: canvasName(pod.canvasId) })}
					</p>
				{/if}
			</li>
		{/each}
	</ul>
{/if}

{#if editor.editable}
	<div class="mt-4 grid gap-2">
		<div class="flex gap-2">
			<Input class="flex-1" placeholder={m.editor_node_name()} bind:value={name} />
			<Input
				inputmode="numeric"
				class="w-28"
				placeholder={m.editor_pod_port_auto()}
				bind:value={port}
				aria-label={m.editor_pod_port()}
				aria-invalid={!portValid}
			/>
		</div>
		<div class="flex gap-2">
			<Select.Root type="single" value={ingress} onValueChange={next => (ingress = next as NewIngress)}>
				<Select.Trigger class="flex-1" aria-label={m.editor_pod_ingress()}>
					{ingressLabel(ingress)}
				</Select.Trigger>
				<Select.Content>
					<Select.Group>
						{#each NEW_INGRESS as option (option)}
							<Select.Item value={option} label={ingressLabel(option)}>{ingressLabel(option)}</Select.Item>
						{/each}
					</Select.Group>
				</Select.Content>
			</Select.Root>
			<Button
				size="sm"
				variant="secondary"
				disabled={pending || name.trim() === '' || !portValid}
				onclick={add}
			>
				<PlusIcon />
				{m.editor_pod_add()}
			</Button>
		</div>
	</div>
{/if}
