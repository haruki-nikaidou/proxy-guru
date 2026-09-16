<script lang="ts">
import { isRelay, type RelayKind, subtreeOf, type Target } from 'guru-graph';
import type { PickRequest } from '#lib/components/canvas/editor.svelte.js';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Dialog from '#lib/components/ui/dialog/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import * as Tabs from '#lib/components/ui/tabs/index.js';
import type { CanvasGraph } from '#lib/dto/topology.js';
import { ingressLabel, RELAY_KINDS } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * Where a new way on leads: a relay pod that exists, an exit, or a new relay pod
 * on a server, listening in the protocol chosen here — which is also the
 * protocol every pod leading to it dials in.
 */
let {
	request,
	graph,
	onpick
}: {
	request: PickRequest | null;
	graph: CanvasGraph;
	onpick: (target: Target | null) => void;
} = $props();

const editor = useEditor();

let tab = $state<'pod' | 'exit' | 'server'>('server');
let podId = $state('');
let exitId = $state('');
let serverId = $state('');
let ingress = $state<RelayKind>('relay_quic');

const scope = $derived(request ? subtreeOf(graph, request.scope) : new Set<string>());
const serverName = (id: string) => graph.servers.find(server => server.id === id)?.name ?? id;
const pods = $derived(
	graph.pods
		.filter(
			pod =>
				scope.has(pod.canvasId) && isRelay(pod.ingress) && !request?.excludePods?.includes(pod.id)
		)
		.sort((a, b) => a.name.localeCompare(b.name))
);
const exits = $derived(
	graph.exits.filter(exit => scope.has(exit.canvasId)).sort((a, b) => a.name.localeCompare(b.name))
);
const servers = $derived(
	graph.servers
		.filter(server => scope.has(server.canvasId))
		.sort((a, b) => a.name.localeCompare(b.name))
);

// Every request starts from a clean choice, on the server it names if any.
let seeded: PickRequest | null = null;
$effect(() => {
	if (request === seeded) return;
	seeded = request;
	if (!request) return;
	serverId = request.server ?? '';
	podId = '';
	exitId = '';
	ingress = 'relay_quic';
	tab = request.server ? 'server' : 'pod';
});

const podLabel = (id: string) => {
	const pod = graph.pods.find(entry => entry.id === id);
	return pod ? `${pod.name} · ${serverName(pod.serverId)} · ${ingressLabel(pod.ingress.kind)}` : '';
};
const exitLabel = (id: string) => {
	const exit = graph.exits.find(entry => entry.id === id);
	return exit ? `${exit.name} · ${exit.destination}` : '';
};

const chosen = $derived.by((): Target | null => {
	if (tab === 'pod') return podId ? { pod: podId } : null;
	if (tab === 'exit') return exitId ? { exit: exitId } : null;
	if (!serverId) return null;
	const server = graph.servers.find(entry => entry.id === serverId);
	if (!server) return null;
	// A server of this canvas (or drawn here) gets its pod drawn here; one inside
	// a subcanvas gets it drawn where the server is.
	const canvasId = request?.server ? editor.canvasId : server.canvasId;
	return { server: serverId, ingress, canvasId };
});
</script>

{#snippet protocolField()}
	<Field.Field>
		<Field.FieldLabel for="target-ingress">{m.editor_target_protocol()}</Field.FieldLabel>
		<Select.Root type="single" value={ingress} onValueChange={next => (ingress = next as RelayKind)}>
			<Select.Trigger id="target-ingress">{ingressLabel(ingress)}</Select.Trigger>
			<Select.Content>
				<Select.Group>
					{#each RELAY_KINDS as option (option)}
						<Select.Item value={option} label={ingressLabel(option)}>
							{ingressLabel(option)}
						</Select.Item>
					{/each}
				</Select.Group>
			</Select.Content>
		</Select.Root>
		<Field.FieldDescription>{m.editor_target_protocol_hint()}</Field.FieldDescription>
	</Field.Field>
{/snippet}

<Dialog.Root
	open={request !== null}
	onOpenChange={open => {
		if (!open) onpick(null);
	}}
>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>{request?.title}</Dialog.Title>
			<Dialog.Description>
				{request?.server
					? m.editor_target_new_relay_description({ server: serverName(request.server) })
					: m.editor_target_description()}
			</Dialog.Description>
		</Dialog.Header>


		{#if request?.server}
			{@render protocolField()}
		{:else}
			<Tabs.Root bind:value={tab}>
				<Tabs.List>
					<Tabs.Trigger value="pod">{m.editor_target_tab_pod()}</Tabs.Trigger>
					<Tabs.Trigger value="exit">{m.editor_target_tab_exit()}</Tabs.Trigger>
					<Tabs.Trigger value="server">{m.editor_target_tab_server()}</Tabs.Trigger>
				</Tabs.List>
				<Tabs.Content value="pod" class="pt-3">
					<Select.Root type="single" value={podId} onValueChange={next => (podId = next)}>
						<Select.Trigger class="w-full">{podLabel(podId) || m.editor_target_pick_pod()}</Select.Trigger>
						<Select.Content>
							<Select.Group>
								{#each pods as pod (pod.id)}
									<Select.Item value={pod.id} label={podLabel(pod.id)}>{podLabel(pod.id)}</Select.Item>
								{/each}
							</Select.Group>
						</Select.Content>
					</Select.Root>
					{#if pods.length === 0}
						<p class="mt-2 text-xs text-muted-foreground">{m.editor_target_no_pods()}</p>
					{/if}
				</Tabs.Content>
				<Tabs.Content value="exit" class="pt-3">
					<Select.Root type="single" value={exitId} onValueChange={next => (exitId = next)}>
						<Select.Trigger class="w-full">{exitLabel(exitId) || m.editor_target_pick_exit()}</Select.Trigger>
						<Select.Content>
							<Select.Group>
								{#each exits as exit (exit.id)}
									<Select.Item value={exit.id} label={exitLabel(exit.id)}>{exitLabel(exit.id)}</Select.Item>
								{/each}
							</Select.Group>
						</Select.Content>
					</Select.Root>
					{#if exits.length === 0}
						<p class="mt-2 text-xs text-muted-foreground">{m.editor_target_no_exits()}</p>
					{/if}
				</Tabs.Content>
				<Tabs.Content value="server" class="grid gap-3 pt-3">
					<Select.Root type="single" value={serverId} onValueChange={next => (serverId = next)}>
						<Select.Trigger class="w-full">
							{serverId ? serverName(serverId) : m.editor_target_pick_server()}
						</Select.Trigger>
						<Select.Content>
							<Select.Group>
								{#each servers as server (server.id)}
									<Select.Item value={server.id} label={server.name}>{server.name}</Select.Item>
								{/each}
							</Select.Group>
						</Select.Content>
					</Select.Root>
					{@render protocolField()}
				</Tabs.Content>
			</Tabs.Root>
		{/if}

		<Dialog.Footer class="mt-4">
			<Button variant="outline" onclick={() => onpick(null)}>{m.common_cancel()}</Button>
			<Button disabled={chosen === null} onclick={() => onpick(chosen)}>
				{m.editor_target_confirm()}
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
