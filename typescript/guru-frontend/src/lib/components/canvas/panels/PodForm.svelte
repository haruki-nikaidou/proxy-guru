<script lang="ts">
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import {
	edgeTargetPod,
	type Ingress,
	type IngressKind,
	type Pod,
	putPod,
	removeAll
} from 'guru-graph';
import CopyButton from '#lib/components/CopyButton.svelte';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import RuleDots from '#lib/components/canvas/nodes/RuleDots.svelte';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Separator } from '#lib/components/ui/separator/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type { ProxyProtocolName } from '#lib/dto/topology.js';
import {
	ADVERTISE_AUTO,
	BIND_ALL,
	BIND_V4,
	INGRESS_KINDS,
	ingressLabel,
	isClientIngress,
	PROXY_OPTIONS,
	podAdvertiseLabel,
	podBindLabel,
	proxyLabel,
	proxyName,
	proxyVersion
} from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import { seedOn } from '#lib/seed.svelte.js';
import PodTlsFields, { type TlsDraft, tlsIssues } from './PodTlsFields.svelte';
import RouteEditor from './RouteEditor.svelte';

/**
 * A pod: one listener on one server. How it listens decides who may arrive —
 * clients, or other pods relaying in that protocol — and its route decides
 * where the traffic goes on. Every field is a draft until Save.
 */
let { pod }: { pod: Pod } = $props();

const editor = useEditor();
const graph = $derived(editor.graph);
const server = $derived(graph.servers.find(entry => entry.id === pod.serverId));

let name = $state('');
let comment = $state('');
let kind = $state<IngressKind>('client_raw');
let proxy = $state<ProxyProtocolName>('none');
// A text field, not `type="number"`: Svelte binds a number input as a number,
// and as null for anything it cannot parse (`80e`), which would read as auto.
let port = $state('');
let bindIp = $state(BIND_ALL);
let advertiseIp = $state(ADVERTISE_AUTO);
let tls = $state<TlsDraft>({ sni: '', dnsProviderId: '', domainId: '', acmeDirectory: '' });
let saves = $state(0);
let pending = $state(false);

// Reseeded after every save too: a port the control plane picked, or an
// address it canonicalised, is what the next save sends.
seedOn(
	() => `${pod.id}#${saves}`,
	() => {
		name = pod.name;
		comment = pod.comment;
		kind = pod.ingress.kind;
		proxy =
			pod.ingress.kind === 'client_raw' || pod.ingress.kind === 'client_tls'
				? proxyName(pod.ingress.receiveProxyProtocol)
				: 'none';
		port = pod.port === 0 ? '' : String(pod.port);
		bindIp = pod.bindIp ?? BIND_ALL;
		advertiseIp = pod.advertiseIp ?? ADVERTISE_AUTO;
		tls =
			pod.ingress.kind === 'client_tls'
				? { ...pod.ingress.tls }
				: { sni: '', dnsProviderId: '', domainId: '', acmeDirectory: '' };
	}
);

const addresses = $derived(server?.addresses);
/** What a pod may bind: any address the host actually has, plus the extras. */
const bindOptions = $derived([
	BIND_ALL,
	BIND_V4,
	...new Set([
		...(addresses?.reportedInterfaces ?? []),
		...(addresses?.extra ?? []),
		// A stored address that is no longer a candidate stays selectable, so an
		// edit never silently rewrites it.
		...(bindIp !== BIND_ALL && bindIp !== BIND_V4 ? [bindIp] : [])
	])
]);
/** Every address the server is known by, pinned values first, deduplicated. */
const advertiseOptions = $derived([
	ADVERTISE_AUTO,
	...new Set(
		[
			addresses?.v4.pinned ?? '',
			addresses?.v4.reported ?? '',
			addresses?.v6.pinned ?? '',
			addresses?.v6.reported ?? '',
			...(addresses?.extra ?? []),
			addresses?.observedAddress ?? '',
			advertiseIp
		].filter(value => value !== '')
	)
]);

const portValue = $derived(port.trim() === '' ? 0 : Number(port));
const portValid = $derived(Number.isInteger(portValue) && portValue >= 0 && portValue <= 65535);
const issues = $derived(kind === 'client_tls' ? tlsIssues(tls) : null);
const valid = $derived(
	name.trim() !== '' &&
		portValid &&
		(issues === null || Object.values(issues).every(issue => issue === ''))
);

async function save() {
	const ingress: Ingress =
		kind === 'client_raw'
			? { kind, receiveProxyProtocol: proxyVersion(proxy) }
			: kind === 'client_tls'
				? {
						kind,
						receiveProxyProtocol: proxyVersion(proxy),
						tls: {
							sni: tls.sni.trim(),
							dnsProviderId: tls.dnsProviderId.trim(),
							domainId: tls.domainId.trim(),
							acmeDirectory: tls.acmeDirectory.trim()
						}
					}
				: { kind };
	const next: Pod = {
		...pod,
		name: name.trim(),
		comment: comment.trim(),
		port: portValue,
		bindIp: bindIp === BIND_ALL ? null : bindIp,
		advertiseIp: advertiseIp === ADVERTISE_AUTO ? null : advertiseIp,
		ingress
	};
	pending = true;
	const written = await editor.commit(() => putPod(editor.graph, next), m.editor_saved());
	pending = false;
	if (written) saves += 1;
}

const dialedBy = $derived(
	graph.edges
		.filter(edge => edgeTargetPod(edge) === pod.id)
		.flatMap(edge => {
			const source = graph.pods.find(entry => entry.id === edge.sourcePodId);
			return source ? [{ edge, source }] : [];
		})
		.sort((a, b) => a.source.name.localeCompare(b.source.name))
);
const serverName = (id: string) => graph.servers.find(entry => entry.id === id)?.name ?? id;
const rules = $derived(editor.drawing.rules.pods.get(pod.id) ?? []);

function remove() {
	const current = editor.graph;
	editor.review({
		title: m.editor_pod_delete_title(),
		description: m.editor_pod_delete_description({ name: pod.name, port: pod.port }),
		prunable: true,
		build: prune => removeAll(current, editor.drawing, { podIds: [pod.id] }, prune),
		success: m.editor_deleted()
	});
}
</script>

<div class="mb-4 grid gap-1 text-xs text-muted-foreground">
	<div class="flex items-center gap-1">
		<span>{m.editor_pod_id()}</span>
		<span class="truncate font-mono">{pod.id}</span>
		<CopyButton value={pod.id} label={m.editor_pod_id_copy()} size="icon-xs" class="shrink-0" />
	</div>
	<div class="flex items-center gap-2">
		<span>{m.editor_kind_server()}:</span>
		<button
			type="button"
			class="truncate text-foreground hover:underline"
			onclick={() => editor.open({ kind: 'server', id: pod.serverId })}
		>
			{server?.name ?? pod.serverId}
		</button>
		<span class="ms-auto"><RuleDots {rules} /></span>
	</div>
</div>

<Field.FieldGroup>
	<Field.Field>
		<Field.FieldLabel for="pod-name">{m.editor_node_name()}</Field.FieldLabel>
		<Input id="pod-name" bind:value={name} disabled={!editor.editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="pod-comment">{m.editor_node_comment()}</Field.FieldLabel>
		<Textarea id="pod-comment" bind:value={comment} disabled={!editor.editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="pod-ingress">{m.editor_pod_ingress()}</Field.FieldLabel>
		<Select.Root
			type="single"
			value={kind}
			disabled={!editor.editable}
			onValueChange={next => (kind = next as IngressKind)}
		>
			<Select.Trigger id="pod-ingress">{ingressLabel(kind)}</Select.Trigger>
			<Select.Content>
				<Select.Group>
					{#each INGRESS_KINDS as option (option)}
						<Select.Item value={option} label={ingressLabel(option)}>{ingressLabel(option)}</Select.Item>
					{/each}
				</Select.Group>
			</Select.Content>
		</Select.Root>
		<Field.FieldDescription>
			{isClientIngress(kind) ? m.editor_pod_ingress_client_hint() : m.editor_pod_ingress_relay_hint()}
		</Field.FieldDescription>
	</Field.Field>

	{#if isClientIngress(kind)}
		<Field.Field>
			<Field.FieldLabel for="pod-proxy">{m.editor_receive_proxy()}</Field.FieldLabel>
			<Select.Root
				type="single"
				value={proxy}
				disabled={!editor.editable}
				onValueChange={next => (proxy = next as ProxyProtocolName)}
			>
				<Select.Trigger id="pod-proxy">{proxyLabel(proxy)}</Select.Trigger>
				<Select.Content>
					<Select.Group>
						{#each PROXY_OPTIONS as option (option)}
							<Select.Item value={option} label={proxyLabel(option)}>{proxyLabel(option)}</Select.Item>
						{/each}
					</Select.Group>
				</Select.Content>
			</Select.Root>
		</Field.Field>
	{/if}

	<Field.Field>
		<Field.FieldLabel for="pod-port">{m.editor_pod_port()}</Field.FieldLabel>
		<Input
			id="pod-port"
			inputmode="numeric"
			placeholder={m.editor_pod_port_auto()}
			bind:value={port}
			disabled={!editor.editable}
			aria-invalid={!portValid}
		/>
		<Field.FieldDescription>{m.editor_pod_port_hint()}</Field.FieldDescription>
	</Field.Field>

	<div class="grid grid-cols-2 gap-3">
		<Field.Field>
			<Field.FieldLabel for="pod-bind">{m.editor_pod_bind()}</Field.FieldLabel>
			<Select.Root
				type="single"
				value={bindIp}
				disabled={!editor.editable}
				onValueChange={next => (bindIp = next)}
			>
				<Select.Trigger id="pod-bind">{podBindLabel(bindIp)}</Select.Trigger>
				<Select.Content>
					<Select.Group>
						{#each bindOptions as option (option)}
							<Select.Item value={option} label={podBindLabel(option)}>{podBindLabel(option)}</Select.Item>
						{/each}
					</Select.Group>
				</Select.Content>
			</Select.Root>
		</Field.Field>
		<Field.Field>
			<Field.FieldLabel for="pod-advertise">{m.editor_pod_advertise()}</Field.FieldLabel>
			<Select.Root
				type="single"
				value={advertiseIp}
				disabled={!editor.editable}
				onValueChange={next => (advertiseIp = next)}
			>
				<Select.Trigger id="pod-advertise">{podAdvertiseLabel(advertiseIp)}</Select.Trigger>
				<Select.Content>
					<Select.Group>
						{#each advertiseOptions as option (option)}
							<Select.Item value={option} label={podAdvertiseLabel(option)}>
								{podAdvertiseLabel(option)}
							</Select.Item>
						{/each}
					</Select.Group>
				</Select.Content>
			</Select.Root>
		</Field.Field>
	</div>
</Field.FieldGroup>

{#if kind === 'client_tls'}
	<PodTlsFields bind:tls editable={editor.editable} admin={editor.admin} />
{/if}

<Button class="mt-6 w-full" disabled={!editor.editable || pending || !valid} onclick={save}>
	{#if pending}<Spinner data-icon="inline-start" />{/if}
	{m.common_save()}
</Button>

<Separator class="my-6" />

<RouteEditor {pod} />

<Separator class="my-6" />

<h3 class="text-sm font-medium">{m.editor_pod_dialed_by()}</h3>
{#if isClientIngress(pod.ingress.kind)}
	<p class="mt-1 text-xs text-muted-foreground">{m.editor_pod_dialed_by_client()}</p>
{:else if dialedBy.length === 0}
	<p class="mt-1 text-xs text-muted-foreground">{m.editor_pod_dialed_by_none()}</p>
{:else}
	<ul class="mt-2 grid gap-1">
		{#each dialedBy as { edge, source } (edge.id)}
			<li>
				<button
					type="button"
					class="flex w-full items-center gap-2 rounded px-2 py-1 text-start text-sm hover:bg-accent"
					onclick={() => editor.open({ kind: 'pod', id: source.id })}
				>
					<span class="min-w-0 truncate">{source.name}</span>
					<span class="ms-auto shrink-0 text-xs text-muted-foreground">{serverName(source.serverId)}</span>
				</button>
			</li>
		{/each}
	</ul>
{/if}

{#if editor.editable}
	<Button class="mt-6 w-full" variant="outline" onclick={remove}>
		<Trash2Icon />
		{m.editor_pod_delete_title()}
	</Button>
{/if}
