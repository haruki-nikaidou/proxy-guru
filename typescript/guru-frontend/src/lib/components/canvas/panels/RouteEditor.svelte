<script lang="ts">
import ArrowDownIcon from '@lucide/svelte/icons/arrow-down';
import ArrowUpIcon from '@lucide/svelte/icons/arrow-up';
import EllipsisIcon from '@lucide/svelte/icons/ellipsis';
import PlusIcon from '@lucide/svelte/icons/plus';
import { connect, type Edge, type Pod, putPod, removeAll, type Route, routes } from 'guru-graph';
import DialAddressDialog from '#lib/components/canvas/dialogs/DialAddressDialog.svelte';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import { Checkbox } from '#lib/components/ui/checkbox/index.js';
import * as DropdownMenu from '#lib/components/ui/dropdown-menu/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Switch } from '#lib/components/ui/switch/index.js';
import { ingressLabel, policyLabel } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import { seedOn } from '#lib/seed.svelte.js';

/**
 * A pod's route: a tree over its own out-edges. A balance spreads connections
 * over its members by weight (optionally sticky by client address), a failover
 * uses the first member that is alive, and either may hold the other. The shape
 * — policies, weights, order, nesting — is a draft until Save; adding or
 * removing a way on changes edges and is written at once.
 */
let { pod }: { pod: Pod } = $props();

const editor = useEditor();
const graph = $derived(editor.graph);

let draft = $state<Route | null>(null);
/** The members picked for nesting, in one group at a time. */
let picked = $state<{ group: string; indexes: number[] }>({ group: '', indexes: [] });
let dialing = $state<Edge | null>(null);
let pending = $state(false);

const stored = $derived(JSON.stringify(pod.route));
seedOn(
	() => `${pod.id}#${stored}`,
	() => {
		draft = pod.route;
		picked = { group: '', indexes: [] };
	}
);
const dirty = $derived(JSON.stringify(draft) !== stored);

const edgeOf = (id: string) => graph.edges.find(edge => edge.id === id);
const serverName = (id: string) => graph.servers.find(server => server.id === id)?.name ?? id;
function targetLabel(edge: Edge | undefined): { name: string; detail: string } {
	if (!edge) return { name: '?', detail: '' };
	if ('exit' in edge.target) {
		const exitId = edge.target.exit;
		const exit = graph.exits.find(entry => entry.id === exitId);
		return { name: exit?.name ?? exitId, detail: exit?.destination ?? '' };
	}
	const podId = edge.target.pod;
	const target = graph.pods.find(entry => entry.id === podId);
	return {
		name: target?.name ?? podId,
		detail: target ? `${serverName(target.serverId)} · ${ingressLabel(target.ingress.kind)}` : ''
	};
}

const keyOf = (path: number[]) => path.join('.');
const replace = (path: number[], next: Route) => {
	if (!draft) return;
	draft = routes.replaceAt(draft, path, next);
};

function setPolicy(path: number[], kind: 'balance' | 'failover') {
	const node = draft ? routes.at(draft, path) : null;
	if (!node || 'edge' in node) return;
	replace(path, routes.withPolicy(node, { kind, sticky: false, weights: routes.weightsOf(node) }));
}
function setSticky(path: number[], sticky: boolean) {
	const node = draft ? routes.at(draft, path) : null;
	if (!node || !('balance' in node)) return;
	replace(
		path,
		routes.withPolicy(node, { kind: 'balance', sticky, weights: routes.weightsOf(node) })
	);
}
function setWeight(groupPath: number[], index: number, value: string) {
	const group = draft ? routes.at(draft, groupPath) : null;
	const weight = Number(value);
	if (!group || !('balance' in group) || !Number.isInteger(weight) || weight < 1) return;
	const weights = routes.weightsOf(group);
	weights[index] = weight;
	replace(groupPath, routes.withChildren(group, routes.children(group), weights));
}
function move(groupPath: number[], index: number, by: -1 | 1) {
	const group = draft ? routes.at(draft, groupPath) : null;
	if (!group || 'edge' in group) return;
	const order = routes.children(group).map((_, i) => i);
	const other = index + by;
	if (other < 0 || other >= order.length) return;
	[order[index], order[other]] = [order[other] as number, order[index] as number];
	replace(groupPath, routes.reordered(group, order));
	picked = { group: '', indexes: [] };
}
function togglePick(groupPath: number[], index: number, on: boolean) {
	const group = keyOf(groupPath);
	const indexes = picked.group === group ? picked.indexes : [];
	picked = {
		group,
		indexes: on ? [...new Set([...indexes, index])] : indexes.filter(i => i !== index)
	};
}
function nest(groupPath: number[], kind: 'balance' | 'failover') {
	if (!draft) return;
	draft = routes.nest(draft, groupPath, picked.indexes, kind);
	picked = { group: '', indexes: [] };
}
function unnest(path: number[]) {
	if (!draft) return;
	draft = routes.unnest(draft, path);
	picked = { group: '', indexes: [] };
}

async function save() {
	pending = true;
	const route = draft;
	await editor.commit(() => putPod(editor.graph, { ...pod, route }), m.editor_saved());
	pending = false;
}

async function addWay(path: number[]) {
	const root = graph.canvases[0]?.id ?? editor.canvasId;
	const target = await editor.pickTarget({
		title: m.editor_target_title(),
		scope: root,
		excludePods: [pod.id]
	});
	if (!target) return;
	await editor.commit(() => connect(editor.graph, pod.id, target, path), m.editor_connected());
}

function removeWay(edgeId: string) {
	const current = editor.graph;
	editor.review({
		title: m.editor_way_remove_title(),
		description: m.editor_way_remove_description({ name: targetLabel(edgeOf(edgeId)).name }),
		prunable: true,
		build: prune => removeAll(current, editor.drawing, { edgeIds: [edgeId] }, prune),
		success: m.editor_deleted()
	});
}

const locked = $derived(!editor.editable || dirty);
</script>

{#snippet member(route: Route, path: number[], parent: Route | null)}
	{@const index = path[path.length - 1] ?? 0}
	{@const groupPath = path.slice(0, -1)}
	{@const siblings = parent ? routes.children(parent).length : 0}
	{#if parent}
		<div class="flex items-center gap-1">
			{#if editor.editable}
				<Checkbox
					checked={picked.group === keyOf(groupPath) && picked.indexes.includes(index)}
					onCheckedChange={on => togglePick(groupPath, index, on === true)}
					aria-label={m.editor_route_pick()}
				/>
			{/if}
			{#if 'balance' in parent}
				<Input
					type="number"
					min={1}
					class="h-7 w-14 px-1 text-xs"
					value={String(routes.weightsOf(parent)[index] ?? 1)}
					disabled={!editor.editable}
					aria-label={m.editor_route_weight()}
					onchange={event => setWeight(groupPath, index, event.currentTarget.value)}
				/>
			{:else}
				<span class="w-14 text-center font-mono text-xs text-muted-foreground">#{index + 1}</span>
			{/if}
			<div class="min-w-0 flex-1">{@render node(route, path)}</div>
			{#if editor.editable}
				<div class="flex shrink-0 flex-col">
					<button
						type="button"
						class="text-muted-foreground hover:text-foreground disabled:opacity-30"
						disabled={index === 0}
						aria-label={m.editor_route_up()}
						onclick={() => move(groupPath, index, -1)}
					>
						<ArrowUpIcon class="size-3" />
					</button>
					<button
						type="button"
						class="text-muted-foreground hover:text-foreground disabled:opacity-30"
						disabled={index === siblings - 1}
						aria-label={m.editor_route_down()}
						onclick={() => move(groupPath, index, 1)}
					>
						<ArrowDownIcon class="size-3" />
					</button>
				</div>
			{/if}
		</div>
	{:else}
		{@render node(route, path)}
	{/if}
{/snippet}

{#snippet node(route: Route, path: number[])}
	{#if 'edge' in route}
		{@const edge = edgeOf(route.edge)}
		{@const label = targetLabel(edge)}
		<div class="flex items-center gap-2 rounded-md border bg-background px-2 py-1">
			<div class="min-w-0 flex-1">
				<p class="truncate text-sm">→ {label.name}</p>
				<p class="truncate text-xs text-muted-foreground">
					{label.detail}
					{#if edge?.overrideIp || edge?.overridePort}
						· <span class="font-mono">{edge.overrideIp ?? ''}{edge.overridePort ? `:${edge.overridePort}` : ''}</span>
					{/if}
				</p>
			</div>
			{#if editor.editable && edge}
				<DropdownMenu.Root>
					<DropdownMenu.Trigger>
						{#snippet child({ props })}
							<Button {...props} size="icon-xs" variant="ghost" aria-label={m.common_actions()}>
								<EllipsisIcon />
							</Button>
						{/snippet}
					</DropdownMenu.Trigger>
					<DropdownMenu.Content align="end">
						{#if 'pod' in edge.target}
							<DropdownMenu.Item disabled={locked} onSelect={() => (dialing = edge)}>
								{m.editor_dial_title()}
							</DropdownMenu.Item>
						{/if}
						<DropdownMenu.Item variant="destructive" disabled={locked} onSelect={() => removeWay(edge.id)}>
							{m.editor_way_remove_title()}
						</DropdownMenu.Item>
					</DropdownMenu.Content>
				</DropdownMenu.Root>
			{/if}
		</div>
	{:else}
		{@const kind = 'balance' in route ? 'balance' : 'failover'}
		{@const members = routes.children(route)}
		{@const group = keyOf(path)}
		<div class="grid gap-1 rounded-md border border-dashed p-2">
			<div class="flex flex-wrap items-center gap-2">
				<Select.Root
					type="single"
					value={kind}
					disabled={!editor.editable}
					onValueChange={next => setPolicy(path, next as 'balance' | 'failover')}
				>
					<Select.Trigger size="sm" class="h-7 w-32">{policyLabel(kind)}</Select.Trigger>
					<Select.Content>
						<Select.Group>
							<Select.Item value="balance" label={policyLabel('balance')}>{policyLabel('balance')}</Select.Item>
							<Select.Item value="failover" label={policyLabel('failover')}>{policyLabel('failover')}</Select.Item>
						</Select.Group>
					</Select.Content>
				</Select.Root>
				{#if 'balance' in route}
					<label class="flex items-center gap-1 text-xs">
						<Switch
							checked={route.sticky === 'client_ip'}
							disabled={!editor.editable}
							onCheckedChange={on => setSticky(path, on)}
						/>
						{m.editor_sticky()}
					</label>
				{/if}
				{#if editor.editable}
					<div class="ms-auto flex gap-1">
						{#if path.length > 0}
							<Button size="xs" variant="ghost" onclick={() => unnest(path)}>{m.editor_route_unnest()}</Button>
						{/if}
						<Button size="xs" variant="ghost" disabled={locked} onclick={() => addWay(path)}>
							<PlusIcon />
							{m.editor_way_add()}
						</Button>
					</div>
				{/if}
			</div>
			{#each members as child, i (i)}
				{@render member(child, [...path, i], route)}
			{/each}
			{#if picked.group === group && picked.indexes.length >= 2}
				<div class="flex flex-wrap items-center gap-2 pt-1 text-xs">
					<span class="text-muted-foreground">{m.editor_route_nest({ count: picked.indexes.length })}</span>
					<Button size="xs" variant="secondary" onclick={() => nest(path, 'balance')}>
						{policyLabel('balance')}
					</Button>
					<Button size="xs" variant="secondary" onclick={() => nest(path, 'failover')}>
						{policyLabel('failover')}
					</Button>
				</div>
			{/if}
		</div>
	{/if}
{/snippet}

<div class="flex items-center gap-2">
	<h3 class="text-sm font-medium">{m.editor_route_title()}</h3>
	{#if dirty}
		<Badge variant="outline" class="ms-auto">{m.editor_route_unsaved()}</Badge>
	{/if}
</div>
<p class="mt-1 text-xs text-muted-foreground">{m.editor_route_description()}</p>

<div class="mt-3 grid gap-2">
	{#if draft === null}
		<p class="text-sm text-muted-foreground">
			{pod.ingress.kind.startsWith('relay_') || editor.graph.edges.some(edge => edge.sourcePodId === pod.id)
				? m.editor_route_none()
				: m.editor_route_none_client()}
		</p>
	{:else}
		{@render member(draft, [], null)}
	{/if}
	{#if editor.editable && (draft === null || 'edge' in draft)}
		<Button size="sm" variant="outline" disabled={locked} onclick={() => addWay([])}>
			<PlusIcon />
			{m.editor_way_add()}
		</Button>
	{/if}
	{#if dirty}
		<p class="text-xs text-muted-foreground">{m.editor_route_dirty_hint()}</p>
		<div class="flex gap-2">
			<Button size="sm" class="flex-1" disabled={pending} onclick={save}>
				{#if pending}<Spinner data-icon="inline-start" />{/if}
				{m.common_save()}
			</Button>
			<Button size="sm" variant="outline" disabled={pending} onclick={() => (draft = pod.route)}>
				{m.editor_route_reset()}
			</Button>
		</div>
	{/if}
</div>

<DialAddressDialog bind:edge={dialing} />
