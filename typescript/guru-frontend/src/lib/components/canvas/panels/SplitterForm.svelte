<script lang="ts">
import ArrowDownIcon from '@lucide/svelte/icons/arrow-down';
import ArrowUpIcon from '@lucide/svelte/icons/arrow-up';
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import { addSplitterMember, removeAll, type SplitterCard, setSplitterPolicy } from 'guru-graph';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { cardLabel } from '#lib/components/canvas/flow/nodes.js';
import RuleDots from '#lib/components/canvas/nodes/RuleDots.svelte';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Separator } from '#lib/components/ui/separator/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Switch } from '#lib/components/ui/switch/index.js';
import { policyLabel } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import { seedOn } from '#lib/seed.svelte.js';

/**
 * A splitter: the route nodes of several pods that choose the same way between
 * the same cards, edited as one. A change here rewrites every route it stands
 * for; a member added here is added for every one of those pods.
 */
let { card }: { card: SplitterCard } = $props();

const editor = useEditor();
const graph = $derived(editor.graph);
const drawing = $derived(editor.drawing);

let kind = $state<'balance' | 'failover'>('balance');
let sticky = $state(false);
/** The members in their new order: each one's old index and weight. */
let members = $state<{ from: number; weight: string }[]>([]);
let pending = $state(false);

// Reseeded when the stored splitter changes too, so a save never sends back
// values the form no longer shows the operator.
const shape = $derived(`${card.id}#${JSON.stringify([card.policy, card.sticky, card.weights])}`);
seedOn(
	() => shape,
	() => {
		kind = card.policy;
		sticky = card.sticky;
		members = card.weights.map((weight, from) => ({ from, weight: String(weight) }));
	}
);

const wayLabel = (index: number) => {
	const bus = drawing.buses.find(
		entry => entry.source.node === card.id && entry.source.handle === `out:${index}`
	);
	return bus ? cardLabel(graph, drawing, bus.target.node) : '';
};
const weightsValid = $derived(
	kind === 'failover' ||
		members.every(member => Number.isInteger(Number(member.weight)) && Number(member.weight) >= 1)
);
const dirty = $derived(
	kind !== card.policy ||
		sticky !== card.sticky ||
		members.some((member, i) => member.from !== i) ||
		(kind === 'balance' &&
			members.some(member => Number(member.weight) !== card.weights[member.from]))
);

function move(index: number, by: -1 | 1) {
	const other = index + by;
	if (other < 0 || other >= members.length) return;
	const next = [...members];
	[next[index], next[other]] = [
		next[other] as (typeof next)[number],
		next[index] as (typeof next)[number]
	];
	members = next;
}

async function save() {
	pending = true;
	const policy = {
		kind,
		sticky: kind === 'balance' && sticky,
		weights: members.map(member => (kind === 'balance' ? Number(member.weight) : 1)),
		order: members.map(member => member.from)
	};
	await editor.commit(
		() => setSplitterPolicy(editor.graph, editor.drawing, card.id, policy),
		m.editor_saved()
	);
	pending = false;
}

async function addMember() {
	const target = await editor.pickTarget({
		title: m.editor_target_title(),
		scope: editor.canvasId
	});
	if (!target) return;
	await editor.commit(
		() => addSplitterMember(editor.graph, editor.drawing, card.id, target),
		m.editor_member_added()
	);
}

function remove() {
	editor.review({
		title: m.editor_splitter_delete(),
		description: m.editor_splitter_delete_description({ count: card.members.length }),
		prunable: true,
		build: prune => removeAll(editor.graph, editor.drawing, { splitterIds: [card.id] }, prune),
		success: m.editor_deleted()
	});
}

const pods = $derived(
	card.members.flatMap(member => {
		const pod = graph.pods.find(entry => entry.id === member.podId);
		return pod ? [pod] : [];
	})
);
</script>

<Field.FieldGroup>
	<Field.Field>
		<Field.FieldLabel for="splitter-policy">{m.editor_splitter_policy()}</Field.FieldLabel>
		<Select.Root
			type="single"
			value={kind}
			disabled={!editor.editable}
			onValueChange={next => (kind = next as 'balance' | 'failover')}
		>
			<Select.Trigger id="splitter-policy">{policyLabel(kind)}</Select.Trigger>
			<Select.Content>
				<Select.Group>
					<Select.Item value="balance" label={policyLabel('balance')}>{policyLabel('balance')}</Select.Item>
					<Select.Item value="failover" label={policyLabel('failover')}>{policyLabel('failover')}</Select.Item>
				</Select.Group>
			</Select.Content>
		</Select.Root>
		<Field.FieldDescription>
			{kind === 'failover' ? m.editor_policy_failover_hint() : m.editor_policy_balance_hint()}
		</Field.FieldDescription>
	</Field.Field>

	{#if kind === 'balance'}
		<Field.Field orientation="horizontal">
			<Field.FieldContent>
				<Field.FieldLabel for="splitter-sticky">{m.editor_sticky()}</Field.FieldLabel>
				<Field.FieldDescription>{m.editor_sticky_hint()}</Field.FieldDescription>
			</Field.FieldContent>
			<Switch id="splitter-sticky" bind:checked={sticky} disabled={!editor.editable} />
		</Field.Field>
	{/if}
</Field.FieldGroup>

<h3 class="mt-6 text-sm font-medium">
	{kind === 'failover' ? m.editor_splitter_tiers() : m.editor_splitter_members()}
</h3>
<ul class="mt-2 grid gap-1">
	{#each members as member, index (member.from)}
		<li class="flex items-center gap-2">
			{#if kind === 'balance'}
				<Input
					type="number"
					min={1}
					class="h-8 w-16"
					bind:value={member.weight}
					disabled={!editor.editable}
					aria-label={m.editor_route_weight()}
				/>
			{:else}
				<span class="w-16 text-center font-mono text-xs text-muted-foreground">#{index + 1}</span>
			{/if}
			<span class="min-w-0 flex-1 truncate text-sm">{wayLabel(member.from)}</span>
			{#if editor.editable}
				<Button size="icon-xs" variant="ghost" disabled={index === 0} onclick={() => move(index, -1)} aria-label={m.editor_route_up()}>
					<ArrowUpIcon />
				</Button>
				<Button
					size="icon-xs"
					variant="ghost"
					disabled={index === members.length - 1}
					onclick={() => move(index, 1)}
					aria-label={m.editor_route_down()}
				>
					<ArrowDownIcon />
				</Button>
			{/if}
		</li>
	{/each}
</ul>

{#if editor.editable}
	<div class="mt-3 flex gap-2">
		<Button size="sm" class="flex-1" disabled={pending || !dirty || !weightsValid} onclick={save}>
			{#if pending}<Spinner data-icon="inline-start" />{/if}
			{m.common_save()}
		</Button>
		<Button size="sm" variant="outline" disabled={dirty} onclick={addMember}>
			{m.editor_splitter_add_member()}
		</Button>
	</div>
	<p class="mt-1 text-xs text-muted-foreground">{m.editor_splitter_add_member_hint()}</p>
{/if}

<Separator class="my-6" />

<h3 class="text-sm font-medium">{m.editor_splitter_stands_for()}</h3>
<ul class="mt-2 grid gap-1">
	{#each pods as pod (pod.id)}
		<li>
			<button
				type="button"
				class="flex w-full items-center gap-2 rounded px-2 py-1 text-start text-sm hover:bg-accent"
				onclick={() => editor.open({ kind: 'pod', id: pod.id })}
			>
				<RuleDots rules={drawing.rules.pods.get(pod.id) ?? []} max={4} />
				<span class="min-w-0 truncate">{pod.name}</span>
			</button>
		</li>
	{/each}
</ul>

{#if editor.editable}
	<Button class="mt-6 w-full" variant="outline" onclick={remove}>
		<Trash2Icon />
		{m.editor_splitter_delete()}
	</Button>
{/if}
