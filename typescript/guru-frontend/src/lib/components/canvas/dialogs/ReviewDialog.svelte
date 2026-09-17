<script lang="ts">
import { EditError, type GraphChange } from 'guru-graph';
import { untrack } from 'svelte';
import { toast } from 'svelte-sonner';
import { applyGraphChange } from '#lib/components/canvas/commands.js';
import {
	changeSummary,
	editErrorText,
	type ReviewRequest,
	useEditor
} from '#lib/components/canvas/editor.svelte.js';
import * as Alert from '#lib/components/ui/alert/index.js';
import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
import { buttonVariants } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Switch } from '#lib/components/ui/switch/index.js';
import type { ApplyOutcomeDto, CanvasGraph } from '#lib/dto/topology.js';
import { errorText } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import { reportError } from '#lib/report.js';

/**
 * A batch shown before it is applied: how much it takes away, and what the
 * control plane says about the graph it leads to (a dry run). Errors keep the
 * confirm button disabled; warnings are listed and do not.
 */
let {
	request = $bindable(null),
	graph,
	canvasId
}: { request?: ReviewRequest | null; graph: CanvasGraph; canvasId: string } = $props();

const editor = useEditor();
let prune = $state(true);

const built = $derived.by((): { change: GraphChange } | { error: string } | null => {
	if (!request) return null;
	try {
		return { change: request.build(request.prunable && prune) };
	} catch (err) {
		return { error: err instanceof EditError ? editErrorText(err.code) : errorText(err) };
	}
});
const change = $derived(built && 'change' in built ? built.change : null);
const summary = $derived(change ? changeSummary(change) : null);
const podNames = $derived(
	change
		? change.deletePodIds.map(id => graph.pods.find(pod => pod.id === id)?.name ?? id).slice(0, 12)
		: []
);

let dryRun = $state<ApplyOutcomeDto | null>(null);
let dryRunError = $state('');
let checking = $state(false);
$effect(() => {
	const batch = change;
	dryRun = null;
	dryRunError = '';
	if (!batch) return;
	let live = true;
	checking = true;
	// Untracked: a command counts its pending calls in state of its own, which
	// this effect would otherwise read and write, and run again forever.
	untrack(() => applyGraphChange({ canvasId, change: batch, dryRun: true }))
		.then(outcome => {
			if (live) dryRun = outcome;
		})
		.catch(err => {
			if (live) dryRunError = errorText(err);
		})
		.finally(() => {
			if (live) checking = false;
		});
	return () => {
		live = false;
	};
});
const errors = $derived(dryRun?.diagnostics.filter(diagnostic => diagnostic.error) ?? []);

let pending = $state(false);

/**
 * The batch goes through the editor, which reports a refusal itself; what has to
 * follow it (deleting servers, canvases) only runs once it is written.
 */
async function confirm() {
	const current = request;
	const batch = change;
	if (!current || !batch) return;
	pending = true;
	try {
		if (!(await editor.commit(batch))) return;
		await current.after?.();
		toast.success(current.success);
		request = null;
	} catch (err) {
		reportError(err);
	} finally {
		pending = false;
	}
}
</script>

<AlertDialog.Root
	open={request !== null}
	onOpenChange={open => {
		if (!open) request = null;
	}}
>
	<AlertDialog.Content class="sm:max-w-lg">
		<AlertDialog.Header>
			<AlertDialog.Title>{request?.title}</AlertDialog.Title>
			<AlertDialog.Description>{request?.description}</AlertDialog.Description>
		</AlertDialog.Header>

		{#if built && 'error' in built}
			<Alert.Root variant="destructive">
				<Alert.Description>{built.error}</Alert.Description>
			</Alert.Root>
		{:else if summary}
			<ul class="grid gap-1 text-sm">
				{#if summary.podsRemoved > 0}
					<li>
						{m.editor_review_pods_removed({ count: summary.podsRemoved })}
						<span class="text-xs text-muted-foreground">
							{podNames.join(', ')}{summary.podsRemoved > podNames.length ? ' …' : ''}
						</span>
					</li>
				{/if}
				{#if summary.edgesRemoved > 0}
					<li>{m.editor_review_edges_removed({ count: summary.edgesRemoved })}</li>
				{/if}
				{#if summary.exitsRemoved > 0}
					<li>{m.editor_review_exits_removed({ count: summary.exitsRemoved })}</li>
				{/if}
				{#if summary.podsWritten > 0}
					<li>{m.editor_review_pods_written({ count: summary.podsWritten })}</li>
				{/if}
				{#each request?.alsoDeletes?.servers ?? [] as name (name)}
					<li>{m.editor_review_server_deleted({ name })}</li>
				{/each}
				{#each request?.alsoDeletes?.canvases ?? [] as name (name)}
					<li>{m.editor_review_canvas_deleted({ name })}</li>
				{/each}
			</ul>
		{/if}

		{#if request?.prunable}
			<Field.Field orientation="horizontal">
				<Field.FieldContent>
					<Field.FieldLabel for="review-prune">{m.editor_review_prune()}</Field.FieldLabel>
					<Field.FieldDescription>{m.editor_review_prune_hint()}</Field.FieldDescription>
				</Field.FieldContent>
				<Switch id="review-prune" bind:checked={prune} />
			</Field.Field>
		{/if}

		{#if checking}
			<p class="flex items-center gap-2 text-xs text-muted-foreground">
				<Spinner class="size-3" />
				{m.editor_review_checking()}
			</p>
		{:else if dryRunError}
			<Alert.Root variant="destructive">
				<Alert.Description>{dryRunError}</Alert.Description>
			</Alert.Root>
		{:else if errors.length > 0}
			<Alert.Root variant="destructive">
				<Alert.Title>{m.editor_review_refused()}</Alert.Title>
				<Alert.Description>
					<ul class="list-disc ps-4">
						{#each errors as diagnostic, index (index)}
							<li>{diagnostic.message}</li>
						{/each}
					</ul>
				</Alert.Description>
			</Alert.Root>
		{:else if dryRun}
			<p class="text-xs text-muted-foreground">{m.editor_review_ok()}</p>
		{/if}

		<AlertDialog.Footer>
			<AlertDialog.Cancel>{m.common_cancel()}</AlertDialog.Cancel>
			<AlertDialog.Action
				class={buttonVariants({ variant: 'destructive' })}
				disabled={pending || checking || !dryRun || errors.length > 0 || !change}
				onclick={confirm}
			>
				{#if pending}<Spinner data-icon="inline-start" />{/if}
				{m.editor_review_confirm()}
			</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>
