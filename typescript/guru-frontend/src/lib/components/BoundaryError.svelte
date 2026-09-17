<script lang="ts">
import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
import RotateCcwIcon from '@lucide/svelte/icons/rotate-ccw';
import TriangleAlertIcon from '@lucide/svelte/icons/triangle-alert';
import type { Snippet } from 'svelte';
import { refreshAll } from '$app/navigation';
import CopyButton from '#lib/components/CopyButton.svelte';
import * as Alert from '#lib/components/ui/alert/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { errorDetails, toAppError } from '#lib/errors.js';
import { appErrorText } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * What a failure puts on screen: what kind of failure it was, what exactly
 * failed (under Details, with a copy button), and what can be done about it.
 * Every `<svelte:boundary>`, every failed query and the error page show
 * failures through this, so they all say it the same way — and none of them
 * says "the control plane is unreachable" about a bug in the page.
 *
 * Try again re-reads every query, then — given a boundary's `reset` — renders
 * the boundary again. A query's own failure (`query.error`) passes `retry`
 * instead: re-reading is all it needs. `page` fills the area it stands in;
 * `inline` is an alert for a section.
 */
let {
	error,
	reset,
	retry,
	title,
	variant = 'page',
	reload,
	actions
}: {
	error: unknown;
	reset?: () => void;
	/** Offer Try again without a boundary to reset. */
	retry?: boolean;
	title?: string;
	variant?: 'page' | 'inline';
	/** Offer reloading the page; by default only when reloading is the fix. */
	reload?: boolean;
	actions?: Snippet;
} = $props();

const appError = $derived(toAppError(error));
const text = $derived(appErrorText(appError));
const details = $derived(errorDetails(appError));
const reloadable = $derived(
	reload ?? (appError.code === 'stale_app' || appError.code === 'client_error')
);

const retryable = $derived(reset !== undefined || retry === true);
let retrying = $state(false);

async function tryAgain() {
	retrying = true;
	try {
		await refreshAll();
	} catch {
		// A read that fails again holds its own error, which this view shows.
	} finally {
		retrying = false;
	}
	reset?.();
}
</script>

{#snippet buttons(size: 'sm' | 'default')}
	{#if retryable || reloadable || actions}
		<div class="flex flex-wrap gap-2" class:justify-center={variant === 'page'}>
			{#if retryable}
				<Button variant="outline" {size} disabled={retrying} onclick={tryAgain}>
					{#if retrying}<Spinner data-icon="inline-start" />{:else}<RotateCcwIcon />{/if}
					{m.error_retry()}
				</Button>
			{/if}
			{#if reloadable}
				<Button variant="outline" {size} onclick={() => location.reload()}>
					<RefreshCwIcon />
					{m.error_reload()}
				</Button>
			{/if}
			{@render actions?.()}
		</div>
	{/if}
{/snippet}

{#snippet detailsBlock()}
	<details class="w-full text-start">
		<summary
			class="cursor-pointer text-xs text-muted-foreground select-none"
			class:text-center={variant === 'page'}
		>
			{m.error_details()}
		</summary>
		<div class="mt-2 flex items-start gap-1 rounded-lg bg-muted p-2">
			<pre
				class="max-h-48 min-w-0 flex-1 overflow-auto font-mono text-xs break-all whitespace-pre-wrap text-foreground">{details}</pre>
			<CopyButton value={details} label={m.error_copy_details()} size="icon-xs" class="shrink-0" />
		</div>
	</details>
{/snippet}

{#if variant === 'inline'}
	<Alert.Root variant="destructive">
		<TriangleAlertIcon />
		<Alert.Title>{title ?? m.error_title()}</Alert.Title>
		<Alert.Description class="grid gap-3">
			<span>{text}</span>
			{@render buttons('sm')}
			{@render detailsBlock()}
		</Alert.Description>
	</Alert.Root>
{:else}
	<Empty.Root>
		<Empty.Header>
			<Empty.Media variant="icon"><TriangleAlertIcon /></Empty.Media>
			<Empty.Title>{title ?? m.error_title()}</Empty.Title>
			<Empty.Description>{text}</Empty.Description>
		</Empty.Header>
		<Empty.Content class="max-w-lg">
			{@render buttons('default')}
			{@render detailsBlock()}
		</Empty.Content>
	</Empty.Root>
{/if}
