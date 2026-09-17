<script lang="ts">
import { ModeWatcher } from 'mode-watcher';
import { onMount } from 'svelte';
import favicon from '#lib/assets/favicon.svg';
import { Toaster } from '#lib/components/ui/sonner/index.js';
import { isIgnorable } from '#lib/errors.js';
import { reportError } from '#lib/report.js';
import './layout.css';

let { children } = $props();

/**
 * What no boundary and no handler caught — an event handler that threw, a
 * promise nothing awaited — would otherwise only reach the console. It is
 * reported like any other failure, and the same failure repeating in quick
 * succession toasts once.
 */
onMount(() => {
	let lastKey = '';
	let lastAt = 0;
	const report = (err: unknown) => {
		if (isIgnorable(err)) return;
		const key = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
		const now = Date.now();
		if (key === lastKey && now - lastAt < 5000) return;
		lastKey = key;
		lastAt = now;
		reportError(err);
	};
	const onError = (event: ErrorEvent) => report(event.error ?? event.message);
	const onRejection = (event: PromiseRejectionEvent) => report(event.reason);
	window.addEventListener('error', onError);
	window.addEventListener('unhandledrejection', onRejection);
	return () => {
		window.removeEventListener('error', onError);
		window.removeEventListener('unhandledrejection', onRejection);
	};
});
</script>

<svelte:head><link rel="icon" href={favicon} /></svelte:head>
<ModeWatcher />
<Toaster />
{@render children()}
