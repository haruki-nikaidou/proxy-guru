<script lang="ts">
import { mode } from 'mode-watcher';
import { onMount } from 'svelte';
import type { Content, Mode } from 'svelte-jsoneditor';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import { cn } from '#lib/utils.js';
// The dark theme ships as plain CSS, so importing it here is SSR-safe — only
// the editor's JavaScript touches browser globals.
import 'svelte-jsoneditor/themes/jse-theme-dark.css';

// Text in, text out: the component knows nothing about what the document means.
let {
	value,
	onChange,
	readOnly = false,
	class: className
}: {
	value: string;
	onChange: (text: string) => void;
	readOnly?: boolean;
	class?: string;
} = $props();

type LoadedEditor = {
	JSONEditor: typeof import('svelte-jsoneditor').JSONEditor;
	textMode: Mode;
};

// `JSONEditor` reaches for `document` and `window` while it initialises, so it
// must never be part of the server bundle: it is pulled in after mount, and the
// placeholder below keeps the layout the same height across the swap.
let loaded = $state<LoadedEditor | null>(null);

onMount(async () => {
	const jse = await import('svelte-jsoneditor');
	loaded = { JSONEditor: jse.JSONEditor, textMode: jse.Mode.text };
});
</script>

<!-- `jse-*` variables are the library's only styling contract; mapping them to
     the app's tokens is what keeps the editor from arriving in its own blue
     theme. They are set here rather than in `layout.css` so the mapping lives
     with the component that owns the widget. -->
<div
	class={cn(
		'json-document-editor h-112 overflow-hidden rounded-md border',
		mode.current === 'dark' && 'jse-theme-dark',
		className
	)}
>
	{#if loaded}
		{@const Editor = loaded.JSONEditor}
		<Editor
			content={{ text: value }}
			mode={loaded.textMode}
			{readOnly}
			navigationBar={false}
			askToFormat={false}
			onChange={(content: Content) => {
				// Text mode hands the document back verbatim, so a syntactically
				// invalid document survives editing instead of being repaired
				// behind the operator's back. The menu can still switch to tree
				// mode — only possible while the text parses — which reports
				// `{ json }`; that one is serialised back to text.
				onChange('text' in content ? content.text : JSON.stringify(content.json, null, 2));
			}}
		/>
	{:else}
		<Skeleton class="size-full rounded-md" />
	{/if}
</div>

<style>
/* Scoped, so these never leak onto another jsoneditor instance. The dark theme
   class above still supplies the syntax colours; only the shell is remapped,
   and the app tokens it points at already flip with the colour scheme. */
.json-document-editor {
	--jse-theme-color: var(--secondary);
	--jse-theme-color-highlight: var(--muted);
	--jse-menu-color: var(--secondary-foreground);
	--jse-background-color: var(--card);
	--jse-text-color: var(--card-foreground);
	--jse-panel-background: var(--muted);
	--jse-main-border: none;
	--jse-font-family: var(--font-sans);
	--jse-font-size-mono: 0.8125rem;
}
</style>
