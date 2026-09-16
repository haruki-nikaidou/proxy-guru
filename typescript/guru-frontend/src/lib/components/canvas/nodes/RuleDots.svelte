<script lang="ts">
import { useCanvasContext } from '#lib/components/canvas/context.svelte.js';

/** One dot per rule, in the rule's colour; the name is the tooltip. */
let { rules, max = 8 }: { rules: string[]; max?: number } = $props();

const canvas = useCanvasContext();
const shown = $derived(rules.slice(0, max));
</script>

<span class="inline-flex shrink-0 items-center gap-0.5">
	{#each shown as rule (rule)}
		<span
			class="size-2 rounded-full"
			style="background: {canvas.ruleColor(rule)}"
			title={canvas.ruleName(rule)}
		></span>
	{/each}
	{#if rules.length > max}
		<span class="text-[10px] text-muted-foreground">+{rules.length - max}</span>
	{/if}
</span>
