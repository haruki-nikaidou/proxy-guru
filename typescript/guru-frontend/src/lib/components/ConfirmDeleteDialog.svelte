<script lang="ts">
import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
import { buttonVariants } from '#lib/components/ui/button/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * A plain "are you sure" for a destructive action the control plane will
 * validate itself. The caller owns the action and its error handling; this
 * only gates it.
 *
 * `open` takes a function binding where the caller keeps the target rather than
 * a flag: `bind:open={() => target !== null, next => { if (!next) target = null }}`.
 */
let {
	open = $bindable(false),
	title,
	description,
	confirm = m.common_delete(),
	pending = false,
	onconfirm
}: {
	open?: boolean;
	title: string;
	description: string;
	/** The destructive button's text; defaults to "Delete". */
	confirm?: string;
	pending?: boolean;
	onconfirm: () => void;
} = $props();
</script>

<AlertDialog.Root bind:open>
	<AlertDialog.Content>
		<AlertDialog.Header>
			<AlertDialog.Title>{title}</AlertDialog.Title>
			<AlertDialog.Description>{description}</AlertDialog.Description>
		</AlertDialog.Header>
		<AlertDialog.Footer>
			<AlertDialog.Cancel>{m.common_cancel()}</AlertDialog.Cancel>
			<AlertDialog.Action
				class={buttonVariants({ variant: 'destructive' })}
				disabled={pending}
				onclick={onconfirm}
			>
				{#if pending}<Spinner data-icon="inline-start" />{/if}
				{confirm}
			</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>
