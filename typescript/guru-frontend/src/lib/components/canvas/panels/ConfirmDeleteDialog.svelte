<script lang="ts">
import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
import { buttonVariants } from '#lib/components/ui/button/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * A plain "are you sure" for a delete the control plane will validate itself.
 * The caller owns the action and its error handling; this only gates it.
 */
let {
	open = $bindable(false),
	title,
	description,
	pending = false,
	onconfirm
}: {
	open?: boolean;
	title: string;
	description: string;
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
				{m.common_delete()}
			</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>
