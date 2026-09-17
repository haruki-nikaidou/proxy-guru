<script lang="ts">
import { page } from '$app/state';
import BoundaryError from '#lib/components/BoundaryError.svelte';
import { buttonVariants } from '#lib/components/ui/button/index.js';
import { m } from '#lib/paraglide/messages.js';

// `page.error` has been through `handleError` on the server or in the browser,
// so it carries a code and, for anything unexpected, what failed.
const error = $derived(page.error ?? { status: page.status, message: '' });
</script>

<div class="grid min-h-svh place-items-center p-4">
	<BoundaryError {error} reload>
		{#snippet actions()}
			<a href="/" class={buttonVariants({ variant: 'outline' })}>{m.common_back_home()}</a>
		{/snippet}
	</BoundaryError>
</div>
