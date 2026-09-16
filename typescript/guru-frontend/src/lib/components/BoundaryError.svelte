<script lang="ts">
import * as Empty from '#lib/components/ui/empty/index.js';
import { errorText } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * What a `<svelte:boundary>` puts on screen when the query inside it failed.
 * A boundary is how one failing read is kept from taking a whole page down, so
 * every one of them needs this — and they should all say it the same way.
 */
let { error }: { error: unknown } = $props();
</script>

<Empty.Root>
	<Empty.Header>
		<Empty.Title>{m.error_title()}</Empty.Title>
		<Empty.Description>{errorText(error)}</Empty.Description>
	</Empty.Header>
</Empty.Root>
