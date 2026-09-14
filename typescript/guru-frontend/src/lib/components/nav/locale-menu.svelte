<script lang="ts">
import LanguagesIcon from '@lucide/svelte/icons/languages';
import { Button } from '#lib/components/ui/button/index.js';
import * as DropdownMenu from '#lib/components/ui/dropdown-menu/index.js';
import { m } from '#lib/paraglide/messages.js';
import { LOCALE_LABELS } from '#lib/i18n/locales.js';
import { getLocale, locales, setLocale } from '#lib/paraglide/runtime.js';
</script>

<DropdownMenu.Root>
	<DropdownMenu.Trigger>
		{#snippet child({ props })}
			<Button {...props} variant="ghost" size="icon" aria-label={m.nav_language()}>
				<LanguagesIcon />
			</Button>
		{/snippet}
	</DropdownMenu.Trigger>
	<DropdownMenu.Content align="end">
		<DropdownMenu.Group>
			<DropdownMenu.GroupHeading>{m.nav_language()}</DropdownMenu.GroupHeading>
			<DropdownMenu.Separator />
			{#each locales as locale (locale)}
				<DropdownMenu.Item disabled={getLocale() === locale} onSelect={() => setLocale(locale)}>
					{LOCALE_LABELS[locale]()}
				</DropdownMenu.Item>
			{/each}
		</DropdownMenu.Group>
	</DropdownMenu.Content>
</DropdownMenu.Root>
