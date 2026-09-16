<script lang="ts">
import ChevronsUpDownIcon from '@lucide/svelte/icons/chevrons-up-down';
import LanguagesIcon from '@lucide/svelte/icons/languages';
import LogOutIcon from '@lucide/svelte/icons/log-out';
import SunMoonIcon from '@lucide/svelte/icons/sun-moon';
import { resetMode, setMode } from 'mode-watcher';
import { goto } from '$app/navigation';
import type { Identity } from '#lib/dto/identity.js';
import * as Avatar from '#lib/components/ui/avatar/index.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import * as DropdownMenu from '#lib/components/ui/dropdown-menu/index.js';
import * as Sidebar from '#lib/components/ui/sidebar/index.js';
import { roleLabel } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import { LOCALE_LABELS } from '#lib/i18n/locales.js';
import { getLocale, locales, setLocale } from '#lib/paraglide/runtime.js';
import { logout } from '../../../routes/auth/auth.remote.js';

let { identity }: { identity: Identity } = $props();

// No avatars exist in the API; the initials stand in for one.
const initials = $derived(identity.email.slice(0, 2).toUpperCase());

async function handleLogout() {
	await logout();
	await goto('/auth', { invalidateAll: true });
}
</script>

<DropdownMenu.Root>
	<DropdownMenu.Trigger>
		{#snippet child({ props })}
			<Sidebar.MenuButton {...props} size="lg">
				<Avatar.Root class="size-8 rounded-lg">
					<Avatar.Fallback class="rounded-lg">{initials}</Avatar.Fallback>
				</Avatar.Root>
				<div class="grid flex-1 text-left text-sm leading-tight">
					<span class="truncate font-medium">{identity.email}</span>
					<span class="truncate text-xs text-muted-foreground">{roleLabel(identity.role)}</span>
				</div>
				<ChevronsUpDownIcon class="ml-auto size-4" />
			</Sidebar.MenuButton>
		{/snippet}
	</DropdownMenu.Trigger>

	<DropdownMenu.Content
		class="w-(--bits-dropdown-menu-anchor-width) min-w-56"
		side="top"
		align="end"
	>
		<DropdownMenu.Group>
			<DropdownMenu.GroupHeading class="truncate">{identity.email}</DropdownMenu.GroupHeading>
			<div class="px-2 pb-2">
				<Badge variant="secondary">{roleLabel(identity.role)}</Badge>
			</div>
		</DropdownMenu.Group>
		<DropdownMenu.Separator />

		<DropdownMenu.Sub>
			<DropdownMenu.SubTrigger>
				<SunMoonIcon />
				{m.nav_theme()}
			</DropdownMenu.SubTrigger>
			<DropdownMenu.SubContent>
				<DropdownMenu.Item onSelect={() => setMode('light')}>{m.nav_theme_light()}</DropdownMenu.Item>
				<DropdownMenu.Item onSelect={() => setMode('dark')}>{m.nav_theme_dark()}</DropdownMenu.Item>
				<DropdownMenu.Item onSelect={() => resetMode()}>{m.nav_theme_system()}</DropdownMenu.Item>
			</DropdownMenu.SubContent>
		</DropdownMenu.Sub>

		<DropdownMenu.Sub>
			<DropdownMenu.SubTrigger>
				<LanguagesIcon />
				{m.nav_language()}
			</DropdownMenu.SubTrigger>
			<DropdownMenu.SubContent>
				{#each locales as locale (locale)}
					<DropdownMenu.Item disabled={getLocale() === locale} onSelect={() => setLocale(locale)}>
						{LOCALE_LABELS[locale]()}
					</DropdownMenu.Item>
				{/each}
			</DropdownMenu.SubContent>
		</DropdownMenu.Sub>

		<DropdownMenu.Separator />
		<DropdownMenu.Item onSelect={handleLogout}>
			<LogOutIcon />
			{m.auth_sign_out()}
		</DropdownMenu.Item>
	</DropdownMenu.Content>
</DropdownMenu.Root>
