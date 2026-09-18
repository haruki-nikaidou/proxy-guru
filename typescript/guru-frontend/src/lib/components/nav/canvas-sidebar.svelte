<script lang="ts">
import BellIcon from '@lucide/svelte/icons/bell';
import CircleHelpIcon from '@lucide/svelte/icons/circle-help';
import HeartPulseIcon from '@lucide/svelte/icons/heart-pulse';
import SettingsIcon from '@lucide/svelte/icons/settings';
import WorkflowIcon from '@lucide/svelte/icons/workflow';
import { page } from '$app/state';
import NavBrand from '#lib/components/nav/nav-brand.svelte';
import NavUser from '#lib/components/nav/nav-user.svelte';
import * as Sidebar from '#lib/components/ui/sidebar/index.js';
import type { Identity } from '#lib/dto/identity.js';
import { m } from '#lib/paraglide/messages.js';

let { identity, canvasId }: { identity: Identity; canvasId: string } = $props();

const items = $derived([
	{ href: `/canvas/${canvasId}`, label: m.nav_canvas(), icon: WorkflowIcon },
	{ href: `/canvas/${canvasId}/health`, label: m.nav_health(), icon: HeartPulseIcon },
	{ href: `/canvas/${canvasId}/notifications`, label: m.nav_notifications(), icon: BellIcon },
	{ href: `/canvas/${canvasId}/settings`, label: m.nav_settings(), icon: SettingsIcon },
	{ href: `/canvas/${canvasId}/help`, label: m.nav_help(), icon: CircleHelpIcon }
]);

// Longest matching prefix wins, so `/health` does not also light up the editor.
const activeHref = $derived(
	items
		.map(item => item.href)
		.filter(href => page.url.pathname.startsWith(href))
		.reduce((best, href) => (href.length > best.length ? href : best), '')
);
</script>

<Sidebar.Root>
	<Sidebar.Header>
		<Sidebar.Menu>
			<NavBrand />
		</Sidebar.Menu>
	</Sidebar.Header>

	<Sidebar.Content>
		<Sidebar.Group>
			<Sidebar.Menu>
				{#each items as item (item.href)}
					<Sidebar.MenuItem>
						<Sidebar.MenuButton isActive={item.href === activeHref}>
							{#snippet child({ props })}
								<a href={item.href} {...props}>
									<item.icon />
									<span>{item.label}</span>
								</a>
							{/snippet}
						</Sidebar.MenuButton>
					</Sidebar.MenuItem>
				{/each}
			</Sidebar.Menu>
		</Sidebar.Group>
	</Sidebar.Content>

	<Sidebar.Footer>
		<Sidebar.Menu>
			<Sidebar.MenuItem>
				<NavUser {identity} />
			</Sidebar.MenuItem>
		</Sidebar.Menu>
	</Sidebar.Footer>
</Sidebar.Root>
