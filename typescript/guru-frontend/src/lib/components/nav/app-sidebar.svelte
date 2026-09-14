<script lang="ts">
import LayoutDashboardIcon from '@lucide/svelte/icons/layout-dashboard';
import NetworkIcon from '@lucide/svelte/icons/network';
import ShieldCheckIcon from '@lucide/svelte/icons/shield-check';
import UsersIcon from '@lucide/svelte/icons/users';
import { page } from '$app/state';
import NavUser from '#lib/components/nav/nav-user.svelte';
import * as Sidebar from '#lib/components/ui/sidebar/index.js';
import type { Identity } from '#lib/dto/identity.js';
import { m } from '#lib/paraglide/messages.js';

let { identity }: { identity: Identity } = $props();

const items = [
	{ href: '/', label: () => m.nav_canvases(), icon: LayoutDashboardIcon },
	{ href: '/tls', label: () => m.nav_tls(), icon: ShieldCheckIcon },
	{ href: '/management', label: () => m.nav_management(), icon: UsersIcon }
];

// Longest matching prefix wins, so `/management` does not also light up `/`.
const activeHref = $derived(
	items
		.map(item => item.href)
		.filter(href => (href === '/' ? page.url.pathname === '/' : page.url.pathname.startsWith(href)))
		.reduce((best, href) => (href.length > best.length ? href : best), '')
);
</script>

<Sidebar.Root>
	<Sidebar.Header>
		<Sidebar.Menu>
			<Sidebar.MenuItem>
				<Sidebar.MenuButton size="lg">
					{#snippet child({ props })}
						<a href="/" {...props}>
							<div
								class="flex aspect-square size-8 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground"
							>
								<NetworkIcon class="size-4" />
							</div>
							<span class="font-semibold">{m.nav_brand()}</span>
						</a>
					{/snippet}
				</Sidebar.MenuButton>
			</Sidebar.MenuItem>
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
									<span>{item.label()}</span>
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
