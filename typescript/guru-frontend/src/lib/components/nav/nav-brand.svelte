<script lang="ts">
import logo from '#lib/assets/favicon.svg';
import * as Sidebar from '#lib/components/ui/sidebar/index.js';
import { m } from '#lib/paraglide/messages.js';
</script>

<!-- Sidebar header brand: the project logo (the same file the favicon uses)
     linking back to the canvas list. Shared by every sidebar so the mark and
     the wordmark can only ever be changed in one place. -->
<Sidebar.MenuItem>
	<Sidebar.MenuButton size="lg">
		{#snippet child({ props })}
			<a href="/" {...props}>
				<img src={logo} alt="" class="size-8 shrink-0" />
				<span class="font-semibold">{m.nav_brand()}</span>
			</a>
		{/snippet}
	</Sidebar.MenuButton>
</Sidebar.MenuItem>
