<script lang="ts">
import PlusIcon from '@lucide/svelte/icons/plus';
import SendIcon from '@lucide/svelte/icons/send';
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import { toast } from 'svelte-sonner';
import { page } from '$app/state';
import BoundaryError from '#lib/components/BoundaryError.svelte';
import { Button } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import { Label } from '#lib/components/ui/label/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Switch } from '#lib/components/ui/switch/index.js';
import {
	NOTICE_KINDS,
	NOTIFY_LANGUAGES,
	type NoticeKindName,
	type NotifyLanguageName
} from '#lib/dto/notify.js';
import { errorText } from '#lib/i18n/codes.js';
import { noticeKindLabel, notifyLanguageLabel } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import { canEditWorkspace } from '#lib/permissions.js';
import { seedOn } from '#lib/seed.svelte.js';
import {
	getCanvasNotifySetting,
	getPersonalNotifySetting,
	saveCanvasNotifySetting,
	savePersonalNotifySetting,
	sendTestNotification
} from '../notifications.remote.js';
import type { PageProps } from './$types.js';

let { data }: PageProps = $props();

const canvasId = $derived(page.params.canvasId ?? '');
const editable = $derived(canEditWorkspace(data.identity.role));
const workspace = $derived(getCanvasNotifySetting({ canvasId }));
const mine = $derived(getPersonalNotifySetting({ canvasId }));

/**
 * The drafts. Both cards edit a set and a list, so the stored row is seeded
 * into local state once per canvas rather than bound to directly: a refresh
 * after a save must not wipe an edit in the other card.
 */
let groupLanguage = $state<NotifyLanguageName>('en');
let groupEvents = $state<NoticeKindName[]>([]);
let groupEmails = $state<string[]>([]);
let groupChats = $state<string[]>([]);
let mineLanguage = $state<NotifyLanguageName>('en');
let mineEvents = $state<NoticeKindName[]>([]);
let mineEmailEnabled = $state(false);
let mineChat = $state('');

let savingGroup = $state(false);
let savingMine = $state(false);
let testing = $state(false);

seedOn(
	() => (workspace.current ? `${canvasId}:${workspace.current.canvasId}` : null),
	() => {
		const setting = workspace.current;
		if (!setting) return;
		groupLanguage = setting.language;
		groupEvents = [...setting.events];
		groupEmails = [...setting.emails];
		groupChats = [...setting.telegramChats];
	}
);

seedOn(
	() => (mine.current ? `${canvasId}:mine` : null),
	() => {
		const setting = mine.current;
		if (!setting) return;
		mineLanguage = setting.language;
		mineEvents = [...setting.events];
		mineEmailEnabled = setting.emailEnabled;
		mineChat = setting.telegramChat;
	}
);

function toggle(events: NoticeKindName[], kind: NoticeKindName, on: boolean): NoticeKindName[] {
	// Declaration order, not click order: the set is what is stored, and a
	// stable order keeps the saved row comparable.
	return on
		? NOTICE_KINDS.filter(candidate => candidate === kind || events.includes(candidate))
		: events.filter(candidate => candidate !== kind);
}

async function saveGroup() {
	savingGroup = true;
	try {
		await saveCanvasNotifySetting({
			canvasId,
			language: groupLanguage,
			events: groupEvents,
			emails: groupEmails.map(email => email.trim()).filter(email => email.length > 0),
			telegramChats: groupChats.map(chat => chat.trim()).filter(chat => chat.length > 0)
		});
		toast.success(m.notify_saved());
	} catch (err) {
		toast.error(errorText(err));
	} finally {
		savingGroup = false;
	}
}

async function saveMine() {
	savingMine = true;
	try {
		await savePersonalNotifySetting({
			canvasId,
			language: mineLanguage,
			events: mineEvents,
			emailEnabled: mineEmailEnabled,
			telegramChat: mineChat.trim()
		});
		toast.success(m.notify_saved());
	} catch (err) {
		toast.error(errorText(err));
	} finally {
		savingMine = false;
	}
}

async function test() {
	testing = true;
	try {
		await sendTestNotification({ canvasId });
		toast.success(m.notify_test_sent());
	} catch (err) {
		toast.error(errorText(err));
	} finally {
		testing = false;
	}
}
</script>

<div class="flex flex-1 flex-col gap-6 p-4 md:p-8">
	<div>
		<h1 class="text-2xl font-semibold">{m.notify_title()}</h1>
		<p class="text-sm text-muted-foreground">{m.notify_description()}</p>
	</div>

	<svelte:boundary>
		{#if workspace.error}
			<BoundaryError error={workspace.error} retry />
		{:else if workspace.current === undefined}
			<Skeleton class="h-96 w-full max-w-2xl rounded-xl" />
		{:else}
			<Card.Root class="max-w-2xl">
				<Card.Header>
					<Card.Title>{m.notify_workspace_title()}</Card.Title>
					<Card.Description>{m.notify_workspace_description()}</Card.Description>
				</Card.Header>
				<Card.Content>
					<Field.FieldGroup>
						<Field.Field data-disabled={!editable}>
							<Field.FieldLabel for="group-language">{m.notify_language()}</Field.FieldLabel>
							<Select.Root
								type="single"
								value={groupLanguage}
								disabled={!editable}
								onValueChange={(next) => {
									groupLanguage = next as NotifyLanguageName;
								}}
							>
								<Select.Trigger id="group-language">
									{notifyLanguageLabel(groupLanguage)}
								</Select.Trigger>
								<Select.Content>
									{#each NOTIFY_LANGUAGES as language (language)}
										<Select.Item value={language}>{notifyLanguageLabel(language)}</Select.Item>
									{/each}
								</Select.Content>
							</Select.Root>
						</Field.Field>

						<Field.Field data-disabled={!editable}>
							<Field.FieldLabel>{m.notify_events()}</Field.FieldLabel>
							<Field.FieldDescription>{m.notify_events_hint()}</Field.FieldDescription>
							<div class="flex flex-col gap-3">
								{#each NOTICE_KINDS as kind (kind)}
									<div class="flex items-center justify-between gap-4">
										<Label for={`group-${kind}`}>{noticeKindLabel(kind)}</Label>
										<Switch
											id={`group-${kind}`}
											disabled={!editable}
											checked={groupEvents.includes(kind)}
											onCheckedChange={(on) => {
												groupEvents = toggle(groupEvents, kind, on);
											}}
										/>
									</div>
								{/each}
							</div>
						</Field.Field>

						<Field.Field data-disabled={!editable}>
							<Field.FieldLabel>{m.notify_emails()}</Field.FieldLabel>
							<Field.FieldDescription>{m.notify_emails_hint()}</Field.FieldDescription>
							{#each groupEmails as _email, index (index)}
								<div class="flex items-center gap-2">
									<Input
										type="email"
										disabled={!editable}
										placeholder="ops@example.com"
										bind:value={groupEmails[index]}
									/>
									<Button
										variant="ghost"
										size="icon"
										disabled={!editable}
										aria-label={m.notify_remove()}
										onclick={() => {
											groupEmails = groupEmails.filter((_, at) => at !== index);
										}}
									>
										<Trash2Icon />
									</Button>
								</div>
							{/each}
							{#if editable}
								<Button
									variant="outline"
									size="sm"
									class="self-start"
									onclick={() => {
										groupEmails = [...groupEmails, ''];
									}}
								>
									<PlusIcon data-icon="inline-start" />
									{m.notify_add_email()}
								</Button>
							{/if}
						</Field.Field>

						<Field.Field data-disabled={!editable}>
							<Field.FieldLabel>{m.notify_telegram_chats()}</Field.FieldLabel>
							<Field.FieldDescription>{m.notify_telegram_hint()}</Field.FieldDescription>
							{#each groupChats as _chat, index (index)}
								<div class="flex items-center gap-2">
									<Input
										disabled={!editable}
										placeholder="-1001234567890"
										class="font-mono"
										bind:value={groupChats[index]}
									/>
									<Button
										variant="ghost"
										size="icon"
										disabled={!editable}
										aria-label={m.notify_remove()}
										onclick={() => {
											groupChats = groupChats.filter((_, at) => at !== index);
										}}
									>
										<Trash2Icon />
									</Button>
								</div>
							{/each}
							{#if editable}
								<Button
									variant="outline"
									size="sm"
									class="self-start"
									onclick={() => {
										groupChats = [...groupChats, ''];
									}}
								>
									<PlusIcon data-icon="inline-start" />
									{m.notify_add_chat()}
								</Button>
							{/if}
						</Field.Field>
					</Field.FieldGroup>
				</Card.Content>
				{#if editable}
					<Card.Footer class="justify-end">
						<Button onclick={saveGroup} disabled={savingGroup}>
							{#if savingGroup}<Spinner data-icon="inline-start" />{/if}
							{m.common_save()}
						</Button>
					</Card.Footer>
				{/if}
			</Card.Root>
		{/if}

		{#if mine.error}
			<BoundaryError error={mine.error} retry />
		{:else if mine.current === undefined}
			<Skeleton class="h-96 w-full max-w-2xl rounded-xl" />
		{:else}
			<Card.Root class="max-w-2xl">
				<Card.Header>
					<Card.Title>{m.notify_mine_title()}</Card.Title>
					<Card.Description>{m.notify_mine_description()}</Card.Description>
				</Card.Header>
				<Card.Content>
					<Field.FieldGroup>
						<Field.Field>
							<Field.FieldLabel for="mine-language">{m.notify_language()}</Field.FieldLabel>
							<Select.Root
								type="single"
								value={mineLanguage}
								onValueChange={(next) => {
									mineLanguage = next as NotifyLanguageName;
								}}
							>
								<Select.Trigger id="mine-language">
									{notifyLanguageLabel(mineLanguage)}
								</Select.Trigger>
								<Select.Content>
									{#each NOTIFY_LANGUAGES as language (language)}
										<Select.Item value={language}>{notifyLanguageLabel(language)}</Select.Item>
									{/each}
								</Select.Content>
							</Select.Root>
						</Field.Field>

						<Field.Field>
							<Field.FieldLabel>{m.notify_events()}</Field.FieldLabel>
							<div class="flex flex-col gap-3">
								{#each NOTICE_KINDS as kind (kind)}
									<div class="flex items-center justify-between gap-4">
										<Label for={`mine-${kind}`}>{noticeKindLabel(kind)}</Label>
										<Switch
											id={`mine-${kind}`}
											checked={mineEvents.includes(kind)}
											onCheckedChange={(on) => {
												mineEvents = toggle(mineEvents, kind, on);
											}}
										/>
									</div>
								{/each}
							</div>
						</Field.Field>

						<Field.Field orientation="horizontal">
							<Field.FieldContent>
								<Field.FieldLabel for="mine-email">{m.notify_email_enabled()}</Field.FieldLabel>
								<Field.FieldDescription>
									{m.notify_email_enabled_hint({ email: data.identity.email })}
								</Field.FieldDescription>
							</Field.FieldContent>
							<Switch id="mine-email" bind:checked={mineEmailEnabled} />
						</Field.Field>

						<Field.Field>
							<Field.FieldLabel for="mine-chat">{m.notify_telegram_chat()}</Field.FieldLabel>
							<Field.FieldDescription>{m.notify_telegram_chat_hint()}</Field.FieldDescription>
							<Input id="mine-chat" class="font-mono" placeholder="12345" bind:value={mineChat} />
						</Field.Field>
					</Field.FieldGroup>
				</Card.Content>
				<Card.Footer class="justify-end gap-2">
					<Button variant="outline" onclick={test} disabled={testing}>
						{#if testing}
							<Spinner data-icon="inline-start" />
						{:else}
							<SendIcon data-icon="inline-start" />
						{/if}
						{m.notify_send_test()}
					</Button>
					<Button onclick={saveMine} disabled={savingMine}>
						{#if savingMine}<Spinner data-icon="inline-start" />{/if}
						{m.common_save()}
					</Button>
				</Card.Footer>
			</Card.Root>
		{/if}

		{#snippet failed(error, reset)}
			<BoundaryError {error} {reset} />
		{/snippet}
	</svelte:boundary>
</div>
