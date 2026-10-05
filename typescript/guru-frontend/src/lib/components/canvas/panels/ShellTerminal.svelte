<script lang="ts">
import CornerDownLeftIcon from '@lucide/svelte/icons/corner-down-left';
import RotateCwIcon from '@lucide/svelte/icons/rotate-cw';
import { untrack } from 'svelte';
import { listShellSessions, sendShellCommand } from '#lib/components/canvas/commands.js';
import * as Alert from '#lib/components/ui/alert/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import * as InputGroup from '#lib/components/ui/input-group/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import {
	MAX_SHELL_COMMAND_BYTES,
	type ShellCloseReasonName,
	type ShellFrame,
	type ShellSessionDto,
	type ShellStreamName
} from '#lib/dto/shell.js';
import { toAppError } from '#lib/errors.js';
import { errorText, issueMessage } from '#lib/i18n/codes.js';
import { TRANSIENT } from '#lib/live.svelte.js';
import { m } from '#lib/paraglide/messages.js';
import { cn } from '#lib/utils.js';
import { panelWrites } from '#lib/writes.svelte.js';

/**
 * One remote shell session: its transcript, followed live over server-sent
 * events, and the line that sends it the next command. Mounted per session —
 * the parent keys it — and the stream is closed when it unmounts.
 */
let { serverId, session }: { serverId: string; session: ShellSessionDto } = $props();

type Segment =
	| { kind: ShellStreamName; text: string }
	| { kind: 'command'; text: string }
	| { kind: 'exit'; code: number }
	| { kind: 'truncated'; dropped: number }
	/** Older lines this view dropped to stay small. */
	| { kind: 'trimmed' };

/** Characters of scrollback the view holds before it drops the oldest. */
const MAX_SCROLLBACK = 1_000_000;
const TRIM_TO = 750_000;
const MAX_BACKOFF_MS = 30_000;
/** How close to the bottom still counts as following the output. */
const FOLLOW_SLACK_PX = 24;

const writes = panelWrites();
const encoder = new TextEncoder();

let segments = $state<Segment[]>([]);
/** Bumped on every change to the transcript, for the scroll effect. */
let revision = $state(0);
let running = $state<string | null>(null);
let status = $state<'connecting' | 'live' | 'reconnecting' | 'failed' | 'ended'>('connecting');
let failure = $state<unknown>(null);
let ended = $state<ShellCloseReasonName | 'gone' | null>(null);
let input = $state('');
let view = $state<HTMLElement | null>(null);
let inputRef = $state<HTMLInputElement | null>(null);

// Plain bookkeeping the template never reads.
let source: EventSource | null = null;
/** The transcript position everything shown so far ends at. */
let position = 0;
let held = 0;
let attempt = 0;
let retryTimer: ReturnType<typeof setTimeout> | undefined;
let disposed = false;
let follow = true;
// One streaming decoder per stream: a chunk may end inside a UTF-8 sequence.
let decoders: Record<ShellStreamName, TextDecoder> = {
	stdout: new TextDecoder(),
	stderr: new TextDecoder()
};

const tooLong = $derived(encoder.encode(input).length > MAX_SHELL_COMMAND_BYTES);
const inputDisabled = $derived(status === 'ended' || running !== null || writes.pending);
const canRun = $derived(!inputDisabled && input.trim() !== '' && !tooLong);

const weight = (segment: Segment): number => ('text' in segment ? segment.text.length : 1);

function push(segment: Segment) {
	const last = segments.at(-1);
	if (segment.kind === 'stdout' || segment.kind === 'stderr') {
		if (segment.text === '') return;
		// Consecutive output of one stream is one segment, to keep the DOM small.
		if ((last?.kind === 'stdout' || last?.kind === 'stderr') && last.kind === segment.kind) {
			last.text += segment.text;
		} else {
			segments.push(segment);
		}
	} else {
		segments.push(segment);
	}
	held += weight(segment);
	revision += 1;
	if (held > MAX_SCROLLBACK) trim();
}

/** Drops the oldest scrollback down to TRIM_TO, cutting into a segment if needed. */
function trim() {
	const kept = segments[0]?.kind === 'trimmed' ? 1 : 0;
	let excess = held - TRIM_TO;
	let index = kept;
	while (excess > 0 && index < segments.length) {
		const segment = segments[index];
		const size = weight(segment);
		if ('text' in segment && size > excess) {
			segment.text = segment.text.slice(excess);
			held -= excess;
			break;
		}
		excess -= size;
		held -= size;
		index += 1;
	}
	segments.splice(kept, index - kept);
	if (kept === 0) segments.unshift({ kind: 'trimmed' });
}

/** Ends both streams' pending multi-byte sequences before a marker line. */
function flush() {
	push({ kind: 'stdout', text: decoders.stdout.decode() });
	push({ kind: 'stderr', text: decoders.stderr.decode() });
}

function finish(reason: ShellCloseReasonName | 'gone') {
	flush();
	ended = reason;
	status = 'ended';
	running = null;
	source?.close();
	source = null;
}

function apply(frame: ShellFrame) {
	switch (frame.type) {
		case 'output': {
			const binary = atob(frame.data);
			const bytes = new Uint8Array(binary.length);
			for (let index = 0; index < binary.length; index += 1) {
				bytes[index] = binary.charCodeAt(index);
			}
			push({ kind: frame.stream, text: decoders[frame.stream].decode(bytes, { stream: true }) });
			break;
		}
		case 'started':
			flush();
			push({ kind: 'command', text: frame.command });
			running = frame.command;
			break;
		case 'finished':
			flush();
			push({ kind: 'exit', code: frame.exitCode });
			running = null;
			break;
		case 'truncated':
			// What a decoder still holds belongs to the bytes that are gone.
			decoders = { stdout: new TextDecoder(), stderr: new TextDecoder() };
			push({ kind: 'truncated', dropped: frame.dropped });
			break;
		case 'closed':
			finish(frame.reason);
			// The list says the session is gone; it shows its own failure.
			listShellSessions({ serverId })
				.refresh()
				.catch(() => undefined);
			break;
	}
}

function connect() {
	source?.close();
	const url = `/shell/${encodeURIComponent(serverId)}/${encodeURIComponent(session.sessionId)}?from=${position}`;
	const stream = new EventSource(url);
	stream.onopen = () => {
		status = 'live';
		attempt = 0;
	};
	stream.onmessage = event => {
		const frame = JSON.parse(event.data) as ShellFrame;
		// A reconnect may resume behind what is already shown; those frames
		// are skipped by their position. The others carry no position.
		if (
			frame.type === 'output' ||
			frame.type === 'started' ||
			frame.type === 'finished' ||
			frame.type === 'truncated'
		) {
			const end = Number(event.lastEventId);
			if (end <= position) return;
			position = end;
		}
		apply(frame);
	};
	stream.onerror = () => {
		if (stream.readyState === EventSource.CLOSED) void recover();
		else status = 'reconnecting';
	};
	source = stream;
}

/**
 * The relay refused the stream with an HTTP status, which ends an
 * `EventSource` for good and does not say why. The session list does: a
 * session no longer listed is gone, and otherwise the stream is opened again
 * after a backoff. A list that fails for a reason time fixes (the control
 * plane restarting) is retried on the same backoff; any other failure is
 * shown.
 */
async function recover() {
	source?.close();
	source = null;
	status = 'reconnecting';
	let sessions: ShellSessionDto[];
	try {
		const list = listShellSessions({ serverId });
		await list.refresh();
		sessions = await list;
	} catch (err) {
		if (disposed) return;
		if (TRANSIENT.has(toAppError(err).code ?? '')) {
			retryTimer = setTimeout(() => void recover(), nextBackoff());
			return;
		}
		failure = err;
		status = 'failed';
		return;
	}
	if (disposed) return;
	if (!sessions.some(entry => entry.sessionId === session.sessionId)) {
		finish('gone');
		return;
	}
	retryTimer = setTimeout(connect, nextBackoff());
}

function nextBackoff(): number {
	const delay = Math.min(1000 * 2 ** attempt, MAX_BACKOFF_MS);
	attempt += 1;
	return delay;
}

function reconnect() {
	failure = null;
	status = 'connecting';
	connect();
}

async function run(event: SubmitEvent) {
	event.preventDefault();
	if (!canRun) return;
	const command = input;
	// No success message: the transcript shows the command and its output.
	await writes.run(async () => {
		await sendShellCommand({ serverId, sessionId: session.sessionId, command });
		if (input === command) input = '';
	});
}

$effect(() => {
	untrack(() => {
		// What the list knew; the transcript corrects it as it replays.
		running = session.running;
		connect();
	});
	return () => {
		disposed = true;
		clearTimeout(retryTimer);
		source?.close();
		source = null;
	};
});

// Keeps the newest output in view while the operator has not scrolled up.
$effect(() => {
	void revision;
	if (view && follow) view.scrollTop = view.scrollHeight;
});

// The line is disabled while a command runs; hand the focus back after.
$effect(() => {
	if (!inputDisabled) inputRef?.focus();
});

function onscroll() {
	if (view) follow = view.scrollHeight - view.scrollTop - view.clientHeight < FOLLOW_SLACK_PX;
}
</script>

{#snippet line(segment: Segment)}
	{#if segment.kind === 'stdout'}
		<span>{segment.text}</span>
	{:else if segment.kind === 'stderr'}
		<span class="text-destructive">{segment.text}</span>
	{:else if segment.kind === 'command'}
		<span class="block font-semibold">$ {segment.text}</span>
	{:else if segment.kind === 'exit'}
		<span class={cn('block', segment.code === 0 ? 'text-muted-foreground' : 'text-destructive')}
			>{m.editor_shell_exit({ code: segment.code })}</span
		>
	{:else if segment.kind === 'truncated'}
		<span class="block text-muted-foreground italic"
			>{m.editor_shell_truncated({ count: segment.dropped })}</span
		>
	{:else}
		<span class="block text-muted-foreground italic">{m.editor_shell_trimmed()}</span>
	{/if}
{/snippet}

<div class="flex min-h-0 flex-1 flex-col gap-3">
	<div class="flex items-center gap-2 text-xs text-muted-foreground">
		<span class="truncate font-mono" title={session.sessionId}>{session.sessionId}</span>
		<span class="ms-auto flex shrink-0 items-center gap-1.5">
			{#if status === 'connecting'}
				<Spinner class="size-3" />{m.editor_shell_connecting()}
			{:else if status === 'reconnecting'}
				<Spinner class="size-3" />{m.editor_shell_reconnecting()}
			{:else if status === 'live'}
				{m.editor_shell_live()}
			{/if}
		</span>
	</div>

	<!-- Whitespace is the output's own: the segments sit inside the <pre>
	     with nothing between them. -->
	<pre
		bind:this={view}
		{onscroll}
		class="min-h-0 flex-1 overflow-auto rounded-md border bg-muted/40 p-3 font-mono text-xs break-all whitespace-pre-wrap">{#each segments as segment}{@render line(segment)}{/each}</pre>

	{#if status === 'failed'}
		<Alert.Root variant="destructive">
			<Alert.Title>{m.editor_shell_stream_failed()}</Alert.Title>
			<Alert.Description>{errorText(failure)}</Alert.Description>
			<Alert.Action>
				<Button size="sm" variant="outline" onclick={reconnect}>
					<RotateCwIcon data-icon="inline-start" />
					{m.editor_shell_reconnect()}
				</Button>
			</Alert.Action>
		</Alert.Root>
	{:else if ended !== null}
		<Alert.Root>
			<Alert.Title>{m.editor_shell_closed_title()}</Alert.Title>
			<Alert.Description>
				{#if ended === 'closed'}
					{m.editor_shell_closed_closed()}
				{:else if ended === 'idle'}
					{m.editor_shell_closed_idle()}
				{:else if ended === 'exited'}
					{m.editor_shell_closed_exited()}
				{:else if ended === 'shutdown'}
					{m.editor_shell_closed_shutdown()}
				{:else if ended === 'gone'}
					{m.editor_shell_closed_gone()}
				{:else}
					{m.editor_shell_closed_unknown()}
				{/if}
			</Alert.Description>
		</Alert.Root>
	{/if}

	<form onsubmit={run}>
		<Field.Field data-invalid={tooLong} data-disabled={inputDisabled}>
			<Field.FieldLabel for="shell-command-{session.sessionId}" class="sr-only">
				{m.editor_shell_command_label()}
			</Field.FieldLabel>
			<InputGroup.Root>
				<InputGroup.Addon>
					{#if running !== null || writes.pending}
						<Spinner />
					{:else}
						<span class="font-mono">$</span>
					{/if}
				</InputGroup.Addon>
				<InputGroup.Input
					id="shell-command-{session.sessionId}"
					bind:ref={inputRef}
					bind:value={input}
					disabled={inputDisabled}
					aria-invalid={tooLong}
					autocomplete="off"
					autocapitalize="off"
					spellcheck={false}
					class="font-mono"
					placeholder={running !== null
						? m.editor_shell_running()
						: m.editor_shell_command_placeholder()}
				/>
				<InputGroup.Addon align="inline-end">
					<InputGroup.Button type="submit" variant="default" disabled={!canRun}>
						<CornerDownLeftIcon data-icon="inline-start" />
						{m.editor_shell_run()}
					</InputGroup.Button>
				</InputGroup.Addon>
			</InputGroup.Root>
			{#if tooLong}
				<Field.FieldError>{issueMessage('shell_command_too_long')}</Field.FieldError>
			{/if}
		</Field.Field>
	</form>
</div>
