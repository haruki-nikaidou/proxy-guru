/**
 * The remote shell on a server's worker: what the session list carries, and the
 * frames the transcript stream (`/shell/[serverId]/[sessionId]`) sends as
 * server-sent events.
 */

/** The capability a worker advertises when it was started with `--remote-shell`. */
export const REMOTE_SHELL_CAPABILITY = 'remote_shell';

/** The worker refuses a longer command line. */
export const MAX_SHELL_COMMAND_BYTES = 64 * 1024;

export type ShellSessionDto = {
	sessionId: string;
	/** RFC 3339. */
	openedAt: string;
	/** The command running now; `null` while the shell is idle. */
	running: string | null;
	/** How many dashboards watch the session right now. */
	viewers: number;
};

export type ShellStreamName = 'stdout' | 'stderr';
export type ShellCloseReasonName = 'closed' | 'idle' | 'exited' | 'shutdown' | 'unknown';

/**
 * One transcript event, as the `data` of an SSE message. A frame that occupies
 * transcript positions (output, started, finished) is sent with the SSE `id`
 * set to the position right after it, so a reconnect resumes there.
 */
export type ShellFrame =
	/** Output bytes, base64-encoded: a chunk may end inside a UTF-8 sequence. */
	| { type: 'output'; stream: ShellStreamName; data: string }
	| { type: 'started'; command: string }
	| { type: 'finished'; exitCode: number }
	/** Positions the worker's ring buffer no longer held. */
	| { type: 'truncated'; dropped: number }
	/** The session ended; nothing follows. */
	| { type: 'closed'; reason: ShellCloseReasonName };
