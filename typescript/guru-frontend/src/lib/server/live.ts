/**
 * Several gRPC server streams read as one sequence, for `query.live` bodies.
 *
 * Each stream is re-opened on its own when the control plane drops it (a
 * master restart, a transport blip) once it has delivered anything, with
 * exponential backoff; the source closure is called again, so a caller that
 * captures a watermark in it resumes from there. A failure before the first
 * event, or a status a retry cannot fix, is handed to the caller as an item
 * so the live query fails like a unary call would instead of hanging.
 */
import { ClientError, Status } from 'nice-grpc';
import { getRequestEvent } from '$app/server';
import { grpcFailure } from './errors.js';
import { SESSION_COOKIE } from './session.js';

export type StreamSource<E> = (signal: AbortSignal) => AsyncIterable<E>;
export type StreamItem<E> = { key: string; event: E } | { key: string; error: unknown };

/** Statuses a reopen may fix; anything else ends the stream with its error. */
const RETRYABLE: readonly Status[] = [
	Status.UNAVAILABLE,
	Status.CANCELLED,
	Status.DEADLINE_EXCEEDED,
	Status.INTERNAL,
	Status.UNKNOWN,
	Status.ABORTED,
	Status.RESOURCE_EXHAUSTED
];
const MAX_BACKOFF_MS = 30_000;

export class GrpcStreams<E> {
	readonly #signal: AbortSignal;
	readonly #open = new Map<string, AbortController>();
	readonly #queue: StreamItem<E>[] = [];
	#wake: (() => void) | undefined;

	/** `signal` is the live query's request signal: when it aborts, iteration ends. */
	constructor(signal: AbortSignal) {
		this.#signal = signal;
		signal.addEventListener('abort', () => this.#notify(), { once: true });
	}

	/** Starts reading `source` under `key`; a no-op while `key` is already open. */
	open(key: string, source: StreamSource<E>): void {
		if (this.#open.has(key) || this.#signal.aborted) return;
		const own = new AbortController();
		this.#open.set(key, own);
		const onParentAbort = () => own.abort();
		this.#signal.addEventListener('abort', onParentAbort, { once: true });
		const release = () => {
			this.#signal.removeEventListener('abort', onParentAbort);
			if (this.#open.get(key) === own) this.#open.delete(key);
		};
		void this.#run(key, source, own.signal, release);
	}

	/** Aborts that stream's call. */
	close(key: string): void {
		const own = this.#open.get(key);
		if (!own) return;
		this.#open.delete(key);
		own.abort();
	}

	closeAll(): void {
		for (const key of [...this.#open.keys()]) this.close(key);
	}

	async *[Symbol.asyncIterator](): AsyncIterator<StreamItem<E>> {
		while (true) {
			const item = this.#queue.shift();
			if (item) {
				yield item;
				continue;
			}
			if (this.#signal.aborted) return;
			await new Promise<void>(resolve => {
				this.#wake = resolve;
			});
		}
	}

	#push(item: StreamItem<E>): void {
		this.#queue.push(item);
		this.#notify();
	}

	#notify(): void {
		const wake = this.#wake;
		this.#wake = undefined;
		wake?.();
	}

	/**
	 * `release` drops the key's registration; it runs before an error item is
	 * queued, so a caller that reopens the key while handling that item (a
	 * graph snapshot listing the member again) starts a new stream instead of
	 * hitting the no-op in `open`.
	 */
	async #run(
		key: string,
		source: StreamSource<E>,
		signal: AbortSignal,
		release: () => void
	): Promise<void> {
		let attempt = 0;
		// Once the stream has delivered anything, every later failure is a
		// transport loss to bridge: a reopen that fails before its first event
		// (the master still restarting) must keep retrying.
		let delivered = false;
		try {
			while (!signal.aborted) {
				try {
					for await (const event of source(signal)) {
						if (signal.aborted) return;
						delivered = true;
						attempt = 0;
						this.#push({ key, event });
					}
					// The control plane closed the stream (a restart, a shutdown):
					// reopen. A stream that never delivered still closes cleanly only
					// when the master went away mid-open, so this is not an error.
				} catch (error) {
					if (signal.aborted) return;
					const retryable =
						delivered && error instanceof ClientError && RETRYABLE.includes(error.code);
					if (!retryable) {
						release();
						this.#push({ key, error });
						return;
					}
				}
				const delay = Math.min(1000 * 2 ** attempt, MAX_BACKOFF_MS);
				attempt += 1;
				await new Promise<void>(resolve => {
					const timer = setTimeout(done, delay);
					signal.addEventListener('abort', done, { once: true });
					function done() {
						clearTimeout(timer);
						signal.removeEventListener('abort', done);
						resolve();
					}
				});
			}
		} finally {
			release();
		}
	}
}

/**
 * The session a live query runs as, or `undefined` when the cookie is gone.
 * The layout load gates every page's first render, so a missing cookie here is
 * a reconnect after a logout, and the query ends quietly — see
 * [`streamFailure`] for why it must not redirect.
 */
export function liveSessionId(): string | undefined {
	return getRequestEvent().cookies.get(SESSION_COOKIE);
}

/**
 * What a stream failure does to the live query: a gRPC status is mapped like
 * a unary call, so a session cut mid-stream throws the redirect that sends the
 * page to `/auth`. The exception is a session already gone before the query
 * has yielded anything: that query is the reconnect the client opens at once
 * after a redirect frame, and redirecting it again would repeat without end
 * (the client backs off on nothing but transport errors, and keeps a live
 * query until its proxy is garbage-collected). Ending it instead is what stops
 * the client, so the caller returns on `'end'`.
 */
export function streamFailure(error: unknown, yielded: boolean): 'end' {
	if (!yielded && error instanceof ClientError && error.code === Status.UNAUTHENTICATED) {
		return 'end';
	}
	if (error instanceof ClientError) grpcFailure(error);
	throw error;
}
