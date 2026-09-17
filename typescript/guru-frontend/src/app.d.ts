// See https://svelte.dev/docs/kit/types#app.d.ts
// for information about these interfaces
declare global {
	namespace App {
		interface Error {
			message: string;
			/** Stable lookup key for `#lib/i18n/codes.ts`; `server_message` = show `message`. */
			code?: string;
			/**
			 * What actually failed, in the words of whatever failed — a gRPC status and
			 * its details, an exception's name and message. Shown under "Details",
			 * never translated.
			 */
			detail?: string;
			/** Names the line the dashboard server logged for this error. */
			id?: string;
		}
		// interface Locals {}
		// interface PageData {}
		// interface PageState {}
		// interface Platform {}
	}
}

export {};
