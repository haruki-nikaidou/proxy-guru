import { toast } from 'svelte-sonner';
import { errorText } from '#lib/i18n/codes.js';

/**
 * What every panel and dialog does around a write: keep its controls disabled
 * while the call is in flight, then report either the success message or the
 * control plane's own text. Remote functions reject with the app's error body,
 * so a refused write shows the reason the control plane gave — `errorMessage`
 * resolves its code, and falls back to the message when the code is unknown.
 *
 * Everything that must only happen on success (closing a dialog, clearing a
 * draft row, calling back) belongs inside `write`, where it runs before the
 * toast and never runs at all when the call fails.
 */
export type PanelWrites = {
	/** True while a write is in flight. */
	readonly pending: boolean;
	/**
	 * Runs `write`, then says `success`. A write whose result the panel renders
	 * in place passes no message: then only a failure is worth a toast.
	 */
	run(write: () => Promise<unknown>, success?: string): Promise<void>;
};

export function panelWrites(): PanelWrites {
	let pending = $state(false);
	return {
		get pending() {
			return pending;
		},
		async run(write, success) {
			pending = true;
			try {
				await write();
				if (success !== undefined) toast.success(success);
			} catch (err) {
				toast.error(errorText(err));
			} finally {
				pending = false;
			}
		}
	};
}
