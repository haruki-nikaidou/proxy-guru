import { toast } from 'svelte-sonner';
import { errorDetails, toAppError } from '#lib/errors.js';
import { appErrorText } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';

/** Puts an error's details on the clipboard, saying so when the browser refuses. */
export async function copyErrorDetails(error: App.Error): Promise<boolean> {
	try {
		await navigator.clipboard.writeText(errorDetails(error));
		return true;
	} catch {
		toast.error(m.common_copy_failed());
		return false;
	}
}

/**
 * Says in a toast what went wrong: the sentence for the kind of failure, what
 * exactly failed underneath, and a button that copies the details whole. A
 * refusal the control plane worded itself is complete on its own and toasts
 * alone.
 */
export function reportError(err: unknown): void {
	const error = toAppError(err);
	const text = appErrorText(error);
	const underneath = [
		error.detail?.split('\n')[0],
		error.id ? m.error_reference({ id: error.id }) : undefined
	]
		.filter(part => part !== undefined && part !== '')
		.join(' · ');
	if (underneath === '') {
		toast.error(text);
		return;
	}
	toast.error(text, {
		description: underneath,
		action: {
			label: m.error_copy_details(),
			onClick: () => void copyErrorDetails(error)
		}
	});
}
