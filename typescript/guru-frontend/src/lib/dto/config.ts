/**
 * The `app_config` documents an Admin may read and replace from the dashboard.
 * Protobuf never reaches the client, so a document is plain text here.
 *
 * Reads are **undecoded**: `json` is the row exactly as stored (pretty-printed),
 * so a document that no longer matches its type stays visible and repairable
 * instead of being hidden behind `Default`. `stored` is false when the key has
 * no row at all — a normal state on an unseeded installation — and `json` then
 * equals `defaultsJson`.
 *
 * `defaultsJson` is always what `manage-tool config seed` would write, so the
 * UI can offer the defaults verbatim.
 *
 * A write replaces the **whole** document: the control plane decodes the
 * payload into the config type first, so a wrong-shaped payload is rejected and
 * the row keeps its previous contents. `guru-master` loads every key once at
 * startup, so a saved change needs a restart to take effect — there is no live
 * reload.
 */

export type ConfigKeyName = 'auth' | 'notify' | 'orchestration';

export type ConfigDocumentDto = {
	key: ConfigKeyName;
	stored: boolean;
	json: string;
	defaultsJson: string;
};
