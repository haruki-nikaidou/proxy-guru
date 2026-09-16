/**
 * Ids the dashboard mints for rows it creates: the shape every stored id has, 20
 * characters of `[a-z0-9]`, so one batch can create a pod, the edges into and
 * out of it and the routes naming them before any of them exists.
 */

const ALPHABET = 'abcdefghijklmnopqrstuvwxyz0123456789';
const KEY_LEN = 20;

export function newId(): string {
	const bytes = new Uint8Array(KEY_LEN);
	crypto.getRandomValues(bytes);
	let out = '';
	for (const byte of bytes) out += ALPHABET[byte % ALPHABET.length];
	return out;
}

export const isRecordKey = (key: string): boolean => /^[a-z0-9]{20}$/.test(key);

/** FNV-1a over the UTF-16 code units, as 13 base-36 digits: stable, short, not secret. */
export function hashKey(text: string): string {
	let hash = 0xcbf29ce484222325n;
	const prime = 0x100000001b3n;
	const mask = (1n << 64n) - 1n;
	for (let i = 0; i < text.length; i += 1) {
		hash ^= BigInt(text.charCodeAt(i));
		hash = (hash * prime) & mask;
	}
	return hash.toString(36).padStart(13, '0');
}
