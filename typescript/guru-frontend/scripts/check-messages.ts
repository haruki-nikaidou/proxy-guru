// Every locale in project.inlang/settings.json must carry exactly the keys of
// the base locale, with the same `{placeholders}` in each message. Paraglide
// only warns about a missing translation (and falls back to the base locale),
// which is how a half-translated locale ships unnoticed.
//
// Run: `bun scripts/check-messages.ts` (also the first step of `bun run check`).

import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const root = join(import.meta.dir, '..');
const settings = JSON.parse(readFileSync(join(root, 'project.inlang/settings.json'), 'utf8')) as {
	baseLocale: string;
	locales: string[];
};

type Catalogue = Record<string, string>;
const load = (locale: string): Catalogue => {
	const raw = JSON.parse(readFileSync(join(root, 'messages', `${locale}.json`), 'utf8')) as Catalogue;
	delete raw.$schema;
	return raw;
};
const placeholders = (text: string): string =>
	[...text.matchAll(/\{(\w+)\}/g)]
		.map((match) => match[1])
		.sort()
		.join(',');

const base = load(settings.baseLocale);
let failures = 0;
const fail = (line: string) => {
	failures += 1;
	console.error(line);
};

for (const locale of settings.locales) {
	if (locale === settings.baseLocale) continue;
	const catalogue = load(locale);
	for (const key of Object.keys(base)) {
		if (!(key in catalogue)) {
			fail(`${locale}: missing "${key}"`);
			continue;
		}
		if (placeholders(catalogue[key]) !== placeholders(base[key])) {
			fail(`${locale}: "${key}" placeholders differ from ${settings.baseLocale}`);
		}
	}
	for (const key of Object.keys(catalogue)) {
		if (!(key in base)) fail(`${locale}: unexpected "${key}" (not in ${settings.baseLocale})`);
	}
}

if (failures > 0) {
	console.error(`\n${failures} message problem(s).`);
	process.exit(1);
}
console.log(`messages OK: ${Object.keys(base).length} keys × ${settings.locales.length} locales`);
