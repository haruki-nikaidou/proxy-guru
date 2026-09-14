import { m } from '#lib/paraglide/messages.js';
import type { Locale } from '#lib/paraglide/runtime.js';

/**
 * Display name of every locale, in its own language, keyed so that adding a
 * locale to `project.inlang/settings.json` without a label is a type error.
 * Each `m` function is referenced explicitly (no `m[\`nav_language_${l}\`]`)
 * so paraglide can still tree-shake the messages this menu does not use.
 */
export const LOCALE_LABELS: Record<Locale, () => string> = {
	en: m.nav_language_en,
	ja: m.nav_language_ja,
	'zh-CN': m.nav_language_zh_cn,
	'zh-TW': m.nav_language_zh_tw,
	ko: m.nav_language_ko
};
