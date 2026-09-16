import { animals, colors, uniqueNamesGenerator } from 'unique-names-generator';
import { getLocale, type Locale } from '#lib/paraglide/runtime.js';

/**
 * Default names for freshly created servers and nodes.
 *
 * A bare type label ("Entry", "エントリー") is useless the moment a canvas holds
 * two of a kind, and users rarely rename right away, so the type label gets a
 * random colour + animal suffix in the active locale. Colours and animals only:
 * the generator's adjective dictionary contains words like `broken` and `wrong`
 * that read as status on an infrastructure canvas.
 *
 * `unique-names-generator` ships English dictionaries only — no maintained npm
 * word lists exist for Japanese, Chinese or Korean — so those words are curated
 * here and fed to the same generator. A locale without a dictionary falls back
 * to the English one.
 */

/** `〜いろの` forms only, so they glue straight onto a noun. */
const JA_COLORS = [
	'あさぎいろの',
	'あいいろの',
	'うぐいすいろの',
	'きいろの',
	'きんいろの',
	'ぎんいろの',
	'くろいろの',
	'こんいろの',
	'さくらいろの',
	'しゅいろの',
	'しろいろの',
	'そらいろの',
	'ちゃいろの',
	'はいいろの',
	'ふじいろの',
	'べにいろの',
	'みずいろの',
	'みどりいろの',
	'むらさきいろの',
	'もえぎいろの',
	'ももいろの',
	'やまぶきいろの',
	'るりいろの',
	'わかくさいろの'
];

const JA_ANIMALS = [
	'アザラシ',
	'イタチ',
	'イルカ',
	'ウサギ',
	'オオカミ',
	'カピバラ',
	'カメ',
	'カワウソ',
	'キツネ',
	'クジラ',
	'クマ',
	'コアラ',
	'シカ',
	'スズメ',
	'タヌキ',
	'ツバメ',
	'ツル',
	'トカゲ',
	'ハクチョウ',
	'ハヤブサ',
	'パンダ',
	'フクロウ',
	'ペンギン',
	'ホタル',
	'ラッコ',
	'リス',
	'ワシ'
];

/** `〜色的` forms, so they glue straight onto a noun. */
const ZH_HANS_COLORS = [
	'赤色的',
	'橙色的',
	'黄色的',
	'绿色的',
	'青色的',
	'蓝色的',
	'紫色的',
	'金色的',
	'银色的',
	'白色的',
	'灰色的',
	'黑色的',
	'粉色的',
	'棕色的',
	'琥珀色的',
	'翠绿色的',
	'天蓝色的',
	'藏青色的',
	'绛红色的',
	'米色的',
	'墨色的',
	'茶色的',
	'朱红色的',
	'碧色的'
];
const ZH_HANS_ANIMALS = [
	'海豹',
	'水獭',
	'海豚',
	'兔子',
	'狼',
	'水豚',
	'乌龟',
	'狐狸',
	'鲸鱼',
	'熊',
	'考拉',
	'鹿',
	'麻雀',
	'狸猫',
	'燕子',
	'仙鹤',
	'蜥蜴',
	'天鹅',
	'游隼',
	'熊猫',
	'猫头鹰',
	'企鹅',
	'萤火虫',
	'松鼠',
	'老鹰',
	'小猫',
	'刺猬'
];
const ZH_HANT_COLORS = [
	'赤色的',
	'橙色的',
	'黃色的',
	'綠色的',
	'青色的',
	'藍色的',
	'紫色的',
	'金色的',
	'銀色的',
	'白色的',
	'灰色的',
	'黑色的',
	'粉色的',
	'棕色的',
	'琥珀色的',
	'翠綠色的',
	'天藍色的',
	'藏青色的',
	'絳紅色的',
	'米色的',
	'墨色的',
	'茶色的',
	'朱紅色的',
	'碧色的'
];
const ZH_HANT_ANIMALS = [
	'海豹',
	'水獺',
	'海豚',
	'兔子',
	'狼',
	'水豚',
	'烏龜',
	'狐狸',
	'鯨魚',
	'熊',
	'無尾熊',
	'鹿',
	'麻雀',
	'狸貓',
	'燕子',
	'仙鶴',
	'蜥蜴',
	'天鵝',
	'遊隼',
	'熊貓',
	'貓頭鷹',
	'企鵝',
	'螢火蟲',
	'松鼠',
	'老鷹',
	'小貓',
	'刺蝟'
];
/** Adnominal forms (`〜색`), read as "<colour> <animal>". */
const KO_COLORS = [
	'빨간',
	'주황',
	'노란',
	'초록',
	'파란',
	'남색',
	'보라',
	'금색',
	'은색',
	'하얀',
	'회색',
	'검은',
	'분홍',
	'갈색',
	'호박색',
	'하늘색',
	'자주색',
	'연두',
	'옥색',
	'먹색',
	'살구색',
	'청록'
];
const KO_ANIMALS = [
	'물개',
	'수달',
	'돌고래',
	'토끼',
	'늑대',
	'카피바라',
	'거북이',
	'여우',
	'고래',
	'곰',
	'코알라',
	'사슴',
	'참새',
	'너구리',
	'제비',
	'두루미',
	'도마뱀',
	'백조',
	'매',
	'판다',
	'부엉이',
	'펭귄',
	'반딧불이',
	'다람쥐',
	'독수리',
	'고양이',
	'고슴도치'
];

type Dictionary = { colors: string[]; animals: string[]; separator: string };
const DICTIONARIES: Partial<Record<Locale, Dictionary>> = {
	ja: { colors: JA_COLORS, animals: JA_ANIMALS, separator: '' },
	'zh-CN': { colors: ZH_HANS_COLORS, animals: ZH_HANS_ANIMALS, separator: '' },
	'zh-TW': { colors: ZH_HANT_COLORS, animals: ZH_HANT_ANIMALS, separator: '' },
	ko: { colors: KO_COLORS, animals: KO_ANIMALS, separator: ' ' }
};

const randomPair = (locale: Locale): string => {
	const dictionary = DICTIONARIES[locale];
	return dictionary
		? uniqueNamesGenerator({
				dictionaries: [dictionary.colors, dictionary.animals],
				length: 2,
				separator: dictionary.separator
			})
		: uniqueNamesGenerator({
				dictionaries: [colors, animals],
				length: 2,
				separator: '-',
				style: 'lowerCase'
			});
};

const NO_NAMES: ReadonlySet<string> = new Set();

/**
 * `<type label> <random pair>`, e.g. `Entry azure-otter` or
 * `エントリー るりいろのカワウソ`. Both dictionaries keep the result far below
 * the control plane's 128-character name limit.
 *
 * The generator draws independently, so `taken` (the names already on the
 * canvas) is used to reject collisions — the curated spaces are only ~650 pairs,
 * and a duplicate default is exactly the confusion this is meant to avoid.
 * After a bounded number of draws a counter is appended instead of looping.
 */
export function suggestName(
	typeLabel: string,
	locale: Locale = getLocale(),
	taken: ReadonlySet<string> = NO_NAMES
): string {
	for (let attempt = 0; attempt < 16; attempt += 1) {
		const candidate = `${typeLabel} ${randomPair(locale)}`;
		if (!taken.has(candidate)) return candidate;
	}
	const crowded = `${typeLabel} ${randomPair(locale)}`;
	for (let suffix = 2; ; suffix += 1) {
		const candidate = `${crowded} ${suffix}`;
		if (!taken.has(candidate)) return candidate;
	}
}
