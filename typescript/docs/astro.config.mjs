import starlight from '@astrojs/starlight';
import { defineConfig } from 'astro/config';
import starlightThemeBlack from 'starlight-theme-black';

/**
 * `starlight-theme-black` only translates navbar items declared by `slug` — it
 * ignores `translations` whenever an item carries a literal `label`. Keys are the
 * locale's `lang`, not the content directory: Astro normalises `currentLocale` to
 * the locale's first code, so a page under `zh-cn/` looks up `zh-CN`.
 */
const navLinkTranslations = (en, ja, zhCn) => ({
	en,
	ja,
	'zh-CN': zhCn
});

export default defineConfig({
	integrations: [
		starlight({
			title: 'Proxy Guru',
			description: 'A managed TCP/TLS proxy fabric: design a topology, ship one config per server.',
			defaultLocale: 'root',
			locales: {
				root: { label: 'English', lang: 'en' },
				ja: { label: '日本語', lang: 'ja' },
				'zh-cn': { label: '简体中文', lang: 'zh-CN' }
			},
			logo: {
				src: './src/assets/logo.svg',
				alt: 'Proxy Guru control tower'
			},
			favicon: '/favicon.svg',
			editLink: {
				baseUrl: 'https://github.com/haruki-nikaidou/proxy-guru/edit/main/typescript/docs/'
			},
			social: [
				{
					icon: 'github',
					label: 'GitHub',
					href: 'https://github.com/haruki-nikaidou/proxy-guru'
				}
			],
			sidebar: [
				{
					label: 'Start Here',
					translations: { ja: 'はじめに', 'zh-CN': '从这里开始' },
					items: [
						{
							label: 'Introduction',
							translations: { ja: '概要', 'zh-CN': '简介' },
							link: '/guides/introduction/'
						},
						{
							label: 'Local Development',
							translations: { ja: 'ローカル開発', 'zh-CN': '本地开发' },
							link: '/guides/local-development/'
						},
						{
							label: 'Prerequisites',
							translations: { ja: '前提条件', 'zh-CN': '前置条件' },
							link: '/guides/prerequisites/'
						},
						{
							label: 'Setup Database Schema',
							translations: { ja: 'データベーススキーマの設定', 'zh-CN': '配置数据库 Schema' },
							link: '/guides/setup-database-schema/'
						},
						{
							label: 'Deploy with Docker',
							translations: { ja: 'Docker でデプロイ', 'zh-CN': '使用 Docker 部署' },
							link: '/guides/deploy-with-docker/'
						},
						{
							label: 'Deploy Natively',
							translations: { ja: 'ネイティブ環境へのデプロイ', 'zh-CN': '原生部署' },
							link: '/guides/deploy-natively/'
						},
						{
							label: 'Install and Update Agents',
							translations: { ja: 'エージェントのインストールと更新', 'zh-CN': '安装与更新 Agent' },
							link: '/guides/agent-install/'
						}
					]
				},
				{
					label: 'Core Concepts',
					translations: { ja: '基本コンセプト', 'zh-CN': '核心概念' },
					items: [
						{
							label: 'Canvas',
							translations: { ja: 'キャンバス', 'zh-CN': '画布' },
							link: '/reference/canvas/'
						},
						{
							label: 'Nodes',
							translations: { ja: 'ノード', 'zh-CN': '节点' },
							link: '/reference/nodes/'
						},
						{
							label: 'Rollout',
							translations: { ja: 'ロールアウト', 'zh-CN': '发布' },
							link: '/reference/rollout/'
						}
					]
				},
				{
					label: 'Features',
					translations: { ja: '機能', 'zh-CN': '功能' },
					items: [
						{
							label: 'Health Monitor',
							translations: { ja: 'ヘルスモニター', 'zh-CN': '健康监控' },
							link: '/features/health-monitor/'
						},
						{
							label: 'ACME with DNS',
							translations: { ja: 'DNS を使った ACME', 'zh-CN': '基于 DNS 的 ACME' },
							link: '/features/acme-dns/'
						}
					]
				},
				{
					label: 'Configuration',
					translations: { ja: '設定', 'zh-CN': '配置' },
					items: [
						{
							label: 'Configuration Reference',
							translations: { ja: '設定リファレンス', 'zh-CN': '配置参考' },
							link: '/reference/configuration/'
						},
						{
							label: 'Independent Worker Deployment',
							translations: { ja: '独立ワーカーのデプロイ', 'zh-CN': '独立 Worker 部署' },
							link: '/guides/independent-worker/'
						}
					]
				},
				{
					label: 'Reference',
					translations: { ja: 'リファレンス', 'zh-CN': '参考' },
					items: [
						{
							label: 'Architecture',
							translations: { ja: 'アーキテクチャ', 'zh-CN': '架构' },
							link: '/reference/architecture/'
						},
						{
							label: 'Workspace Layout',
							translations: { ja: 'ワークスペース構成', 'zh-CN': '工作区结构' },
							link: '/reference/workspace-layout/'
						}
					]
				}
			],
			plugins: [
				starlightThemeBlack({
					navLinks: [
						{
							slug: 'guides/introduction',
							translations: navLinkTranslations('Docs', 'ドキュメント', '文档')
						},
						{
							slug: 'reference/architecture',
							translations: navLinkTranslations('Reference', 'リファレンス', '参考')
						},
						{
							label: 'GitHub',
							link: 'https://github.com/haruki-nikaidou/proxy-guru',
							attrs: { target: '_blank', rel: 'noreferrer' }
						}
					]
				})
			]
		})
	]
});
