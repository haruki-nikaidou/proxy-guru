import starlight from '@astrojs/starlight';
import { defineConfig } from 'astro/config';
import lucode from 'lucode-starlight';

export default defineConfig({
	integrations: [
		starlight({
			title: 'Proxy Guru',
			description: 'A managed TCP/TLS proxy fabric: design a topology, ship one config per server.',
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
					items: [
						{ label: 'Introduction', link: '/guides/introduction/' },
						{ label: 'Local Development', link: '/guides/local-development/' },
						{ label: 'Deployment', link: '/guides/deployment/' },
						{ label: 'Single-Host Native Deployment', link: '/guides/single-host/' }
					]
				},
				{
					label: 'Core Concepts',
					items: [{ label: 'Nodes', link: '/reference/nodes/' }]
				},
				{
					label: 'Configuration',
					items: [
						{ label: 'Configuration Reference', link: '/reference/configuration/' },
						{ label: 'Independent Worker Deployment', link: '/guides/independent-worker/' }
					]
				},
				{
					label: 'Reference',
					items: [
						{ label: 'Architecture', link: '/reference/architecture/' },
						{ label: 'Rollout Model', link: '/reference/rollout/' },
						{ label: 'Workspace Layout', link: '/reference/workspace-layout/' }
					]
				}
			],
			plugins: [
				lucode({
					navLinks: [
						{ label: 'Docs', link: '/guides/introduction/' },
						{ label: 'Reference', link: '/reference/architecture/' },
						{
							label: 'GitHub',
							link: 'https://github.com/haruki-nikaidou/proxy-guru',
							attrs: { target: '_blank', rel: 'noreferrer' }
						}
					],
					footerText: 'Proxy Guru — control plane and data plane for a managed proxy fabric.'
				})
			]
		})
	]
});
