import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import yaml from '@rollup/plugin-yaml';
import swup from '@swup/astro';
import { remarkDefinitionList, defListHastHandlers } from 'remark-definition-list';
import { railroadPlugin } from './src/railroad-plugin.js';
import tailwindcss from '@tailwindcss/vite';
import mermaid from 'astro-mermaid';

// https://astro.build/config
export default defineConfig({
    site: 'https://docs.xtdb.com',

    integrations: [
        mermaid(),

        starlight({
            title: 'XTDB',

            social: [
                { icon: 'github', label: 'GitHub', href: 'https://github.com/xtdb/xtdb' },
                { icon: 'discord', label: 'Discord', href: 'https://discord.gg/xtdb' },
            ],

            favicon: '/shared/favicon.svg',

            logo: {
                src: '/shared/public/images/logo-text.svg',
                replacesTitle: true
            },

            sidebar: [
                { label: '← xtdb.com', link: 'https://xtdb.com', attrs: { target: '_blank' } },

                { label: '← 1.x docs', link: 'https://v1-docs.xtdb.com', attrs: { target: '_blank' } },

                {
                    label: 'Getting Started',
                    collapsed: true,
                    items: [
                        'index',
                        'quickstart/sql-overview',
                        'intro/installation-via-docker',
                        {
                            label: 'Industry Use-cases',
                            collapsed: true,
                            items: [
                                {
                                    label: 'Financial Services',
                                    items: [
                                        'tutorials/financial-usecase/time-in-finance',
                                        'tutorials/financial-usecase/commodities-pnl',
                                        'tutorials/financial-usecase/late-trade',
                                        'tutorials/financial-usecase/auditing-change',
                                        'tutorials/financial-usecase/counterparty-risk',
                                        'tutorials/financial-usecase/backtesting',
                                    ],
                                },
                            ],
                        },
                        {
                            label: 'Immutability Walkthrough',
                            collapsed: true,
                            items: [
                                'tutorials/immutability-walkthrough/part-1',
                                'tutorials/immutability-walkthrough/part-2',
                                'tutorials/immutability-walkthrough/part-3',
                                'tutorials/immutability-walkthrough/part-4'
                            ],
                        },
                    ],
                },

                {
                    label: 'About XTDB',
                    collapsed: true,
                    items: [
                        'about/mission',
                        'about/dbs-in-xtdb',
                        'about/txs-in-xtdb',
                        'about/time-in-xtdb',
                        { label: 'Community', link: '/intro/community'}
                    ]
                },

                {
                    label: 'Client Drivers',
                    collapsed: true,
                    items: [
                        { label: 'Overview', link: '/drivers' },
                        { label: 'Clojure', link: '/drivers/clojure' },
                        { label: 'Elixir', link: '/drivers/elixir' },
                        { label: 'Java', link: '/drivers/java' },
                        { label: 'Kotlin', link: '/drivers/kotlin' },
                        { label: 'Node.js', link: '/drivers/nodejs' },
                        { label: 'Python', link: '/drivers/python' },
                    ]
                },

                {
                    label: 'SQL Reference',
                    collapsed: true,
                    items: [
                        { label: 'Overview', link: '/reference/main' },
                        { label: 'SQL Transactions/DML', link: '/reference/main/sql/txs' },
                        { label: 'SQL Queries', link: '/reference/main/sql/queries' },
                        { label: 'Data Types', link: '/reference/main/data-types' },
                        {
                            label: 'Standard Library',
                            collapsed: true,
                            items: [
                                { label: 'Overview', link: '/reference/main/stdlib' },
                                { label: 'Predicates', link: '/reference/main/stdlib/predicates' },
                                { label: 'Numeric functions', link: '/reference/main/stdlib/numeric' },
                                { label: 'String functions', link: '/reference/main/stdlib/string' },
                                { label: 'Temporal functions', link: '/reference/main/stdlib/temporal' },
                                { label: 'URI functions', link: '/reference/main/stdlib/uri' },
                                { label: 'Aggregate functions', link: '/reference/main/stdlib/aggregates' },
                                { label: 'Other functions', link: '/reference/main/stdlib/other' }
                            ]
                        },
                    ]
                },

                {
                    label: 'Server Operations',
                    collapsed: true,
                    items: [
                        {
                            label: 'Cloud Deployment',
                            collapsed: true,
                            items: [
                                {
                                    label: 'AWS',
                                    items: [
                                        { label: 'Setup guide', link: '/ops/guides/starting-with-aws' },
                                        { label: 'Reference', link: '/ops/aws' },
                                    ]
                                },
                                {
                                    label: 'Azure',
                                    items: [
                                        { label: 'Setup guide', link: '/ops/guides/starting-with-azure' },
                                        { label: 'Reference', link: '/ops/azure' },
                                    ]
                                },
                                {
                                    label: 'GCP',
                                    items: [
                                        { label: 'Setup guide', link: '/ops/guides/starting-with-gcp' },
                                        { label: 'Reference', link: '/ops/google-cloud' },
                                    ]
                                },
                            ],
                        },
                        {
                            label: 'Configuration',
                            collapsed: true,
                            items: [
                                { label: 'Overview', link: '/ops/config' },
                                // { label: 'Databases', link: '/ops/config/databases' },
                                {
                                    label: 'Log',
                                    items: [
                                        { label: 'Overview', link: '/ops/config/log' },
                                        { label: 'Kafka', link: '/ops/config/log/kafka' },
                                    ],
                                },
                                { label: 'Storage', link: '/ops/config/storage' },
                                { label: 'Clojure config cookbook', link: '/ops/config/clojure' },
                            ]
                        },
                        {
                            label: 'Authentication',
                            collapsed: true,
                            items: [
                                { label: 'Overview', link: '/ops/config/authentication' },
                                { label: 'OpenID Connect', link: '/ops/config/authentication/oidc' },
                            ],
                        },
                        {
                            label: 'Backup and Restore',
                            collapsed: true,
                            items: [
                                { label: 'Overview', link: '/ops/backup-and-restore/overview' },
                                { label: 'Out of Sync Log & Intact Storage', link: '/ops/backup-and-restore/out-of-sync-log' },
                            ]
                        },
                        { label: 'Maintenance', link: '/ops/maintenance' },
                        {
                            label: 'Monitoring & Observability',
                            collapsed: true,
                            items: [
                                { label: 'Guide: Monitoring with Grafana', link: '/ops/guides/monitoring-with-grafana' },
                                { label: 'Reference', link: '/ops/config/monitoring' },
                            ]
                        },
                        { label: 'Troubleshooting', link: '/ops/troubleshooting' },
                    ]
                },

                {
                    label: 'XTQL',
                    collapsed: true,
                    items: [
                        'xtql/tutorials/introducing-xtql',

                        { label: 'Learn XTQL Today ↗', link: '/static/learn-xtql-today-with-clojure.html', attrs: { target: '_blank' } },

                        {
                            label: 'Reference',
                            collapsed: true,
                            items: [
                                { label: 'Transactions', link: '/reference/main/xtql/txs' },
                                { label: 'Queries', link: '/reference/main/xtql/queries' },
                                { label: 'Standard library', link: '/reference/main/xtql/stdlib' },
                            ]
                        }
                    ]
                },

                {
                    label: 'Appendices',
                    collapsed: true,
                    items: [
                        { label: 'XTDB at a glance', link: '/intro/what-is-xtdb' },
                        { label: 'Key Concepts', link: '/concepts/key-concepts' },
                    ],
                },
            ],

            customCss: [
                './src/styles/tailwind.css',
                './src/styles/railroad-diagrams.css',
            ],
            head: [
                {
                    tag: 'script',
                    attrs: {
                        src: 'https://bunseki.juxt.pro/umami.js',
                        'data-website-id': '2ff1e11e-b8fb-49d2-a3b2-9e77fead4b65',
                        defer: true,
                    },
                },
            ],

            components: {
                TableOfContents: './src/components/table-of-contents.astro',
                MobileTableOfContents: './src/components/mobile-table-of-contents.astro',
                Head: './src/components/Head.astro',
                SiteTitle: './src/components/SiteTitle.astro',
                Header: './src/components/Header.astro',
            },
        }),

        swup({
            theme: false,
            animationClass: false,
            containers: ['.main-frame', '.sidebar'],
            smoothScrolling: false,
            progress: true,
            globalInstance: true,
        }),
    ],

    trailingSlashes: "never",

    build: {
        format: "file"
    },

    markdown: {
        remarkPlugins: [railroadPlugin, remarkDefinitionList],
        remarkRehype: {
            handlers: { ...defListHastHandlers }
        }
    },

    vite: {
        plugins: [tailwindcss(), yaml()]
    },
});
