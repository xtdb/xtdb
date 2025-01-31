import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import tailwind from '@astrojs/tailwind';
import yaml from '@rollup/plugin-yaml';
import swup from '@swup/astro';
import adoc from '/shared/src/adoc';

// https://astro.build/config
export default defineConfig({
    site: 'https://docs.xtdb.com',

    integrations: [
        starlight({
            title: 'XTDB',

            social: {
                github: 'https://github.com/xtdb/xtdb',
                twitter: 'https://twitter.com/xtdb_com',
            },

            favicon: '/shared/favicon.svg',

            logo: {
                src: '/shared/public/images/logo-text.svg',
                replacesTitle: true
            },

            sidebar: [
                { label: '← xtdb.com', link: 'https://xtdb.com', attrs: { target: '_blank' } },

                { label: '← 1.x (stable release) docs', link: 'https://v1-docs.xtdb.com', attrs: { target: '_blank' } },

                {
                    label: 'Introduction',
                    collapsed: true,
                    items: [
                        'index', 'quickstart/sql-overview', 'intro/installation-via-docker'
                    ],
                },

                {
                    label: 'Tutorials',
                    collapsed: true,
                    items: [
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
                    ],
                },

                {
                    label: 'Drivers',
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
                    label: 'Reference',
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
                                { label: 'Aggregate functions', link: '/reference/main/stdlib/aggregates' },
                                { label: 'Other functions', link: '/reference/main/stdlib/other' }
                            ]
                        },
                    ]
                },

                {
                    label: 'Operations',
                    collapsed: true,
                    items: [
                        {
                            label: 'Guides',
                            collapsed: true,
                            items: [
                                'ops/guides/starting-with-aws',
                                'ops/guides/starting-with-azure',
                                'ops/guides/monitoring-with-grafana'
                            ],
                        },
                        {
                            label: 'Configuration',
                            collapsed: true,
                            items: [
                                { label: 'Overview', link: '/ops/config' },
                                'ops/config/clojure',

                                {
                                    label: 'Log',
                                    items: [
                                        { label: 'Overview', link: '/ops/config/log' },
                                        { label: 'Kafka', link: '/ops/config/log/kafka' },
                                    ],
                                },

                                { label: 'Storage', link: '/ops/config/storage' },
                                { label: 'Monitoring & Observability', link: '/ops/config/monitoring' },
                                { label: 'Authentication', link: '/ops/config/authentication' }
                            ]
                        },
                        { label: 'AWS', link: '/ops/aws' },
                        { label: 'Azure', link: '/ops/azure' },
                        { label: 'Google Cloud', link: '/ops/google-cloud' },
                        {
                            label: 'Troubleshooting',
                            collapsed: true,
                            items: [
                                {label: "Overview", link: 'ops/troubleshooting/overview' }
                            ],
                        },

                    ]
                },
                {
                    label: 'XTQL (Clojure)',
                    collapsed: true,
                    items: [
                        'xtql/tutorials/introducing-xtql',

                        { label: 'Learn XTQL Today ↗', link: '/static/learn-xtql-today-with-clojure.html', attrs: { target: '_blank' } },

                        {
                            label: 'Reference',
                            collapsed: true,
                            items: [
                                { label: 'Transactions/DML', link: '/reference/main/xtql/txs' },
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
                        { label: 'Mission', link: '/intro/why-xtdb' },
                        { label: 'XTDB at a glance', link: '/intro/what-is-xtdb' },
                        { label: 'How XTDB works', link: '/intro/data-model' },
                        // { label: 'Architecture', link: '/intro/architecture' },
                        // { label: 'Bitemporality', link: '/intro/bitemporality' }

                        { label: 'Community', link: '/intro/community' },
                        { label: 'Roadmap', link: '/intro/roadmap' },

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
            },
        }),

        tailwind({ applyBaseStyles: false }),

        adoc(),

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

    vite: {
        plugins: [yaml()]
    },
});
