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

                { label: 'Introduction', link: '/index.html' },
                { label: 'SQL Quickstart', link: '/quickstart/sql-overview' },
                { label: 'Installation via Docker', link: '/intro/installation-via-docker' },

                {
                    label: 'Tutorials',
                    collapsed: true,
                    items: [
                        {
                            label: 'Immutability Walkthrough',
                            collapsed: false,
                            items: [
                                { label: '1) Avoiding a lossy database', link: '/tutorials/immutability-walkthrough/part-1' },
                                { label: '2) Understanding change', link: '/tutorials/immutability-walkthrough/part-2' },
                                { label: '3) Updating the past', link: '/tutorials/immutability-walkthrough/part-3' },
                                { label: '4) Changing an immutable database', link: '/tutorials/immutability-walkthrough/part-4' },
                            ],
                        },

                        { label: 'Introducing XTQL', link: '/tutorials/introducing-xtql' },
                    ],
                },

                {
                    label: 'Industry Use-cases',
                    collapsed: true,
                    items: [
                        {
                            label: 'Financial Services',
                            collapsed: false,
                            items: [
                                { label: 'Time in Finance', link: '/tutorials/financial-usecase/time-in-finance' },
                                { label: 'Understanding P&L and Risk', link: '/tutorials/financial-usecase/commodities-pnl' },
                                { label: 'Late trade adjustments', link: '/tutorials/financial-usecase/late-trade' },
				{ label: 'Auditing past trade adjustments', link: '/tutorials/financial-usecase/auditing-change' },
                                { label: 'Analysing counterparty risk', link: '/tutorials/financial-usecase/counterparty-risk' },
                                { label: 'Model backtesting', link: '/tutorials/financial-usecase/backtesting' },
                            ],
                        },
                    ],
                },

                {
                    label: 'Guides',
                    collapsed: true,
                    items: [
                        { label: 'Setting up a cluster on AWS', link: '/guides/starting-with-aws' },
                    ],
                },

                {
                    label: 'Reference',
                    collapsed: true,
                    items: [
                        { label: 'Overview', link: '/reference/main' },

                        {
                            label: 'SQL',
                            collapsed: true,
                            items: [
                                { label: 'Transactions', link: '/reference/main/sql/txs' },
                                { label: 'Queries', link: '/reference/main/sql/queries' },
                            ]
                        },

                        {
                            label: 'XTQL (Clojure)',
                            collapsed: true,
                            items: [
                                { label: 'Transactions', link: '/reference/main/xtql/txs' },
                                { label: 'Queries', link: '/reference/main/xtql/queries' },
                            ]
                        },
                        {
                            label: 'Data Types',
                            collapsed: false,
                            items: [
                                { label: 'Overview', link: '/reference/main/data-types' },
                                { label: 'Temporal Types', link: '/reference/main/data-types/temporal-types' },
                            ]
                        },

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
                    label: 'Drivers',
                    collapsed: true,
                    items: [
                        { label: 'Overview', link: '/drivers' },
                        {
                            label: 'Postgres',
                            collapsed: false,
                            items: [
                                { label: 'Getting started', link: '/drivers/postgres/getting-started' },
                            ]
                        },

                        {
                            label: 'HTTP + JSON',
                            collapsed: true,
                            items: [
                                { label: 'Getting started', link: '/drivers/http/getting-started' },
                                { label: 'HTTP (OpenAPI) ↗', link: '/drivers/http/openapi/index.html', attrs: { target: '_blank' } },
                            ]
                        },

                        {
                            label: 'Java',
                            collapsed: true,
                            items: [
                                { label: 'Getting started', link: '/drivers/java/getting-started' },
                                // TODO broken atm
                                // { label: 'Javadoc ↗', link: '/drivers/java/javadoc/index.html', attrs: {target: '_blank'} }
                            ]
                        },

                        {
                            label: 'Kotlin',
                            collapsed: true,
                            items: [
                                { label: 'Getting started', link: '/drivers/kotlin/getting-started' },
                                { label: 'KDoc ↗', link: '/drivers/kotlin/kdoc/index.html', attrs: { target: '_blank' } }
                            ]
                        },

                        {
                            label: 'Clojure',
                            collapsed: true,
                            items: [
                                { label: 'Getting started', link: '/drivers/clojure/getting-started' },
                                { label: 'Codox ↗', link: '/drivers/clojure/codox/index.html', attrs: { target: '_blank' } },
                                { label: 'Configuration cookbook', link: '/drivers/clojure/configuration' },
                                { label: 'Temporal Types cookbook', link: '/drivers/clojure/temporal' },
                                { label: 'Tutorial: Learn XTQL Today', link: '/tutorials/learn-xtql-today-with-clojure' },

                            ]
                        },

                    ]
                },
                {
                    label: 'Configuration',
                    collapsed: true,
                    items: [
                        { label: 'Overview', link: '/config' },

                        {
                            label: 'Transaction Log',
                            items: [
                                { label: 'Overview', link: '/config/tx-log' },
                                { label: 'Kafka', link: '/config/tx-log/kafka' },
                            ],
                        },

                        {
                            label: 'Storage',
                            items: [
                                { label: 'Overview', link: '/config/storage' },
                                { label: 'AWS S3', link: '/config/storage/s3' },
                                { label: 'Azure Blob Storage', link: '/config/storage/azure' },
                                { label: 'Google Cloud Storage', link: '/config/storage/google-cloud' }
                            ]
                        },

                        {
                            label: 'Optional Modules',
                            items: [
                                { label: 'Overview', link: '/config/modules' },
                                { label: 'HTTP Server', link: '/config/modules/http-server' }
                            ]
                        },
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
                {
                    tag: 'link',
                    attrs: {
                        id: 'hl-light',
                        rel: 'stylesheet',
                        href: "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/atom-one-light.min.css"
                    },
                },
                {
                    tag: 'link',
                    attrs: {
                        id: 'hl-dark',
                        rel: 'stylesheet',
                        href: "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/atom-one-dark.min.css"
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

        swup({ theme: false,
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
