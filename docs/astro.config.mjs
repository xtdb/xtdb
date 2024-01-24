import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import tailwind from '@astrojs/tailwind';
import yaml from '@rollup/plugin-yaml';
import adoc from '/shared/src/adoc'

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
        src: '/shared/public/images/logo-core2.svg',
        replacesTitle: true
      },

      sidebar: [
        { label: '← xtdb.com', link: 'https://xtdb.com', attrs: {target: '_blank'}},
        { label: '← 1.x (stable release) docs', link: 'https://v1-docs.xtdb.com', attrs: {target: '_blank'}},

        {
          label: 'Introduction',
          collapsed: false,
          items: [
            { label: 'Overview', link: '/' },
            { label: 'Why XTDB?', link: '/intro/why-xtdb'},
            { label: 'Getting started', link: '/intro/getting-started'},
            {
              label: 'What is XTDB?',
              items: [
                { label: 'At a glance', link: '/intro/what-is-xtdb' },
                { label: 'What is XTQL?', link: '/intro/what-is-xtql' },
                { label: 'Data model', link: '/intro/data-model' },
                // { label: 'Architecture', link: '/intro/architecture' },
                // { label: 'Bitemporality', link: '/intro/bitemporality' }
              ]
            },
            { label: 'Community', link: '/intro/community'},
            { label: 'Roadmap', link: '/intro/roadmap'},
          ],
        },

        {
          label: 'Tutorials',
          collapsed: true,
          items: [
            { label: 'Learn XTQL Today, with Clojure', link: '/tutorials/learn-xtql-today-with-clojure' },
          ],
        },

        {
          label: 'Guides',
          collapsed: true,
          items: [
            { label: 'Setting up a cluster on AWS', link: '/guides/starting-with-aws' },
            { label: 'XTQL Walkthrough', link: '/guides/xtql-walkthrough' },
          ],
        },

        {
          label: 'Reference',
          collapsed: true,
          items: [
            { label: 'Overview', link: '/reference/main' },
            {
              label: 'XTQL',
              collapsed: true,
              items: [
                { label: 'Transactions', link: '/reference/main/xtql/txs'},
                { label: 'Queries', link: '/reference/main/xtql/queries'},
              ]
            },

            {
              label: 'SQL',
              collapsed: true,
              items: [
                { label: 'Transactions', link: '/reference/main/sql/txs'},
                { label: 'Queries', link: '/reference/main/sql/queries'},
              ]
            },

            { label: 'Data Types', link: '/reference/main/data-types'},

            {
              label: 'Standard Library',
              collapsed: true,
              items: [
                { label: 'Overview', link: '/reference/main/stdlib'},
                { label: 'Predicates', link: '/reference/main/stdlib/predicates'},
                { label: 'Numeric functions', link: '/reference/main/stdlib/numeric'},
                { label: 'String functions', link: '/reference/main/stdlib/string'},
                { label: 'Temporal functions', link: '/reference/main/stdlib/temporal'},
                { label: 'Aggregate functions', link: '/reference/main/stdlib/aggregates'},
              ]
            },

            {
              label: 'SDKs',
              collapsed: true,
              items: [
                { label: 'Overview', link: '/reference/main/sdks' },
                { label: 'Clojure ↗', link: '/sdks/clojure/index.html', attrs: {target: '_blank'} },
                // broken atm.
                // { label: 'Java ↗', link: '/sdks/java/index.html', attrs: {target: '_blank'}},
                { label: 'Kotlin ↗', link: '/sdks/kotlin/index.html', attrs: {target: '_blank'}},
                { label: 'HTTP (OpenAPI) ↗', link: '/sdks/openapi/index.html', attrs: {target: '_blank'}},
              ]
            },

            {
              label: 'Modules',
              collapsed: true,
              items: [
                { label: 'Kafka', link: '/reference/main/modules/kafka'},
                { label: 'S3', link: '/reference/main/modules/s3'},
              ]
            }
          ]
        },
      ],

      customCss: ['./src/styles/tailwind.css'],

      components: {
        TableOfContents: './src/components/table-of-contents.astro',
        MobileTableOfContents: './src/components/mobile-table-of-contents.astro',
      },
    }),

    tailwind({ applyBaseStyles: false }),

    adoc(),
  ],

  trailingSlashes: "never",

  build: {
    format: "file"
  },

  vite: {
    plugins: [yaml()]
  },
});
