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
        src: '/shared/public/images/logo-text.svg',
        replacesTitle: true
      },

      sidebar: [
        { label: 'xtdb.com', link: 'https://xtdb.com'},
        {
          label: 'Learn',
          items: [
            {
              label: 'Introduction',
              items: [
                { label: 'Getting started', link: '/learn/getting-started' },
                { label: 'What is XTDB?', link: '/learn/what-is-xtdb' },
                { label: 'Key Concepts', link: '/learn/key-concepts' },
              ],
            }, {
              label: 'Guides',
              items: [
                { label: 'Setting up an example cluster on AWS', link: '/learn/starting-with-aws' },
              ],
            }, {
              label: 'Tutorials',
              items: [
                { label: 'Bitemporality in SQL', link: '/learn/bitemporality-in-sql' },
                { label: 'Discover XTQL with cURL', link: '/learn/discover-xtql-with-curl' },
              ],
            }, {
              label: 'For Clojurists',
              items: [
                { label: 'Learn XTQL Today, with Clojure', link: '/learn/learn-xtql-today-with-clojure' },
              ],
            },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'Installation', link: '/reference/main/installation'},
            { label: 'Data Types', link: '/reference/main/data-types'},
            {
              label: 'XTQL',
              items: [
                { label: 'Transactions', link: '/reference/main/xtql/txs'},
                { label: 'Queries', link: '/reference/main/xtql/queries'},
              ]
            }, {
              label: 'SQL',
              items: [
                { label: 'Transactions', link: '/reference/main/sql/txs'},
                { label: 'Queries', link: '/reference/main/sql/queries'},
              ]
            }, {
              label: 'Standard Library',
              items: [
                { label: 'Introduction', link: '/reference/main/stdlib'},
                { label: 'Predicates', link: '/reference/main/stdlib/predicates'},
                { label: 'Numeric functions', link: '/reference/main/stdlib/numeric'},
                { label: 'String functions', link: '/reference/main/stdlib/string'},
                { label: 'Temporal functions', link: '/reference/main/stdlib/temporal'},
                { label: 'Aggregate functions', link: '/reference/main/stdlib/aggregates'},
              ]
            },
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

  trailingSlash: 'never',

  build: {
    format: 'file'
  },

  vite: {
    plugins: [yaml()]
  },

  redirects: {
    '/': '/learn',
    '/learn': '/learn/getting-started',
    '/reference': '/reference/main/installation',
  }
});
