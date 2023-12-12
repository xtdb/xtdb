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
        { label: 'xtdb.com', link: 'https://xtdb.com', attrs: {target: '_blank'}},
        { label: 'Welcome!', link: '/index' },

        {
          label: 'Concepts',
          items: [
            { label: 'What is XTDB?', link: '/concepts/what-is-xtdb' },
            { label: 'Key Concepts', link: '/concepts/key-concepts' },
          ],
        },/* {
          label: 'Tutorials',
          items: [
            { label: 'Bitemporality in SQL', link: '/tutorials/bitemporality-in-sql' },
            { label: 'Discover XTQL with cURL', link: '/tutorials/discover-xtql-with-curl' },
          ],
        }, */
        {
          label: 'Guides',
          items: [
            { label: 'Setting up a cluster on AWS', link: '/guides/starting-with-aws' },
          ],
        }, {
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
            }, {
              label: 'Client libraries',
              items: [
                { label: 'Clojure', link: '/clients/clojure/index.html', attrs: {target: '_blank'} },
                { label: 'Java', link: '/clients/java/index.html', attrs: {target: '_blank'}},
                { label: 'HTTP (OpenAPI)', link: '/clients/openapi/index.html', attrs: {target: '_blank'}},
              ]
            }, {
              label: 'Modules',
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

  trailingSlash: 'never',

  build: {
    format: 'file'
  },

  vite: {
    plugins: [yaml()]
  },
});
