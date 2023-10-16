import { defineConfig } from 'astro/config';
import mdx from "@astrojs/mdx";
import tailwind from "@astrojs/tailwind";
import preact from "@astrojs/preact";
import yaml from '@rollup/plugin-yaml';
import sitemap from "@astrojs/sitemap";
import adoc from '/shared/src/adoc'

export default defineConfig({
  integrations: [mdx(), tailwind(), preact(), sitemap(), adoc()],

  trailingSlash: 'never',
  build: {
    format: 'file'
  },
  site: 'https://docs.xtdb.com',
  vite: {
    plugins: [yaml()]
  },
  redirects: {
    '/': '/learn',
    '/learn': '/learn/getting-started',
    '/reference': '/reference/main/installation',
  }
});
