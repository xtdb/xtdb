import colors from 'tailwindcss/colors';
import defaultTheme from 'tailwindcss/defaultTheme'
import starlightPlugin from '@astrojs/starlight-tailwind';

/** @type {import('tailwindcss').Config} */
export default {
	content: ['./src/**/*.{astro,html,js,jsx,md,mdx,adoc,svelte,ts,tsx,vue}'],
	theme: {
        container: {
            padding: {
                DEFAULT: '1rem',
                md: '2rem'
            },
            center: true
        },

        fontFamily: {
            sans: ['Atkinson Hyperlegible', ...defaultTheme.fontFamily.sans],
            mono: ['Source Code Pro', 'Space Mono',...defaultTheme.fontFamily.mono]
        },

		extend: {
			colors: {
				// Your preferred accent color. Indigo is closest to Starlight’s defaults.
				accent: colors.orange,
				// Your preferred gray scale. Zinc is closest to Starlight’s defaults.
				gray: colors.stone,

                'link': '#243c5a',
                'complimentary-blue': '#1e81f7',
                'juxt-orange': '#f7941e',
                'juxt-orange-light': '#f8a139',
                'dark-orange': '#78350f',
                'light-grey': '#cbcbcb',
                'grey': '#434343',
                'dark-grey': '#333333',
                'background-orange': '#fff5ea',
			},
		},
	},
	plugins: [starlightPlugin()],
};
