/** @type {import('tailwindcss').Config} */
const colors = require('tailwindcss/colors')
const defaultTheme = require('tailwindcss/defaultTheme')
const typography = require('@tailwindcss/typography')

module.exports = {
	content: ['./shared/src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue,css}', './src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue,css}'],
	darkMode: 'class',
	theme: {
		container: {
			padding: {
				DEFAULT: '1rem',
				md: '2rem'
			},
			center: true
		},

		fontFamily: {
			sans: ['Atkinson Hyperlegible', ...defaultTheme.fontFamily.mono],
			mono: ['Source Code Pro', 'Space Mono',...defaultTheme.fontFamily.mono]
		},

		extend: {
			colors: {
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
			backgroundImage: {
				'button-dark':'linear-gradient(180deg,rgb(51,51,51)50%,rgb(0,0,0)50%)',
				'button-white':'linear-gradient(180deg,rgb(255,255,255)50%,rgb(240,240,240)50%)',
				'button-orange': 'linear-gradient(180deg,rgb(247,148,30)50%,rgb(222,133,27)50%)',
				'wave': "url('/images/wave.svg')",
				'hero-image': "url('/images/space.webp')",
				'hero-image-nostars': "url('/images/space-nostars.webp')"			
			},
		},
	},

	plugins: [typography]
}
