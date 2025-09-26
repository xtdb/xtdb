import { defineCollection } from 'astro:content';
import { docsSchema } from '@astrojs/starlight/schema';

export const collections = {
	docs: defineCollection({ 
		schema: docsSchema(),
		// Explicitly include .adoc files
		type: 'content'
	}),
};
