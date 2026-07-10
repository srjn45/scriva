import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// GitHub Pages project site lives at https://srjn45.github.io/FileDBv2
export default defineConfig({
  site: 'https://srjn45.github.io',
  base: '/FileDBv2/',
  integrations: [
    starlight({
      title: 'FileDB v2',
      description: 'A lightweight, append-only, file-based document database. Human-readable NDJSON storage, gRPC + REST from one binary, and an embeddable Go engine.',
      logo: {
        light: './src/assets/filedb-wordmark-light.svg',
        dark: './src/assets/filedb-wordmark-dark.svg',
        replacesTitle: true,
      },
      favicon: '/favicon.svg',
      customCss: ['./src/styles/docs.css'],
      head: [
        { tag: 'meta', attrs: { property: 'og:image', content: 'https://srjn45.github.io/FileDBv2/og-image.svg' } },
        { tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
      ],
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/srjn45/FileDBv2' },
      ],
      sidebar: [
        { label: 'Start here', items: [
          { label: 'What is FileDB?', slug: 'start/what-is-filedb' },
          { label: 'Install', slug: 'start/install' },
          { label: 'Quickstart', slug: 'start/quickstart' },
        ]},
        { label: 'Guides', items: [
          { label: 'Data model', slug: 'guides/data-model' },
          { label: 'Queries & indexes', slug: 'guides/queries' },
          { label: 'Durability & backup', slug: 'guides/durability-and-backup' },
          { label: 'Replication & failover', slug: 'guides/replication' },
          { label: 'Embedding (Go library)', slug: 'guides/embedding' },
          { label: 'Client SDKs', slug: 'guides/clients' },
        ]},
        { label: 'Concepts', items: [
          { label: 'Architecture', slug: 'concepts/architecture' },
        ]},
        { label: 'Reference', items: [
          { label: 'Configuration', slug: 'reference/configuration' },
          { label: 'API & OpenAPI', slug: 'reference/api' },
          { label: 'Roadmap', slug: 'reference/roadmap' },
          { label: 'Contributing', slug: 'reference/contributing' },
        ]},
      ],
    }),
  ],
});
