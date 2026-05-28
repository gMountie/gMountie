// @ts-check
// Docusaurus config for the gMountie docs site (docs.gmountie.dev).
// Theme = the gMountie design-system docs theme (src/css/custom.css + Pacer).
// Docs content lives in ../docs as plain CommonMark (markdown.format:'detect') so
// the same files render on GitHub; `.mdx` is the opt-in escape hatch (e.g. <Pacer>,
// admonitions). The TOC-swizzle Pacer card appears on every page regardless.

// Prism token colors calibrated to the gMountie ember/lake palette (from the design).
const prismTokenStyles = [
  {types: ['comment', 'prolog', 'cdata'], style: {color: '#8B826E', fontStyle: 'italic'}},
  {types: ['punctuation', 'operator'], style: {color: '#C9C1AB'}},
  {types: ['property', 'tag', 'boolean', 'number', 'constant', 'symbol', 'deleted'], style: {color: '#ED7A33'}},
  {types: ['selector', 'attr-name', 'string', 'char', 'builtin', 'inserted'], style: {color: '#8FB0CB'}},
  {types: ['atrule', 'attr-value', 'keyword'], style: {color: '#ED7A33'}},
  {types: ['function', 'class-name'], style: {color: '#8FB39C'}},
  {types: ['regex', 'important', 'variable'], style: {color: '#F4A36E'}},
];

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'gMountie',
  tagline: 'Mount remote storage anywhere over the internet. No VPN.',
  favicon: 'img/mark.svg',

  future: {v4: true},

  url: 'https://docs.gmountie.dev',
  baseUrl: '/',

  organizationName: 'gMountie',
  projectName: 'gMountie',

  onBrokenLinks: 'warn',
  onBrokenAnchors: 'warn',

  // Parse `.md` as CommonMark (portable, GitHub-renderable); `.mdx` opts into MDX.
  markdown: {
    format: 'detect',
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {defaultLocale: 'en', locales: ['en']},

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          path: '../docs',
          routeBasePath: '/',
          sidebarPath: './sidebars.js',
          // README.md is a symlink to the repo-root README (the GitHub homepage); its
          // links are root-relative, so the docs site uses its own docs/index.md home.
          // Transient planning artifacts under docs/superpowers/ are never published.
          exclude: ['**/superpowers/**', 'README.md'],
          editUrl: 'https://github.com/gMountie/gMountie/tree/master/docs/',
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
        gtag: {
          trackingID: 'G-FGPD9XB1H5',
          anonymizeIP: true,
        },
      }),
    ],
  ],

  themes: [
    [
      // Offline, build-time search index — no Algolia account, no runtime network.
      require.resolve('@easyops-cn/docusaurus-search-local'),
      /** @type {import('@easyops-cn/docusaurus-search-local').PluginOptions} */
      ({
        hashed: true,
        indexDocs: true,
        indexBlog: false,
        docsDir: '../docs',
        docsRouteBasePath: '/',
        highlightSearchTermsOnTargetPage: true,
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/og-card.svg',
      colorMode: {
        defaultMode: 'light',
        respectPrefersColorScheme: true,
      },
      docs: {
        sidebar: {hideable: false, autoCollapseCategories: false},
      },
      navbar: {
        title: 'gMountie',
        logo: {alt: 'gMountie', src: 'img/mark.svg', width: 26, height: 26},
        // Right side renders in order: search → GitHub, then the (swizzled,
        // labeled) color-mode toggle is auto-appended last — matching the template.
        items: [
          {type: 'docSidebar', sidebarId: 'docs', position: 'left', label: 'Docs'},
          {href: 'https://github.com/gMountie/gMountie/releases', label: 'Releases', position: 'left'},
          {type: 'search', position: 'right'},
          {href: 'https://github.com/gMountie/gMountie', label: 'GitHub', position: 'right'},
        ],
      },
      footer: {
        style: 'light',
        links: [
          {
            title: 'Docs',
            items: [
              {label: 'Quickstart', to: '/quickstart'},
              {label: 'Architecture & Protocol', to: '/design/architecture'},
              {label: 'Roadmap', to: '/roadmap'},
            ],
          },
          {
            title: 'Community',
            items: [
              {label: 'GitHub', href: 'https://github.com/gMountie/gMountie'},
              {label: 'Issues', href: 'https://github.com/gMountie/gMountie/issues'},
              {label: 'Discussions', href: 'https://github.com/gMountie/gMountie/discussions'},
            ],
          },
          {
            title: 'Project',
            items: [
              {label: 'Releases', href: 'https://github.com/gMountie/gMountie/releases'},
              {label: 'Security', href: 'https://github.com/gMountie/gMountie/security'},
              {label: 'License — Apache-2.0', href: 'https://github.com/gMountie/gMountie/blob/master/LICENSE'},
            ],
          },
        ],
        copyright: `© ${new Date().getFullYear()} the gMountie project · network filesystem · Apache-2.0`,
      },
      prism: {
        // Dark terminal in both modes — the CLI is the canonical view of gMountie.
        theme: {plain: {color: '#F4EEDD', backgroundColor: '#0E0C09'}, styles: prismTokenStyles},
        darkTheme: {plain: {color: '#F4EEDD', backgroundColor: '#08070A'}, styles: prismTokenStyles},
        additionalLanguages: ['bash', 'diff', 'json', 'toml', 'yaml', 'go', 'protobuf', 'docker'],
      },
      // Right TOC shows only top-level (h2) headings — no nested h3 indent/rule.
      tableOfContents: {minHeadingLevel: 2, maxHeadingLevel: 2},
    }),
};

export default config;
