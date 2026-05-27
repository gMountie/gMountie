// @ts-check
// Docusaurus config for the gMountie docs site (docs.gmountie.dev).
// Docs content lives in ../docs as plain CommonMark Markdown (see markdown.format
// below) so the same files render on GitHub and on the built site. MDX (.mdx) is
// available as an opt-in escape hatch for the rare page that wants components.

// Always-dark terminal code blocks with the brand's signal palette
// (ember / pine / wire / mute), mirroring the docs UI kit's <pre>.
const gmountieTerminal = {
  plain: {color: '#F4EEDD', backgroundColor: '#0E0C09'},
  styles: [
    {types: ['comment', 'prolog', 'cdata'], style: {color: '#8B826E', fontStyle: 'italic'}},
    {types: ['punctuation'], style: {color: '#9C927F'}},
    {types: ['keyword', 'operator', 'tag', 'selector', 'atrule', 'important', 'rule'], style: {color: '#ED7A33'}},
    {types: ['string', 'char', 'inserted', 'attr-value', 'attr-equals'], style: {color: '#8FB39C'}},
    {types: ['number', 'boolean', 'constant', 'symbol', 'builtin', 'class-name', 'url'], style: {color: '#8FB0CB'}},
    {types: ['function', 'function-variable', 'attr-name', 'variable', 'property'], style: {color: '#F4EEDD'}},
    {types: ['deleted'], style: {color: '#D4736A'}},
  ],
};

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'gMountie',
  tagline: 'Mount remote storage anywhere over the internet — no VPN.',
  favicon: 'img/favicon.ico',

  future: {v4: true},

  url: 'https://docs.gmountie.dev',
  baseUrl: '/',

  organizationName: 'gMountie',
  projectName: 'gMountie',

  // Surface bad cross-references rather than silently 404 — kept at 'warn' through
  // the docsify→Docusaurus migration; tighten to 'throw' once links are settled.
  onBrokenLinks: 'warn',
  onBrokenAnchors: 'warn',

  // Parse `.md` as CommonMark (plain, portable, GitHub-renderable) and `.mdx` as MDX.
  markdown: {
    format: 'detect',
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {defaultLocale: 'en', locales: ['en']},

  stylesheets: [
    'https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,200..800;1,6..72,200..800&family=Geist:wght@100..900&family=Geist+Mono:wght@100..900&display=swap',
  ],

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
      colorMode: {
        defaultMode: 'dark',
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'gMountie',
        logo: {alt: 'gMountie', src: 'img/logo.svg'},
        items: [
          // Link back to the OSS landing site, like the kit header's "gMountie ↗".
          {href: 'https://gmountie.dev', label: 'gMountie ↗', position: 'right'},
          {
            href: 'https://github.com/gMountie/gMountie',
            label: 'GitHub',
            position: 'right',
          },
          // NOTE: re-add `{type: 'docsVersionDropdown', position: 'right'}` here
          // once the first version is cut (`npm run docusaurus docs:version <v>`).
        ],
      },
      footer: {
        style: 'dark',
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
            title: 'Project',
            items: [
              {label: 'GitHub', href: 'https://github.com/gMountie/gMountie'},
              {label: 'Issues', href: 'https://github.com/gMountie/gMountie/issues'},
              {label: 'Releases', href: 'https://github.com/gMountie/gMountie/releases'},
            ],
          },
        ],
        copyright: `© ${new Date().getFullYear()} the gMountie project · Apache-2.0`,
      },
      prism: {
        // Same dark terminal in both modes — the kit's <pre> is always dark.
        theme: gmountieTerminal,
        darkTheme: gmountieTerminal,
        additionalLanguages: ['bash', 'yaml', 'toml', 'go', 'protobuf', 'json', 'docker'],
      },
    }),
};

export default config;
