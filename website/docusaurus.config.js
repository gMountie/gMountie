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
    mermaid: true,
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
    // Mermaid diagrams — styling in src/css/custom.css §19, baseline tokens
    // in themeConfig.mermaid below.
    '@docusaurus/theme-mermaid',
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
      mermaid: {
        // CSS does the heavy lifting (custom.css §19). Both themes use 'base'
        // so our rules have a neutral baseline to override; themeVariables here
        // cover the moment between initial paint and the stylesheet attaching.
        theme: {light: 'base', dark: 'base'},
        options: {
          fontFamily: 'Geist, -apple-system, BlinkMacSystemFont, "Helvetica Neue", Arial, sans-serif',
          themeVariables: {
            background:            'transparent',
            primaryColor:          '#F4F0E6', // paper-1
            primaryBorderColor:    '#BFB39A', // border-strong
            primaryTextColor:      '#15120D', // ink-0
            secondaryColor:        '#ECE6D6', // paper-2
            tertiaryColor:         '#FBE4CC', // ember-mist
            tertiaryBorderColor:   '#D9641C', // ember
            tertiaryTextColor:     '#6E2D08', // ember-ink
            mainBkg:               '#F4F0E6',
            lineColor:             '#6B6151', // ink-2
            textColor:             '#15120D',
            titleColor:            '#15120D',
            edgeLabelBackground:   '#FBF8F2', // paper-0
            clusterBkg:            '#ECE6D6',
            clusterBorder:         '#DFD7C2',
            actorBkg:              '#F4F0E6',
            actorBorder:           '#BFB39A',
            actorTextColor:        '#15120D',
            actorLineColor:        '#BFB39A',
            signalColor:           '#6B6151',
            signalTextColor:       '#3A3328',
            labelBoxBkgColor:      '#F4F0E6',
            labelBoxBorderColor:   '#BFB39A',
            labelTextColor:        '#15120D',
            loopTextColor:         '#6B6151',
            activationBkgColor:    '#ECE6D6',
            activationBorderColor: '#DFD7C2',
            noteBkgColor:          '#FBE4CC',
            noteBorderColor:       '#D9641C',
            noteTextColor:         '#6E2D08',
            altBackground:         '#ECE6D6',
            sectionBkgColor:       'transparent',
            altSectionBkgColor:    'transparent',
            taskBkgColor:          '#DFD7C2',
            taskBorderColor:       '#BFB39A',
            taskTextColor:         '#15120D',
            activeTaskBkgColor:    '#D9641C',
            activeTaskBorderColor: '#B14C12',
            doneTaskBkgColor:      '#BFD2CA',
            doneTaskBorderColor:   '#2F5D50',
            critBkgColor:          '#E9C0B9',
            critBorderColor:       '#9E2F22',
            gridColor:             '#DFD7C2',
            pie1: '#D9641C', pie2: '#3A3328', pie3: '#3D6E96',
            pie4: '#2F5D50', pie5: '#F3B488', pie6: '#9C927F',
          },
        },
      },
    }),
};

export default config;
