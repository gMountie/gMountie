// @ts-check
// Hand-authored sidebar for the real gMountie docs. Doc IDs are file paths under
// ../docs without the extension (with a few overrides via per-doc frontmatter `id`).
// Categories use `collapsed: false` (the design's expanded mono section labels).

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'home',           // docs/index.md — slug "/"
    'quickstart',     // docs/quickstart.mdx
    'cli-cheatsheet', // docs/cli-cheatsheet.md
    'comparison',     // docs/comparison.md
    {
      type: 'category',
      label: 'Server',
      collapsed: false,
      items: ['server/cli', 'server/config'],
    },
    {
      type: 'category',
      label: 'Client',
      collapsed: false,
      items: ['client/cli', 'client/config'],
    },
    {
      type: 'category',
      label: 'Concepts',
      collapsed: false,
      items: [
        'concepts/wire-protocol',
        'concepts/cache',
        'concepts/sessions-and-reconnect',
        'concepts/identity',
      ],
    },
    {
      type: 'category',
      label: 'Recipes',
      collapsed: false,
      link: {type: 'doc', id: 'recipes/index'}, // docs/recipes/index.md
      items: [
        'recipes/systemd',
        'recipes/docker',
        'recipes/caddy-reverse-proxy',
        'recipes/multi-volume-vfs',
      ],
    },
    'troubleshooting',
    'error-catalog',
    'glossary',
    'roadmap',
    {
      type: 'category',
      label: 'Design',
      collapsed: false,
      items: [
        'design/architecture',
        'design/caching-and-consistency',
        'design/performance',
        'design/identity-and-permissions',
        'design/operations-and-packaging',
      ],
    },
  ],
};

export default sidebars;
