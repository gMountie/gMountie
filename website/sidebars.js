// @ts-check
// Hand-authored sidebar for the real gMountie docs. Doc IDs are file paths under
// ../docs without the extension. Categories use `collapsed: false` (expanded, with
// the design's mono uppercase section labels + custom chevron — matches the design
// template's sidebar treatment).

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'home', // docs/index.md — slug "/"
    'quickstart',
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
